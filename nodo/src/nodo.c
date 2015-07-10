/*
 ============================================================================
 Name        : nodo.c
 Author      : karlOS
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#define _FILE_OFFSET_BITS	64
#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/log.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h> //Esta la agregeue para poder definir int con tamaño especifico (uint32_t)
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <semaphore.h>
#include "../../connectionlib/connectionlib.h"

// Estructuras
// La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	int id;
	int nodo_nuevo;
	int cant_bloques;
};

//Estructura que enviaría job a nodo al conectarse
typedef struct {
	uint32_t id_map;
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	uint32_t block;
	char* temp_file_name;
} t_map_dest;


// Una estructura que contiene todos los datos del arch de conf
struct conf_nodo {
	int id;
	char* ip_fs;
	int puerto_fs;
	char* archivo_bin;
	char* dir_temp;
	int nodo_nuevo;
	char* ip_nodo;
	int puerto_nodo;
	int cant_bloques;
};

typedef struct {
	int sockfd;
	pthread_t* thr;
	struct sockaddr_in socketaddr_cli;
} t_hilo_job;

typedef struct {
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	t_list* path_temps;
	int socket_Nodo;
}t_reduce_nodo_dest;

typedef struct {
	char* path;
	uint32_t id_nodo;
} t_lost_temp;

//Variables Globales
struct conf_nodo conf; // estructura que contiene la info del arch de conf
char *data; // data del archivo mapeado

t_log* logger;
pthread_mutex_t* mutex;

pthread_mutex_t mutex_last_script;
uint32_t last_script = 0;

//Prototipos
void levantar_arch_conf_nodo(); // devuelve una estructura con toda la info del archivo de configuracion "nodo.cfg"
void setNodoToSend(struct info_nodo *); // setea la estructura que va a ser enviada al fs al iniciar el nodo
int enviar_info_nodo(int, struct info_nodo*);
void free_conf_nodo();
int mapearArchivo();
void cargarBloque(int, char*, int);
void mostrarBloque(int);
int esperar_instrucciones_del_filesystem(int*);
int esperar_instrucciones_job(int *);
int solicitarConexionConFileSystem(struct conf_nodo);
int recibir_Bloque(int);
int enviar_bloque(int);
int enviar_tmp(int);
void inicializar_mutexs();
void finalizar_mutexs();
int receive_new_client_job();
void free_hilo_job(t_hilo_job* job);
void esperar_finalizacion_hilo_conex_job(t_hilo_job*);
int solicitarConexionConNodo(char* ip_nodo, uint32_t puerto_nodo, uint32_t id_nodo);
int obtener_puerto_job();
void obtener_hilos_jobs(int, t_list*);
int realizar_Map(int);
int realizar_Reduce(int);
int iniciar_Tarea_Map(char *,char *, uint32_t);
void escribir_Sobre_Archivo(FILE *, uint32_t);
void free_reduce_nodo_dest(t_reduce_nodo_dest* self);
int enviar_Tmps_ToNodo (int);

//Main####################################################################################################
int main(void) {
	int socket_fs,listener_job;
	pthread_t thread_FS_conection;
	 signal(SIGPIPE, SIG_IGN);

	t_list* lista_jobs;

	logger = log_create("nodo.log", "NODO", 1, LOG_LEVEL_TRACE);

	levantar_arch_conf_nodo();

    inicializar_mutexs();

	struct info_nodo info_envio;
	setNodoToSend(&info_envio);

	if(mapearArchivo() == -1) {
		free_conf_nodo();
		finalizar_mutexs();
		log_destroy(logger);
		return -1;
	}

    socket_fs = solicitarConexionConFileSystem(conf);

	if (enviar_info_nodo(socket_fs, &info_envio) <= 0) {
		log_error(logger, "no se pudo enviar el info nodo");
	} else {
		log_info(logger, "Se envio correctamente info nodo");
	}


	if ((pthread_create( &thread_FS_conection, NULL,(void *)esperar_instrucciones_del_filesystem, &socket_fs))== -1){
		perror("fallo en el: thread 2");
			exit(1);
	}

	lista_jobs = list_create();

	listener_job = obtener_puerto_job();

	obtener_hilos_jobs(listener_job, lista_jobs);

	list_iterate(lista_jobs, (void *) esperar_finalizacion_hilo_conex_job);
	pthread_join(thread_FS_conection, NULL);
	free_conf_nodo();
	finalizar_mutexs();
	list_destroy_and_destroy_elements(lista_jobs, (void *) free_hilo_job);
	log_destroy(logger);
	return EXIT_SUCCESS;
}

//#########################################################################################################

//----------------------------------------------------------------------------------------------------
int abrirArchivoDeDatosVerificandoEstado(char* path) {

	struct stat stat_file;
	int fd;

	long long int ideal_size = (long long int) BLOCK_SIZE * (long long int) conf.cant_bloques;

	if(stat(path, &stat_file) == 0) {
		log_debug(logger,"Tamaño Real: %ld, Esperado: %ld ", stat_file.st_size, ideal_size);
		if(stat_file.st_size != ideal_size) {

			if(!conf.nodo_nuevo) {
				log_error(logger, "El Archivo de Datos del Nodo no cohincide con el tamaño que debería tener (%i * %i Bytes)",
						conf.cant_bloques, BLOCK_SIZE);
				log_info(logger,"Para Redimensionar el archivo reinicie el proceso con la opción Nodo Nuevo seteada");
				log_warning(logger,"Atención, al realizar esto perderá todos los datos del nodo");
				return -1;
			}

			log_info(logger,"Llevando archivo de Datos a tamaño Deseado");

			if(truncate(path, BLOCK_SIZE * conf.cant_bloques) != 0) {
				log_error(logger,"No se pudo truncar el archivo de Datos");
				return -1;
			}
		}

		if ((fd = open(path, O_RDWR)) == -1) {
			log_error(logger,"No se pudo abrir el archivo de Datos");
			return -1;
		}

		return fd;
	}

	if(!conf.nodo_nuevo) {
		log_error(logger,"No se encontró el archivo de Datos ");
		log_info(logger,"Para Crear uno nuevo Reinicie el proceso con la opción Nodo Nuevo seteada");
		return -1;
	}

	if((fd = open(path, O_CREAT|O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) == -1) {
		log_error(logger,"No se pudo Crear el archivo de Datos ");
		return -1;
	}

	log_info(logger,"Llevando archivo de Datos a tamaño Deseado");

	if(truncate(path, BLOCK_SIZE * conf.cant_bloques) != 0) {
		log_error(logger,"No se pudo truncar el archivo de Datos");
		return -1;
	}

	if(stat(path, &stat_file) == -1) {
		log_error(logger,"Error while stat");
		return -1;
	}

	return fd;
}

//----------------------------------------------------------------------------------------------------
int obtener_puerto_job(){
	int listener_job;

	log_debug(logger, "Obteniendo Puerto para Escuchar Jobs...");
	if ((listener_job = escucharConexionesDesde("", conf.puerto_nodo)) == -1) {
		log_error(logger, "No se pudo obtener Puerto para Escuchar Jobs");
		exit(-1);
	} else {
		log_debug(logger, "Puerto para Escuchar Jobs Obtenido");
	}
	return listener_job;
}

//-----------------------------------------------------------------------------------------------------
void obtener_hilos_jobs(int listener_job, t_list* lista_jobs){

	t_hilo_job* job;
	int bandera = 1;
  	  while (bandera) {
  		  job = malloc(sizeof(t_hilo_job));
  		  job->thr = malloc(sizeof(pthread_t));
  		  printf("Esperando hilos de job... \n");
  		  if ((job->sockfd = aceptarCliente(listener_job, &(job->socketaddr_cli))) == -1) {
  			  log_error(logger, "Error al Aceptar Nuevos Jobs");
  			  exit(-1);
		  }
  		  printf("\n hilos de job... \n");

		  list_add(lista_jobs, job);

		  log_info(logger, "Creación de Hilo Job...");
		  if (pthread_create(job->thr, NULL, (void *) esperar_instrucciones_job, &(job->sockfd)) != 0) {
			  log_error(logger, "No se pudo crear Hilo Job");
			  exit(-1);
		  }

//		  } else {
//			  free_hilo_job(job);
//		  }
//		  if (hora_de_morir) { //XXX
//			  bandera = 0;
//		  }
  	  }
}

//---------------------------------------------------------------------------

int solicitarConexionConFileSystem(struct conf_nodo conf) {

	log_debug(logger, "Solicitando conexión con MDFS...");
	int socketFS = solicitarConexionCon(conf.ip_fs, conf.puerto_fs);

	if (socketFS != -1) {
		log_info(logger, "Conexión con MDFS establecida IP: %s, Puerto: %i", conf.ip_fs, conf.puerto_fs);
	} else {
		log_error(logger, "Conexión con MDFS FALLIDA!!! IP: %s, Puerto: %i", conf.ip_fs, conf.puerto_fs);
		exit(-1);
	}

	return socketFS;
}
//----------------------------------------------------------------------------------------------------


//----------------------------------------------------------------------------------------------------
void free_lost_temp(t_lost_temp* self) {
	free(self->path);
	free(self);
}

//----------------------------------------------------------------------------------------------------
void free_hilo_job(t_hilo_job* job) {
	free(job->thr);
	free(job);
}

//----------------------------------------------------------------------------------------------------
int receive_new_client_job(int sockjob) {
	uint32_t prot = 0;

	prot = receive_protocol_in_order(sockjob);

	switch (prot) {

	case NUEVO_JOB:
		log_debug(logger, "Nuevo Job aceptado");
		return 1;
		break;

	case DISCONNECTED:
		log_error(logger, "Job se desconectó de forma inesperada");
		break;

	case -1:
		log_error(logger, "No se pudo recibir new Job del Job");
		break;

	default:
		log_error(logger, "Protocolo Inesperado %i (MaRTA PANIC!)", prot);
		break;

	}

	return 0;
}


//---------------------------------------------------------------------------

int esperar_instrucciones_del_filesystem(int *socket){

	uint32_t tarea;

    do{
    	log_info(logger, "Esperando Instruccion FS");

    	tarea = receive_protocol_in_order(*socket);

	    switch (tarea) {

		case WRITE_BLOCK:
			if (recibir_Bloque(*socket) <=0) {
				log_error(logger, "no se pudo cargar el bloque");
			}
			else {
					log_info(logger, "Se cargo correctamente el bloque al nodo");
				}
			break;

		case READ_BLOCK:
			if (enviar_bloque(*socket) <=0){
				log_error(logger, "no se pudo enviar el bloque");
			}
			else {
					log_info(logger, "Se envio correctamente el bloque al MDFs");
				}
			break;

		case READ_RESULT_JOB:
			if (enviar_tmp(*socket) <=0){
				log_error(logger, "no se pudo enviar el .tmp");
			}
			else {
					log_info(logger, "Se envio correctamente el .tmp al MDFs");
				}
			break;

		default:
			return -1;
		}
    } while(tarea != DISCONNECTED);

	return tarea;


}
//---------------------------------------------------------------------------

int esperar_instrucciones_job(int *socket_job){
	uint32_t tarea;


	do{
		log_info(logger, "Esperando Instruccion Job");
		tarea = receive_protocol_in_order(*socket_job);
		switch (tarea) {
		case EXECUTE_MAP:
			if (realizar_Map(*socket_job) <=0) {
				log_error(logger, "no se pudo realizar rutina de map");
			}
			else {
				log_info(logger, "Se realizó correctamente la rutina map");
			}
			break;
		case EXECUTE_REDUCE:
			if (realizar_Reduce(*socket_job) <=0){
				log_error(logger, "no se pudo realizar rutina reduce");
			}
			else {
				log_info(logger, "Se realizó correctamente la rutina reduce");
			}
			break;
		case CONNECTION_NODO:
			if (enviar_Tmps_ToNodo(*socket_job) <=0){
				log_error(logger, "no se pudo enviar los Tmps al Nodo");
			}
			else {
				log_info(logger, "Se enviaron Los tmps correctamente al Nodo Amigo");
			}
			break;

		default:
			return -1;
		}
	} while(tarea != DISCONNECTED);
	return tarea;
}
//---------------------------------------------------------------------------

int recibir_Bloque(int socket) {

	int result = 1;
    uint32_t nroBloque;
    int longInfo;

    result = (result > 0) ? receive_int_in_order(socket, &nroBloque) : result;
	pthread_mutex_lock( &mutex[nroBloque] );
	result = (result > 0) ? longInfo=receive_static_array_in_order(socket, &data[nroBloque*BLOCK_SIZE]) : result;
	if(result > 0) {
		data[nroBloque*BLOCK_SIZE + longInfo]='\0';
	}
	pthread_mutex_unlock( &mutex[nroBloque] );
	return result;
}

//---------------------------------------------------------------------------

int enviar_bloque(int socket) {

	int result = 1;
    uint32_t nroBloque;

    result = (result > 0) ? receive_int_in_order(socket, &nroBloque) : result;
    uint32_t largoBloque = strlen(&data[nroBloque*BLOCK_SIZE]);

    pthread_mutex_lock(&mutex[nroBloque]);
	result = (result > 0) ? send_stream_with_size_in_order(socket, &data[nroBloque*BLOCK_SIZE],largoBloque+1) : result;
	pthread_mutex_unlock(&mutex[nroBloque]);
	return result;
}
//---------------------------------------------------------------------------

int enviar_tmp(int socket) {

	int result = 1;
    char* nombreArchivo;
    char* path_completo;

	result = (result > 0) ? receive_dinamic_array_in_order(socket,(void **) &nombreArchivo) : result;
	path_completo = string_from_format("%s/%s",conf.dir_temp,nombreArchivo);

	struct stat stat_file;
	if( stat(path_completo, &stat_file) == -1) {

	}

	result = (result > 0) ? send_entire_file_by_parts(socket,path_completo,(4*1024) ) : result;
	free(path_completo);
	free(nombreArchivo);
	return result;
}
//---------------------------------------------------------------------------

void levantar_arch_conf_nodo() {
	char** properties =
			string_split(
					"ID,IP_FS,PUERTO_FS,ARCHIVO_BIN,DIR_TEMP,NODO_NUEVO,IP_NODO,PUERTO_NODO,CANT_BLOQUES",
					",");
	t_config* conf_arch = config_create("nodo.cfg");

	if (has_all_properties(9, properties, conf_arch)) {
		conf.id = config_get_int_value(conf_arch, properties[0]);
		conf.ip_fs = strdup(config_get_string_value(conf_arch, properties[1]));
		conf.puerto_fs = config_get_int_value(conf_arch, properties[2]);
		conf.archivo_bin = strdup(config_get_string_value(conf_arch, properties[3]));
		conf.dir_temp = strdup(config_get_string_value(conf_arch, properties[4]));
		conf.nodo_nuevo = config_get_int_value(conf_arch, properties[5]);
		conf.ip_nodo = strdup(config_get_string_value(conf_arch, properties[6]));
		conf.puerto_nodo = config_get_int_value(conf_arch, properties[7]);
		conf.cant_bloques = config_get_int_value(conf_arch, properties[8]);
	} else {
		log_error(logger, "Faltan propiedades en archivo de Configuración");
		exit(-1);
	}

	free_string_splits(properties);
	config_destroy(conf_arch);
}

//---------------------------------------------------------------------------
void setNodoToSend(struct info_nodo *info_envio) {
	info_envio->id = conf.id;
	info_envio->nodo_nuevo = conf.nodo_nuevo;
	info_envio->cant_bloques = conf.cant_bloques;
}

//---------------------------------------------------------------------------
int enviar_info_nodo(int socket, struct info_nodo *info_nodo) {

	int result = 1;

	t_buffer* info_nodo_buffer = buffer_create_with_protocol(INFO_NODO);

	buffer_add_int(info_nodo_buffer,info_nodo->id);
	buffer_add_int(info_nodo_buffer,info_nodo->cant_bloques);
	buffer_add_int(info_nodo_buffer,info_nodo->nodo_nuevo);
	buffer_add_int(info_nodo_buffer,inet_addr(conf.ip_nodo));
	buffer_add_int(info_nodo_buffer,conf.puerto_nodo);

	send_buffer_and_destroy(socket,info_nodo_buffer);

	return result;
}

//---------------------------------------------------------------------------
void free_conf_nodo() {
	free(conf.ip_fs);
	free(conf.archivo_bin);
	free(conf.dir_temp);
	free(conf.ip_nodo);
}

//---------------------------------------------------------------------------
int mapearArchivo() {

	int fd;
	struct stat sbuf;

	log_info(logger, "inicio de mapeo");

	fd = abrirArchivoDeDatosVerificandoEstado(conf.archivo_bin);

	if(fd == -1) {
		log_error(logger,"no se pudo mappear el archivo de Datos!");
		return -1;
	}

	if (fstat(fd, &sbuf) == -1) {
		perror("fstat()");
	}
	data = mmap((caddr_t) 0, sbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED,
			fd, 0);

	if (data == (caddr_t) (-1)) {
		perror("mmap");
		exit(1);
	}
	log_info(logger,"mapeo correcto");

	return 1;
}
//---------------------------------------------------------------------------
void cargarBloque(int nroBloque, char* info, int offset_byte) {
	int pos_a_escribir = nroBloque * BLOCK_SIZE + offset_byte;
	memcpy(data + pos_a_escribir, info, strlen(info));
	//data[pos_a_escribir+strlen(info)]='\0';

}
//---------------------------------------------------------------------------
void mostrarBloque(int nroBloque) {
	printf("info bloque: %s\n", &(data[nroBloque * BLOCK_SIZE]));
}

//----------------------------------------------------------------------------
void inicializar_mutexs(void){
	int i;
	mutex = malloc(sizeof(pthread_mutex_t)*conf.cant_bloques);
	for(i=0; i<conf.cant_bloques; i++) {
		pthread_mutex_init(&mutex[i],NULL);
	}

	pthread_mutex_init(&mutex_last_script,NULL);
}
//----------------------------------------------------------------------------
void finalizar_mutexs(void){
	int i;
	for(i=0; i<conf.cant_bloques; i++) {
		pthread_mutex_destroy(&mutex[i]);
	}
	free(mutex);

	pthread_mutex_destroy(&mutex_last_script);
}

//---------------------------------------------------------------------------
void esperar_finalizacion_hilo_conex_job(t_hilo_job* job) {
	void* ret_recep;

	if (pthread_join(*(job->thr), &ret_recep) != 0) {
		printf("Error al hacer join del hilo\n");
	}
}

//----------------------------------------------------------------------------
uint32_t last_script_increment_and_take() {
	uint32_t self;

	pthread_mutex_lock(&mutex_last_script);
	self = ++last_script;
	pthread_mutex_unlock(&mutex_last_script);

	return self;
}

//----------------------------------------------------------------------------
int realizar_Map(int socket) {

	int result = 1;
    uint32_t nroBloque;
    uint32_t id;
    char* pathMap = string_from_format("%s/map_script%i.sh",conf.dir_temp,last_script_increment_and_take());
    char* destino;


	result = (result > 0) ? receive_int_in_order(socket, &id) : result;
	result = (result > 0) ? receive_int_in_order(socket, &nroBloque) : result;
	result = (result > 0) ? receive_dinamic_array_in_order(socket, (void **) &destino) : result;
	result = (result > 0) ? receive_entire_file_by_parts( socket, pathMap, MAX_PART_SIZE) : result;

	if(result > 0) {
		char* destinoCompleto = string_from_format("%s/%s",conf.dir_temp,destino);
		if (id == conf.id){
			log_info(logger, "Realizando tarea de map");
			result = iniciar_Tarea_Map(pathMap, destinoCompleto, nroBloque);
			if(result > 0) {
				send_protocol_in_order(socket, MAP_OK);
			} else {
				send_protocol_in_order(socket, MAP_NOT_OK);
			}
		} else {
			log_error(logger,"Id de Nodo incompatible...Se esperaba otro Nodo");
			send_protocol_in_order(socket,INCORRECT_NODO);
		}
		free(destinoCompleto);
	} else {
		log_error(logger,"No se pudo obtener el Map");
	}

	free(pathMap);
	free(destino);
	return result;
}

//----------------------------------------------------------------------------
int iniciar_Tarea_Map(char *pathPrograma,char *pathArchivoSalida, uint32_t nroBloque)
{
	FILE *entradaARedirigir = NULL;

	char *comandoEntero = string_from_format("./%s | sort > %s", pathPrograma, pathArchivoSalida);

	entradaARedirigir = popen (comandoEntero,"w");

	if (entradaARedirigir != NULL)
	{
		log_info(logger,"Comienzo Map: %s",pathArchivoSalida);
		escribir_Sobre_Archivo(entradaARedirigir, nroBloque);
		log_info(logger,"Terminó Map: %s",pathArchivoSalida);
		pclose (entradaARedirigir);

		free(comandoEntero);
	}
	else
	{
		log_error(logger,"No se pudo ejecutar el programa!");
		return -1;
	}
	return 1;
}

//----------------------------------------------------------------------------
int write_temps_in_pipe_in_order(FILE *archivo, t_list* input_temp_paths) {

	int salida = 1;
	t_list* files;

	if(open_files_to_merge(input_temp_paths, &files) > 0) {
		char* linea;
		while((salida > 0) && (take_next_merged_line(files, &linea) > 0)) {
			salida= fprintf (archivo,"%s", linea);
			free(linea);
		}
		list_destroy_and_destroy_elements(files, (void *) free_file_with_line);
	} else {
		log_error(logger,"No se pudo abrir todos los temporales necesarios para el Reduce");
		return -1;
	}
	return 1;
}

//----------------------------------------------------------------------------
int iniciar_Tarea_Reduce(char *pathPrograma,char *pathArchivoSalida, t_list* input_temp_paths)
{
	FILE *entradaARedirigir = NULL;
	int result;

	char *comandoEntero = string_from_format("./%s | sort > %s", pathPrograma, pathArchivoSalida);

	entradaARedirigir = popen (comandoEntero,"w");

	if (entradaARedirigir != NULL)
	{
		log_info(logger,"Comienzo Reduce: %s",pathArchivoSalida);
		result = write_temps_in_pipe_in_order(entradaARedirigir, input_temp_paths);
		log_info(logger,"Terminó Reduce: %s",pathArchivoSalida);
		pclose (entradaARedirigir);

		free(comandoEntero);

		if(result <= 0) {
			return -1;
		}
	}
	else
	{
		log_error(logger,"No se pudo ejecutar el programa!");
		return -1;
	}
	return 1;
}

//----------------------------------------------------------------------------
void escribir_Sobre_Archivo(FILE *archivo, uint32_t indice)
{

		int salida;

		pthread_mutex_lock( &mutex[indice] );
		salida= fprintf (archivo,"%s",&data[indice * BLOCK_SIZE]);
		pthread_mutex_unlock( &mutex[indice] );

		if(salida<0){
			return;
	      }
}

//----------------------------------------------------------------------------
int receive_reduce_instruction(int socket, uint32_t *id, char** destino, t_list* listPathNodo, t_list* listNodosToConect, char* pathReduce) {
	int result = 1;
	int i,j;
	uint32_t cantTemp,cantNodos,cantTmpxNodo;
	char* aux_dest;

	result = (result > 0) ? receive_int_in_order(socket,id): result;
	if(result > 0) {
		receive_dinamic_array_in_order(socket,(void **) &aux_dest);
		*destino = string_from_format("%s/%s",conf.dir_temp,aux_dest);
		free(aux_dest);
	}
	result = (result > 0) ? receive_int_in_order(socket,&cantTemp): result; //en mi nodo

	for (i=0;i< cantTemp;i++) {
		char* nameTmp;
		result = (result > 0) ? receive_dinamic_array_in_order(socket, (void **) &nameTmp) : result;
		char* pathTmp = string_from_format("%s/%s", conf.dir_temp, nameTmp);
		list_add(listPathNodo, pathTmp);
		free(nameTmp);
	}

	result = (result > 0) ? receive_int_in_order(socket,&cantNodos): result;

	for (i=0;i< cantNodos;i++) {
		t_reduce_nodo_dest *nodoToConect;
	    nodoToConect =  malloc(sizeof(t_reduce_nodo_dest));
		result = (result > 0) ? receive_int_in_order(socket,&nodoToConect->id_nodo) : result;
		result = (result > 0) ? receive_int_in_order(socket,&nodoToConect->ip_nodo) : result;
		result = (result > 0) ? receive_int_in_order(socket,&nodoToConect->puerto_nodo) : result;
		result = (result > 0) ? receive_int_in_order(socket,&cantTmpxNodo) : result;
		nodoToConect->path_temps = list_create();

			for (j=0;j< cantTmpxNodo;j++)
			{
				char* pathTmpNodoGuest;
				result = (result > 0) ? receive_dinamic_array_in_order(socket,(void **) &pathTmpNodoGuest) : result;
				list_add(nodoToConect->path_temps,pathTmpNodoGuest);
			}
		list_add(listNodosToConect,nodoToConect);
	}

	result = (result > 0) ? receive_entire_file_by_parts(socket, pathReduce, MAX_PART_SIZE) : result;

	if(result > 0) {
		log_debug(logger,"Se recibieron instrucciones de Reduce correctamente");
	}

	return result;
}

//----------------------------------------------------------------------------
void conectWithNodoGuest(t_reduce_nodo_dest* nodo_guest, t_list* nodosRechazados) {

	char *ip_nodo = from_int_to_inet_addr(nodo_guest->ip_nodo);
	int socketNodo = solicitarConexionConNodo(ip_nodo, nodo_guest->puerto_nodo,nodo_guest->id_nodo);
	if (socketNodo <0){
		uint32_t* id_nodo = malloc(sizeof(uint32_t));
		*id_nodo = nodo_guest->id_nodo;
		list_add(nodosRechazados,(void *) id_nodo);
	} else{
		nodo_guest->socket_Nodo=socketNodo;
	}
	free(ip_nodo);
}

//----------------------------------------------------------------------------
int solicitarTmpsNodo(int socket, t_reduce_nodo_dest* nodo, t_list* listPathNodo, t_list* lostTemps) {

	int i, cantTmps;
	int result = 1;
	char* pathDest;
	char* tmpName;
	t_buffer* buffer_Tmps_Nods = buffer_create_with_protocol(CONNECTION_NODO);
	void _add_Buffer_tmp(char* path) {
		buffer_add_string(buffer_Tmps_Nods, path);
	}

	cantTmps = list_size(nodo->path_temps);
	buffer_add_int(buffer_Tmps_Nods, cantTmps);
	list_iterate(nodo->path_temps, (void*) _add_Buffer_tmp);
	send_buffer_and_destroy(socket, buffer_Tmps_Nods);

	for (i = 0; i < cantTmps; i++) {
		if(result > 0) {
			result = receive_dinamic_array_in_order(socket, (void **) &tmpName);
			if(result > 0) {
				pathDest = string_from_format("%s/%i%s", conf.dir_temp, nodo->id_nodo, tmpName);
				result = receive_entire_file_by_parts(socket, pathDest, MAX_PART_SIZE);

				if(result > 0) {
					list_add(listPathNodo, pathDest);
				} else {
					free(pathDest);
				}

				if(result == -2) {
					t_lost_temp* lost_temp = malloc(sizeof(t_lost_temp));
					lost_temp->id_nodo = nodo->id_nodo;
					lost_temp-> path = tmpName;
					list_add(lostTemps, lost_temp);
					result = 1;
				} else {
					free(tmpName);
				}
			} else {
				free(tmpName);
			}
		}
	}

	return result;
}

//----------------------------------------------------------------------------
void verify_local_temps(t_list*listPathNodo, t_list* lostTemps) {

	void _verify_local_temp(char* path) {
		struct stat stat_file;
		if( stat(path, &stat_file) == -1) {
			t_lost_temp* lost_temp = malloc(sizeof(t_lost_temp));
			lost_temp->id_nodo = conf.id;
			lost_temp->path = path;
			list_add(lostTemps,lost_temp);
		}
	}
	list_iterate(listPathNodo, (void *) _verify_local_temp);
}

//----------------------------------------------------------------------------
int realizar_Reduce(int socket) {

	int result = 1;
    uint32_t id;
    char* pathReduce = string_from_format("%s/reduce_script%i.sh",conf.dir_temp,last_script_increment_and_take());
    char* destino;
    t_list* listPathNodo = list_create();
    t_list* listNodosToConect = list_create();
    t_list* nodosRechazados = list_create();
    t_list* lostTemps = list_create();


    result = receive_reduce_instruction(socket, &id, &destino, listPathNodo, listNodosToConect, pathReduce);

	if((result > 0) && (id != conf.id)) {
		log_error(logger,"Id de Nodo incompatible...Se esperaba otro Nodo");
		send_protocol_in_order(socket,INCORRECT_NODO);

		free(pathReduce);
		free(destino);
		list_destroy_and_destroy_elements(listNodosToConect, (void *) free_reduce_nodo_dest);
		list_destroy_and_destroy_elements(listPathNodo, (void *) free);
		list_destroy_and_destroy_elements(nodosRechazados, (void *) free);
		list_destroy_and_destroy_elements(lostTemps, (void *) free_lost_temp);
		return -1;
	}

	if(result > 0) {
		verify_local_temps(listPathNodo, lostTemps);
	}

	if((result > 0) && (list_size(listNodosToConect) > 0)) {
		void _establecerConexionConNodo(t_reduce_nodo_dest* nodo_guest){
			conectWithNodoGuest(nodo_guest, nodosRechazados);
		}
		list_iterate(listNodosToConect,(void*) _establecerConexionConNodo );

		if (list_size(nodosRechazados) > 0) {
			log_error(logger,"No se pudo conectar con otros nodos");
			t_buffer* buffer_nodo_rechazado = buffer_create_with_protocol(NODO_NOT_FOUND);
			buffer_add_int(buffer_nodo_rechazado, list_size(nodosRechazados));

			void _buffer_add_id(uint32_t* idnodo) {
				buffer_add_int(buffer_nodo_rechazado, *idnodo);
			}
			list_iterate(nodosRechazados, (void *) _buffer_add_id);
			send_buffer_and_destroy(socket, buffer_nodo_rechazado);

			free(pathReduce);
			free(destino);
			list_destroy_and_destroy_elements(listNodosToConect, (void *) free_reduce_nodo_dest);
			list_destroy_and_destroy_elements(listPathNodo, (void *) free);
			list_destroy_and_destroy_elements(nodosRechazados, (void *) free);
			list_destroy_and_destroy_elements(lostTemps, (void *) free_lost_temp);

			return -1;
		}

		void _solicitarTmpsNodo(t_reduce_nodo_dest* self){
			result = (result > 0) ? solicitarTmpsNodo(self->socket_Nodo, self, listPathNodo, lostTemps) : result;
		}
		list_iterate(listNodosToConect, (void *) _solicitarTmpsNodo);
	}

	if(result > 0) {
		if(list_size(lostTemps) > 0) {
			log_error(logger,"No se pudo encontrar todos los temporales");
			t_buffer* buffer_temp_not_found = buffer_create_with_protocol(TEMP_NOT_FOUND);
			buffer_add_int(buffer_temp_not_found, list_size(lostTemps));

			void _buffer_add_lost_temp(t_lost_temp* lost_temp) {
				buffer_add_int(buffer_temp_not_found, lost_temp->id_nodo);
				buffer_add_string(buffer_temp_not_found, lost_temp->path);
			}
			list_iterate(lostTemps, (void *) _buffer_add_lost_temp);
			send_buffer_and_destroy(socket, buffer_temp_not_found);

			free(pathReduce);
			free(destino);
			list_destroy_and_destroy_elements(listNodosToConect, (void *) free_reduce_nodo_dest);
			list_destroy_and_destroy_elements(listPathNodo, (void *) free);
			list_destroy_and_destroy_elements(nodosRechazados, (void *) free);
			list_destroy_and_destroy_elements(lostTemps, (void *) free_lost_temp);
			return -1;
		}


		result = iniciar_Tarea_Reduce(pathReduce,destino, listPathNodo);

    	t_buffer* buffer_result_reduce;
    	if(result > 0) {
    		buffer_result_reduce = buffer_create_with_protocol(REDUCE_OK);
    	} else {
    		buffer_result_reduce = buffer_create_with_protocol(REDUCE_NOT_OK);
    	}
    	send_buffer_and_destroy(socket, buffer_result_reduce);
	}

	free(pathReduce);
	free(destino);
	list_destroy_and_destroy_elements(listNodosToConect, (void *) free_reduce_nodo_dest);
	list_destroy_and_destroy_elements(listPathNodo, (void *) free);
	list_destroy_and_destroy_elements(nodosRechazados, (void *) free);
	list_destroy_and_destroy_elements(lostTemps, (void *) free_lost_temp);
	return result;
}

//----------------------------------------------------------------------------
void free_reduce_nodo_dest(t_reduce_nodo_dest* self) {
	list_destroy_and_destroy_elements(self->path_temps, (void *) free);
	free(self);
}

//----------------------------------------------------------------------------
int enviar_Tmps_ToNodo (int socket){

	uint32_t cantidad;
	int result = 1;
	int i;
	char *nombre;
	t_list *paths = list_create();
	result = (result > 0) ? receive_int_in_order(socket,&cantidad): result;

	for(i=0;i < cantidad;i++) {
		result = (result > 0) ? receive_dinamic_array_in_order(socket,(void**)&nombre): result;
		list_add(paths,nombre);
	}

	if(result > 0) {
		void _sendPath_tmp (char* filePath){
			char* complete_filePath = string_from_format("%s/%s",conf.dir_temp,filePath);
			result = (result > 0) ? send_stream_with_size_in_order(socket,filePath, strlen(filePath)+1) : result;
			result = (result > 0) ? send_entire_file_by_parts(socket,complete_filePath,MAX_PART_SIZE) : result;
			free(complete_filePath);
			if(result == -2) {
				log_warning(logger,"No se encontró el temporal %s", filePath);
				result = 1;
			}
		}
		list_iterate(paths,(void*)_sendPath_tmp);
	}
	free(paths);
	free(nombre);
	return result;
}

//----------------------------------------------------------------------------
int solicitarConexionConNodo(char* ip_nodo, uint32_t puerto_nodo, uint32_t id_nodo) {

		log_debug(logger, "Solicitando conexión con nodo ID: %i...", id_nodo);
		int socketNodo = solicitarConexionCon(ip_nodo, puerto_nodo);

		if (socketNodo != -1) {
			log_info(logger, "Conexión con nodo establecida IP: %s, Puerto: %i", ip_nodo, puerto_nodo);
		} else {
			log_error(logger, "Conexión con nodo FALLIDA! IP: %s, Puerto: %i", ip_nodo, puerto_nodo);
		}

		return socketNodo;
}

