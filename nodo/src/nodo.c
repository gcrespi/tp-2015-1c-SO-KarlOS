/*
 ============================================================================
 Name        : nodo.c
 Author      : karlOS
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

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
#include <semaphore.h>
#include "../../connectionlib/connectionlib.h"

// Estructuras
// La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	int id;
	int nodo_nuevo;
	int cant_bloques;
};

//XXX Estructura que enviaría job a nodo al conectarse
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

//Variables Globales
struct conf_nodo conf; // estructura que contiene la info del arch de conf
char *data; // data del archivo mapeado
#define block_size 4*1024 // tamaño de cada bloque del dat
t_log* logger;
sem_t semaforo1;
sem_t semaforo2;
pthread_t thread1, thread2, thread3;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t* mutex;

//Prototipos
void levantar_arch_conf_nodo(); // devuelve una estructura con toda la info del archivo de configuracion "nodo.cfg"
void setNodoToSend(struct info_nodo *); // setea la estructura que va a ser enviada al fs al iniciar el nodo
int enviar_info_nodo(int, struct info_nodo*);
void free_conf_nodo();
void mapearArchivo();
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
void free_hilo_job();
void esperar_finalizacion_hilo_conex_job(t_hilo_job*);
//void terminar_hilos();
void solicitarConexionConNodo(int *);
int obtener_puerto_job();
void obtener_hilos_jobs(int, t_list*);


//Main####################################################################################################
int main(void) {
	int socket_fs,listener_job;

	t_list* lista_jobs;

	if((sem_init(&semaforo1, 0, 1))==-1){
		perror("semaphore");
		exit(1);
	}

	logger = log_create("nodo.log", "NODO", 1, LOG_LEVEL_TRACE);

	levantar_arch_conf_nodo();

    inicializar_mutexs();

	struct info_nodo info_envio;
	setNodoToSend(&info_envio);

	mapearArchivo();

    socket_fs = solicitarConexionConFileSystem(conf);

	if (enviar_info_nodo(socket_fs, &info_envio) <= 0) {
		log_error(logger, "no se pudo enviar el info nodo");
	} else {
		log_info(logger, "Se envio correctamente info nodo");
	}


	if ((pthread_create( &thread2, NULL,(void *)esperar_instrucciones_del_filesystem, &socket_fs))== -1){
		perror("fallo en el: thread 2");
			exit(1);
	}

	lista_jobs = list_create();

	listener_job = obtener_puerto_job();

	obtener_hilos_jobs(listener_job, lista_jobs);

	list_iterate(lista_jobs, (void *) esperar_finalizacion_hilo_conex_job);
	pthread_join(thread2, NULL);
	free_conf_nodo();
	finalizar_mutexs();
	free (lista_jobs);
	log_destroy(logger);
	return EXIT_SUCCESS;
}

//#########################################################################################################
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
	int hilos_de_job = 0;
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

		  if (receive_new_client_job(job->sockfd)) {
			  list_add(lista_jobs, job);

		      log_info(logger, "Creación de Hilo Job...");
		      if (pthread_create(job->thr, NULL, (void *) esperar_instrucciones_job, &(job->sockfd)) != 0) {
		    	  log_error(logger, "No se pudo crear Hilo Job");
			      exit(-1);
		      }

		      hilos_de_job++;
		  } else {
			  free_hilo_job(job);
		  }
		  if (hilos_de_job >= 3) { //XXX
			  bandera = 0;
		  }
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
void solicitarConexionConNodo(int *socket_otro_nodo){

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

		case READ_RESULT_JOB: //XXX
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
	log_info(logger, "Esperando Instruccion Job");

	do{
		tarea = receive_protocol_in_order(*socket_job);
		switch (tarea) {
		case ORDER_MAP:
			if (recibir_Bloque(*socket_job) <=0) {//Aca debe ir la función encargada de realizar el map
				log_error(logger, "no se pudo realizar rutina de map");
			}
			else {
				log_info(logger, "Se realizó correctamente la rutina map");
			}
			break;
		case ORDER_REDUCE:
			if (enviar_bloque(*socket_job) <=0){//Acá la función que realizaría el reduce
				log_error(logger, "no se pudo realizar rutina reduce");
			}
			else {
				log_info(logger, "Se realizó correctamente la rutina reduce");
			}
			break;

		default:
			return -1;
		}
	} while(tarea != DISCONNECTED); //Aca no se, el job en algun momento se desconecta? Tal vez el protocolo (tarea) sea otro de los de la conectionlib.h
	return tarea;
}
//---------------------------------------------------------------------------

int recibir_Bloque(int socket) {

	int result = 1;
    uint32_t nroBloque;
    int longInfo;

    result = (result > 0) ? receive_int_in_order(socket, &nroBloque) : result;
	pthread_mutex_lock( &mutex[nroBloque] );
	result = (result > 0) ? longInfo=receive_static_array_in_order(socket, &data[nroBloque*block_size]) : result;
	if(result > 0) {
		data[nroBloque*block_size + longInfo]='\0';
	}
	pthread_mutex_unlock( &mutex[nroBloque] );
	return result;
}

//---------------------------------------------------------------------------

int enviar_bloque(int socket) {

	int result = 1;
    uint32_t nroBloque;

    result = (result > 0) ? receive_int_in_order(socket, &nroBloque) : result;
    uint32_t largoBloque = strlen(&data[nroBloque*block_size]);

    pthread_mutex_lock(&mutex[nroBloque]);
	result = (result > 0) ? send_stream_with_size_in_order(socket, &data[nroBloque*block_size],largoBloque+1) : result;
	pthread_mutex_unlock(&mutex[nroBloque]);
	return result;
}
//---------------------------------------------------------------------------

int enviar_tmp(int socket) {

	int result = 1;
    char* nombreArchivo;
    char* path_completo;
    char *tmp;

	result = (result > 0) ? receive_dinamic_array_in_order(socket,(void **) &nombreArchivo) : result;
	path_completo = string_from_format("%s/%s",conf.dir_temp,nombreArchivo);

	int fd;
	struct stat sbuf;
	log_info(logger, "inicio de mapeo");

	if ((fd = open(path_completo, O_RDWR)) == -1) {
		perror("open()");
		exit(1);
	}
	if (fstat(fd, &sbuf) == -1) {
		perror("fstat()");
	}
	tmp = mmap((caddr_t) 0, sbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED,
			fd, 0);
	if (tmp == (caddr_t) (-1)) {
		perror("mmap");
		exit(1);//XXX extraer a funcion, de verdad quiero mapear esto? o leer secuencial?
	}
	log_info(logger,"mapeo correcto");


	result = (result > 0) ? send_stream_with_size_in_order(socket, &tmp[0], sbuf.st_size) : result;
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
void mapearArchivo() {

	int fd;
	struct stat sbuf;
	char* path = conf.archivo_bin;

	log_info(logger, "inicio de mapeo");

	if ((fd = open(path, O_RDWR)) == -1) {
		perror("open()");
		exit(1);
	}

	if (fstat(fd, &sbuf) == -1) {
		perror("fstat()");
	}
   sem_wait(&semaforo1);
	data = mmap((caddr_t) 0, sbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED,
			fd, 0);

	if (data == (caddr_t) (-1)) {
		perror("mmap");
		exit(1);
	}
	sem_post(&semaforo1);
	log_info(logger,"mapeo correcto");
}
//---------------------------------------------------------------------------
void cargarBloque(int nroBloque, char* info, int offset_byte) {
	int pos_a_escribir = nroBloque * block_size + offset_byte;
	memcpy(data + pos_a_escribir, info, strlen(info));
	//data[pos_a_escribir+strlen(info)]='\0';

}
//---------------------------------------------------------------------------
void mostrarBloque(int nroBloque) {
	printf("info bloque: %s\n", &(data[nroBloque * block_size]));
}

//----------------------------------------------------------------------------
void inicializar_mutexs(void){
	int i;
	mutex = malloc(sizeof(pthread_mutex_t)*conf.cant_bloques);
	for(i=0; i<conf.cant_bloques; i++) {
		pthread_mutex_init(&mutex[i],NULL);
	}

}
//----------------------------------------------------------------------------
void finalizar_mutexs(void){
	int i;
	for(i=0; i<conf.cant_bloques; i++) {
		pthread_mutex_destroy(&mutex[i]);
	}
	free(mutex);

}

//---------------------------------------------------------------------------
void esperar_finalizacion_hilo_conex_job(t_hilo_job* jop) {
	void* ret_recep;

	if (pthread_join(*(jop->thr), &ret_recep) != 0) {
		printf("Error al hacer join del hilo\n");
	}
}

//----------------------------------------------------------------------------

