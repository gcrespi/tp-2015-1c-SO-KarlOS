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
//void solicitarConexionConFS(struct sockaddr_in*, struct info_nodo*); //conecta con el FS
int enviar_info_nodo(int, struct info_nodo*);
void free_conf_nodo();
void mapearArchivo();
void cargarBloque(int, char*, int);
void mostrarBloque(int);
int esperar_instrucciones_del_filesystem(int*);
int solicitarConexionConFileSystem(struct conf_nodo);
int recibir_Bloque(int);
int enviar_bloque(int);

//Main
int main(void) {
	int socket_fs,i,listener_job; // file descriptor del FS

	logger = log_create("nodo.log", "NODO", 1, LOG_LEVEL_TRACE);

	levantar_arch_conf_nodo();

	mutex = malloc(sizeof(pthread_mutex_t)*conf.cant_bloques);

	for(i=0; i<conf.cant_bloques; i++) {
		pthread_mutex_init(&mutex[i],NULL);
	}


	if((sem_init(&semaforo1, 0, 1))==-1){
			perror("semaphore");
			exit(1);
		}




	struct info_nodo info_envio;
	setNodoToSend(&info_envio);

    socket_fs = solicitarConexionConFileSystem(conf);

    mapearArchivo();

	//log_debug(logger,"id: %i",info_envio.cant_bloques);

	if (enviar_info_nodo(socket_fs, &info_envio) <= 0) {
		log_error(logger, "no se pudo enviar el info nodo");
	} else {
		log_info(logger, "Se envio correctamente info nodo");
	}


	if ((pthread_create( &thread2, NULL,(void *)esperar_instrucciones_del_filesystem, &socket_fs))== -1){
		perror("fallo en el: thread 2");
			exit(1);
	}

	log_debug(logger, "Obteniendo Puerto para Escuchar Jobs...");
	if ((listener_job = escucharConexionesDesde("", conf.puerto_nodo)) == -1) {
		log_error(logger, "No se pudo obtener Puerto para Escuchar Jobs");
		exit(-1);
	} else {
		log_debug(logger, "Puerto para Escuchar Jobs Obtenido");
	}

	//esperar_instrucciones_job();

	free_conf_nodo();

	pthread_join(thread2, NULL);

	for(i=0; i<conf.cant_bloques; i++) {
		pthread_mutex_destroy(&mutex[i]);
	}
	free(mutex);

	log_destroy(logger);
	return EXIT_SUCCESS;
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


//---------------------------------------------------------------------------

int esperar_instrucciones_del_filesystem(int *socket){

	uint32_t tarea;
	log_info(logger, "Esperando Instruccion FS");
	//tarea = recibir_protocolo(*socket);

    do{

    	tarea = recibir_protocolo(*socket);

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
//			path_completo = string_from_format("%s/%s",path_temporales,nombre_result);

//			free(path_completo);
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
    int nroBloque;
    int longInfo;
		result = (result > 0) ? recibir(socket, &nroBloque) : result;
		pthread_mutex_lock( &mutex[nroBloque] );
		result = (result > 0) ? longInfo=recibir(socket, &data[nroBloque*block_size]) : result;
		data[nroBloque*block_size + longInfo]='\0';
		pthread_mutex_unlock( &mutex[nroBloque] );
		return result;
}

//---------------------------------------------------------------------------

int enviar_bloque(int socket) {

	int result = 1;
    int nroBloque;
		result = (result > 0) ? recibir(socket, &nroBloque) : result;
		pthread_mutex_lock(&mutex[nroBloque]);
		result = (result > 0) ? enviar_string(socket, &data[nroBloque*block_size]) : result;
		pthread_mutex_unlock(&mutex[nroBloque]);
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
