/*
 ============================================================================
 Name        : marta.c
 Author      : KarlOS
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include <commons/string.h>
#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/log.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include "../../connectionlib/connectionlib.h"

//************************* Campos De Archivo Conf ************************
typedef struct {
	int puerto_listen;
	char* ip_fs;
	int puerto_fs;
}t_conf_MaRTA;

//************************* Estructura Por cada Job Conectado ************************
typedef struct {
	int sockfd;
	pthread_t* thr;
	struct sockaddr_in socketaddr_cli;
}t_hilo_job;


typedef struct {
	int id_job;
	int combiner;
	char* path_result;
	t_list file_list;
	char** paths_files;
}t_info_job;

typedef struct {
	int id_file;
//	char* path_file;
//	int cantBloques;
	t_bitarray* bloques_mapeados;
}t_info_file;

typedef struct {
	int id_temp;
	char* path;
	int id_nodo;
	char result_from_map; //= 1 es el resultado de un map, es el resultado de un reduce
	int id_origen; //id del bloque del cual resulto o ids de los temp del que resulto
} t_temp;


//************************* Estructura de Carga de los Nodos (GLOBAL) ************************
typedef struct {
	int id_nodo;
	in_addr_t ip_nodo;
	int puerto_nodo;
	int cant_op_map_en_curso;
	int cant_op_reduce_en_curso;
}t_nodo_carga;

//************************ Prototipos ******************************
void levantar_arch_conf_marta(t_conf_MaRTA* conf);
void hilo_conex_job(t_hilo_job *job);

//************************ Variables Globales ******************************
pthread_mutex_t mutex_cerrar_marta;
int cerrar_marta = 0;

pthread_mutex_t mutex_ultimo_id;
int ultimo_id = 0;

sem_t conex_fs_ready;
int socket_fs;
pthread_t thr_conex_FS;

t_log* paranoid_log;

//---------------------------------------------------------------------------
void levantar_arch_conf_marta(t_conf_MaRTA* conf) {

	char** properties = string_split("PUERTO_LISTEN,IP_FS,PUERTO_FS", ",");
	t_config* conf_arch = config_create("marta.cfg");

	if (has_all_properties(3, properties, conf_arch)) {
		conf->puerto_listen = config_get_int_value(conf_arch, properties[0]);

		conf->ip_fs = strdup(config_get_string_value(conf_arch, properties[1]));
		conf->puerto_fs = config_get_int_value(conf_arch, properties[2]);
	} else {
		log_error(paranoid_log, "Faltan propiedades en archivo de Configuración");
		exit(-1);
	}
	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------
void terminar_hilos() {

	pthread_mutex_lock(&mutex_cerrar_marta);
	cerrar_marta = 1;
	pthread_mutex_unlock(&mutex_cerrar_marta);

}

//---------------------------------------------------------------------------
int programa_terminado() {
	int endLocal;

	pthread_mutex_lock(&mutex_cerrar_marta);
	endLocal = cerrar_marta;
	pthread_mutex_unlock(&mutex_cerrar_marta);

	return endLocal;
}

//---------------------------------------------------------------------------
int increment_and_take_last_job_id() {

	int aux_last_job_id;

	pthread_mutex_lock(&mutex_ultimo_id);
	aux_last_job_id = ++ultimo_id;
	pthread_mutex_unlock(&mutex_ultimo_id);

	return aux_last_job_id;
}


//---------------------------------------------------------------------------
void crear_hilo_para_un_job(t_hilo_job* job) {

	log_info(paranoid_log, "Creación de Hilo Job");
	if (pthread_create(job->thr, NULL, (void *) hilo_conex_job, job) != 0) {
		log_error(paranoid_log, "No se pudo crear Hilo Job");
		exit(-1);
	}
}


//---------------------------------------------------------------------------
void free_info_job(t_info_job info_job) {
	free(info_job.path_result);
	free_string_splits(info_job.paths_files);
}

//---------------------------------------------------------------------------
int recibir_info_job(t_hilo_job* job, t_info_job* info_job) {

//	t_info_job info_job;
	char* aux_paths_to_apply_files;
	int result = 0;

	info_job->id_job = increment_and_take_last_job_id();
	log_debug(paranoid_log,"Info De Job: ID: %i",info_job->id_job);


	result = (result != -1) ? recibir(job->sockfd, &(info_job->combiner)) : result;
	if(result != -1) {
		log_debug(paranoid_log,"Info De Job Recibida: Combiner: %i",info_job->combiner);
	}

	result = (result != -1) ? recibir_dinamic_buffer(job->sockfd, (void **) &(aux_paths_to_apply_files)) : result;
	if(result != -1) {
		log_debug(paranoid_log,"Info De Job Recibida: Archivos: %s",aux_paths_to_apply_files);
	}

	result = (result != -1) ? recibir_dinamic_buffer(job->sockfd, (void **) &(info_job->path_result)) : result;
	if(result != -1) {
		info_job->paths_files = string_split(aux_paths_to_apply_files,",");
		log_debug(paranoid_log,"Info De Job Recibida: Resultado: %s",info_job->path_result);
	} else {
		info_job->paths_files = malloc(sizeof(int));
		*(info_job->paths_files) = NULL;
	}


	free(aux_paths_to_apply_files);

	return result;
}


//---------------------------------------------------------------------------
t_bitarray* crear_bitarray_limpio(int cant_bits)
{
	t_bitarray* self;
	char* bitarray;
	int i;
	int cant_chars= cant_bits / 8;
	if(cant_bits % 8 != 0) {
		cant_chars++;
	}

	bitarray = malloc(cant_chars*sizeof(char));

	for(i=0; i<cant_chars; i++) {
		bitarray[i]=0;
	}

	self = bitarray_create(bitarray,cant_bits);


	return self;
}


//---------------------------------------------------------------------------
void free_bitarray(t_bitarray* self) {
	free(self->bitarray);
	bitarray_destroy(self);
}


//---------------------------------------------------------------------------
int solicitar_info_de_archivo(char *path_file, t_info_file* info_file) {

	int cant_bloques;
	int result = 1;

	log_debug(paranoid_log,"Pidiendo info del file: %s",path_file);

	sem_wait(&conex_fs_ready);
	result = (result > 0) ? enviar_protocolo(socket_fs, INFO_ARCHIVO) : result;

	result = (result > 0) ? recibir(socket_fs, &(info_file->id_file)) : result;
	result = (result > 0) ? recibir(socket_fs, &cant_bloques) : result;

	sem_post(&conex_fs_ready);
	log_debug(paranoid_log,"Holaaaa",path_file);

	if(result <= 0) {
		log_error(paranoid_log,"No se Pudo tomar info del Archivo: %s",path_file);
	} else {
		info_file->bloques_mapeados = crear_bitarray_limpio(cant_bloques);
	}

	return result;
}

//---------------------------------------------------------------------------
int solicitar_info_de_archivos_a_FS(char **paths_files,t_list* file_list) {

	int i,result=0;
	t_info_file* info_file;

	for(i=0; (paths_files[i]!=NULL) && (result != -1); i++) {
		info_file = malloc(sizeof(t_info_file));
		result = solicitar_info_de_archivo(paths_files[i],info_file);

		if(result > 0) {
			list_add(file_list,info_file);
		} else {
			free(info_file);
		}

	}


	return result;
}

//---------------------------------------------------------------------------
void free_info_file(t_info_file* info_file) {

	free_bitarray(info_file->bloques_mapeados);
	free(info_file);

}

//---------------------------------------------------------------------------
void hilo_conex_job(t_hilo_job *job) {
	char *ip_job;
	int port_job;
	t_info_job info_job;
	t_list* file_list;


	int result_OK = 0;

	getFromSocketAddrStd(job->socketaddr_cli, &ip_job, &port_job);

	log_info(paranoid_log, "Comienzo Hilo Job");

	uint32_t prot = recibir_protocolo(job->sockfd);

	switch (prot) {

	case NUEVO_JOB:
		log_info(paranoid_log,"Obteniendo New Job del Job IP: %s, Puerto: %i",ip_job,port_job);
		if(recibir_info_job(job, &info_job)!= -1) {
			file_list = list_create();
			if(solicitar_info_de_archivos_a_FS(info_job.paths_files,file_list) != -1) {
				log_debug(paranoid_log,"Info de archivos solicitados con exito");
			} else {
				break;
			}
			//solicitar_info_de_bloque_FS(unDeterminadoBloqueDeArchivo);
			//solicitar_info_de_Nodo(); (si no la tengo ya)
			//calcular_cual_es_proxima_tarea()
			//si es un map buscar el bloque donde mejor se hace
			//si es un reduce ...

			list_destroy_and_destroy_elements(file_list,(void *) free_info_file);
			result_OK = 1;
		}
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "Job se desconectó de forma inesperada");
		break;

	case -1:
		log_error(paranoid_log, "No se pudo recivir new Job del Job");
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado");
		break;
	}

	if(result_OK) {
		enviar_protocolo(job->sockfd, FINISHED_JOB);
		log_info(paranoid_log, "Finalizado con éxito Job IP: %s, Puerto: %i",ip_job,port_job);
	} else {
		enviar_protocolo(job->sockfd, ABORTED_JOB);
		log_error(paranoid_log,"No se pudo realizar el Job IP: %s, Puerto: %i",ip_job,port_job);
	}

	sleep(10); //XXX


	free(ip_job);

	free_info_job(info_job);
}

//---------------------------------------------------------------------------
void esperar_finalizacion_hilo_conex_job(t_hilo_job* job) {
	void* ret_recep;

	if (pthread_join(*job->thr, &ret_recep) != 0) {
		printf("Error al hacer join del hilo\n");
	}
}

//-------------------------------------------------------------------
void liberar_job(t_hilo_job* job) {
	free(job->thr);
	free(job);
}

//---------------------------------------------------------------------------
void free_conf_marta(t_conf_MaRTA* conf) {
	free(conf->ip_fs);
}


//---------------------------------------------------------------------------
void init_var_globales() {

	pthread_mutex_init(&mutex_cerrar_marta, NULL);
	pthread_mutex_init(&mutex_ultimo_id, NULL);

	sem_init(&conex_fs_ready,0,1);

	paranoid_log = log_create("./logMaRTA.log", "MaRTA", 1, LOG_LEVEL_TRACE);
}


//--------------------------------------------------------------------------
void end_var_globales() {

	pthread_mutex_destroy(&mutex_cerrar_marta);
	pthread_mutex_destroy(&mutex_ultimo_id);

	sem_destroy(&conex_fs_ready);

	log_destroy(paranoid_log);
}


////-----------------------------------------------------------------------------
//void abrir_conexion_con_FS(t_conf_MaRTA* conf_marta) {
//
//	int socket = 0;
//	int segundos_reconexion= 5;
//	int reconexiones = 0;
//	const int max_reconexiones = 10;
//
//	socket = solicitarConexionCon(conf_marta->ip_fs,conf_marta->puerto_fs);
//
//	while((socket<=0) && (reconexiones <= max_reconexiones))
//	{
//		log_error(paranoid_log,"Error No se pudo Conectar al File System, volviendo a intentar en %i segundos",segundos_reconexion);
//		sleep(segundos_reconexion);
//		segundos_reconexion+= 5;
//		reconexiones++;
//
//		socket = solicitarConexionCon(conf_marta->ip_fs,conf_marta->puerto_fs);
//	}
//
//	if(socket >0)
//	{
//		socket_fs = socket;
//		log_info(paranoid_log,"Conexion con FS establecida IP: %s, PORT: %i",conf_marta->ip_fs,conf_marta->puerto_fs);
//		sem_post(&conex_fs_ready);
//	}
//	else
//	{
//		log_error(paranoid_log,"Demasiadas Reconexiones Sistema Bloqueado");//XXX al parecer no es necesaria una reconexion
//	}
//}


//###########################################################################
int main(void) {

	init_var_globales();


	t_conf_MaRTA conf;
	levantar_arch_conf_marta(&conf); //Levanta el archivo de configuracion "marta.cfg"
	int hilos_de_job = 0;

	int listener_jobs;

	t_list* lista_jobs;
	t_hilo_job* job;

	lista_jobs = list_create();


//	log_info(paranoid_log, "Creando Hilo Para Conectar FS");
//	if (pthread_create(&thr_conex_FS, NULL, (void *) abrir_conexion_con_FS, &conf) != 0) {
//		log_error(paranoid_log, "No se pudo crear Hilo Para Conectar FS");
//		exit(-1);
//	}

	if((socket_fs = solicitarConexionCon(conf.ip_fs,conf.puerto_fs)) == -1) {
		log_error(paranoid_log,"No se pudo Conectar al FS");
		exit(-1);
	}



	log_debug(paranoid_log, "Obteniendo Puerto para Escuchar Jobs...");
	if ((listener_jobs = escucharConexionesDesde("", conf.puerto_listen)) == -1) {
		log_error(paranoid_log, "No se pudo obtener Puerto para Escuchar Jobs");
		exit(-1);
	} else {
		log_debug(paranoid_log, "Puerto para Escuchar Jobs Obtenido");
	}

	while (!programa_terminado()) {
		job = malloc(sizeof(t_hilo_job));
		job->thr = malloc(sizeof(pthread_t));

		list_add(lista_jobs, job);

		if ((job->sockfd = aceptarCliente(listener_jobs, &(job->socketaddr_cli))) == -1) {
			log_error(paranoid_log, "Error al Aceptar Nuevos Jobs");
			exit(-1);
		}

		crear_hilo_para_un_job(job);

		hilos_de_job++;

		if (hilos_de_job >= 3) {//XXX
			terminar_hilos();
		}
	}

	log_info(paranoid_log, "Esperando finalizacion de hilos de Job");
	list_iterate(lista_jobs, (void *) esperar_finalizacion_hilo_conex_job);

	list_destroy_and_destroy_elements(lista_jobs, (void *) liberar_job);

	free_conf_marta(&conf);

	end_var_globales();

	return EXIT_SUCCESS;
}
