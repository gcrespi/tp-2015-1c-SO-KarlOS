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
#include "../../kbitarray/kbitarray.h"

//************************* Campos De Archivo Conf ************************
typedef struct {
	int puerto_listen;
	char* ip_fs;
	int puerto_fs;
} t_conf_MaRTA;

//************************* Estructura Por cada Job Conectado ************************
typedef struct {
	int sockfd;
	pthread_t* thr;
	struct sockaddr_in socketaddr_cli;
} t_hilo_job;

typedef struct {
	int id_job;
	int combiner;
	char* path_result;
	char** paths_files;
} t_info_job;

typedef struct {
	int id_file;
	t_kbitarray* bloques_mapeados;
	pthread_mutex_t mutex;
} t_info_file;

typedef struct {
	int id_temp;
	char* path;
	int id_nodo;
	char result_from_map; //= 1 es el resultado de un map, es el resultado de un reduce
	int id_origen; //id del bloque del cual resulto o ids de los temp del que resulto
} t_temp;


//************************* Estructura Por cada Map ************************
typedef struct {


} t_hilo_map;

//************************* Estructura de Carga de los Nodos (GLOBAL) ************************
typedef struct {
	int id_nodo;
	in_addr_t ip_nodo; //XXX ???
	int puerto_nodo; //XXX ???
	int cant_ops_en_curso;
//	int cant_op_map_en_curso;
//	int cant_op_reduce_en_curso;
} t_nodo_carga;

//************************ Prototipos ******************************
void levantar_arch_conf_marta(t_conf_MaRTA* conf);
void hilo_conex_job(t_hilo_job *job);
void terminar_hilos(); //XXX
int programa_terminado(); //XXX provisorio, en la entrega no se usa
int increment_and_take_last_job_id();

void free_info_job(t_info_job info_job);
int receive_info_job(t_hilo_job* job, t_info_job* info_job);

int solicitar_info_de_archivo(char *path_file, t_info_file* info_file);
int get_info_files_from_FS(char **paths_files, t_list* file_list);
void free_info_file(t_info_file* self);
void mostrar_info_file(t_info_file* self);

void hilo_conex_job(t_hilo_job *job);
void esperar_finalizacion_hilo_conex_job(t_hilo_job* job);
void liberar_job(t_hilo_job* job);

void free_conf_marta(t_conf_MaRTA* conf);
void init_var_globales();
void end_var_globales();


//************************ Variables Globales ******************************
pthread_mutex_t mutex_cerrar_marta;
int cerrar_marta = 0;

pthread_mutex_t mutex_ultimo_id;
int ultimo_id = 0;

sem_t conex_fs_ready;//XXX mutex??
int socket_fs;

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
void free_info_job(t_info_job info_job) {
	free(info_job.path_result);
	free_string_splits(info_job.paths_files);
}

//---------------------------------------------------------------------------
int receive_info_job(t_hilo_job* job, t_info_job* info_job) {

//	t_info_job info_job;
	char* aux_paths_to_apply_files;
	int result = 1;

	info_job->id_job = increment_and_take_last_job_id();
	log_debug(paranoid_log, "Info De Job: ID: %i", info_job->id_job);

	result = (result > 0) ? recibir(job->sockfd, &(info_job->combiner)) : result;
	if (result > 0) {
		log_debug(paranoid_log, "Info De Job Recibida: Combiner: %i", info_job->combiner);
	}

	result = (result > 0) ? recibir_dinamic_buffer(job->sockfd, (void **) &(aux_paths_to_apply_files)) : result;
	if (result > 0) {
		log_debug(paranoid_log, "Info De Job Recibida: Archivos: %s", aux_paths_to_apply_files);
	}

	result = (result > 0) ? recibir_dinamic_buffer(job->sockfd, (void **) &(info_job->path_result)) : result;
	if (result > 0) {
		info_job->paths_files = string_split(aux_paths_to_apply_files, ",");
		log_debug(paranoid_log, "Info De Job Recibida: Resultado: %s", info_job->path_result);
	} else {
		info_job->paths_files = malloc(sizeof(char *));
		*(info_job->paths_files) = NULL;
	}

	free(aux_paths_to_apply_files);

	return result;
}

//---------------------------------------------------------------------------
int solicitar_info_de_archivo(char *path_file, t_info_file* info_file) {

	int cant_bloques;
	int result = 1;

	log_debug(paranoid_log, "Pidiendo info del file: %s", path_file);

	sem_wait(&conex_fs_ready);
	result = (result > 0) ? enviar_protocolo(socket_fs, INFO_ARCHIVO) : result;

	result = (result > 0) ? recibir(socket_fs, &(info_file->id_file)) : result;
	result = (result > 0) ? recibir(socket_fs, &cant_bloques) : result;

	sem_post(&conex_fs_ready);

	if (result <= 0) {
		log_error(paranoid_log, "No se Pudo tomar info del Archivo: %s", path_file);
	} else {
		info_file->bloques_mapeados = kbitarray_create_and_clean_all(cant_bloques);
	}

	return result;
}

//---------------------------------------------------------------------------
int get_info_files_from_FS(char **paths_files, t_list* file_list) {

	int i, result = 1;
	t_info_file* info_file;

	for (i = 0; (paths_files[i] != NULL) && (result > 0); i++) {
		info_file = malloc(sizeof(t_info_file));
		result = solicitar_info_de_archivo(paths_files[i], info_file);

		if (result > 0) {
			pthread_mutex_init(&(info_file->mutex), NULL);
			list_add(file_list, info_file);
		} else {
			free(info_file);
		}

	}

	if (result > 0) {
		log_debug(paranoid_log, "Info de archivos solicitados con exito");
		list_iterate(file_list, (void *) mostrar_info_file);
	}
	return result;
}

//---------------------------------------------------------------------------
void free_info_file(t_info_file* self) {

	kbitarray_destroy(self->bloques_mapeados);
	pthread_mutex_destroy(&(self->mutex));
	free(self);

}

//---------------------------------------------------------------------------
void free_temp(t_temp* self) {

	free(self);

}

//---------------------------------------------------------------------------
void mostrar_info_file(t_info_file* self) {

	int cant = kbitarray_get_size_in_bits(self->bloques_mapeados);
	int mapeados = kbitarray_amount_bits_set(self->bloques_mapeados);

	log_debug(paranoid_log, "ID Archivo: %i Cantidad Bloques: %i Mapeados: %i", self->id_file, cant, mapeados);
}

//---------------------------------------------------------------------------
void free_hilos_map(t_temp* self) {

	free(self);

}


//---------------------------------------------------------------------------
int plan_maps(t_info_job info_job,t_list* file_list, t_list* temp_list) {

	t_list* hilos_map = list_create();
	int result = 1;

	off_t block_without_map;
	int file_without_map;

	int _find_block_without_map_and_lock_it(t_info_file* info_file) {

		pthread_mutex_lock(&(info_file->mutex));
		block_without_map = kbitarray_find_first_clean(info_file->bloques_mapeados);
		kbitarray_set_bit(info_file->bloques_mapeados,block_without_map);
		file_without_map = info_file->id_file;
		pthread_mutex_unlock(&(info_file->mutex));
		return (block_without_map != -1);

	}

	while((list_find(file_list,(void *)_find_block_without_map_and_lock_it) != NULL) && (result > 0)) {
//		log_info(paranoid_log,"Planificando map del Archivo:%i Bloque: %i ",file_without_map,block_without_map);
	}

	//TODO por acá dejé, hay que evaluar que tan conveniente sería tener hilos por cada rutina de map planificada


	list_destroy_and_destroy_elements(hilos_map,(void *)free_hilos_map);
	return 1;
}


//---------------------------------------------------------------------------
int plan_reduces() {
	return 1;
}


//---------------------------------------------------------------------------
int save_result_file_in_MDFS() {
	return 1;
}

//---------------------------------------------------------------------------
void hilo_conex_job(t_hilo_job *job) {
	char *ip_job;
	int port_job;
	t_info_job info_job;
	t_list* file_list = list_create();
	t_list* temp_list = list_create();

	int result = 0;

	getFromSocketAddrStd(job->socketaddr_cli, &ip_job, &port_job);

	log_info(paranoid_log, "Comienzo Hilo Job");

	uint32_t prot = recibir_protocolo(job->sockfd);

	switch (prot) {

	case NUEVO_JOB:
		log_info(paranoid_log, "Obteniendo New Job del Job IP: %s, Puerto: %i", ip_job, port_job);
		result = 1;
		result = (result > 0) ? receive_info_job(job, &info_job) : result;
		result = (result > 0) ? get_info_files_from_FS(info_job.paths_files, file_list) : result;
		result = (result > 0) ? plan_maps(info_job,file_list,temp_list) : result;
		result = (result > 0) ? plan_reduces() : result;
		result = (result > 0) ? save_result_file_in_MDFS() : result;

		//solicitar_info_de_bloque_FS(unDeterminadoBloqueDeArchivo);
		//solicitar_info_de_Nodo(); (si no la tengo ya)
		//calcular_cual_es_proxima_tarea()
		//si es un map buscar el bloque donde mejor se hace
		//si es un reduce ...
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "Job se desconectó de forma inesperada");
		break;

	case -1:
		log_error(paranoid_log, "No se pudo recibir new Job del Job");
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado");
		break;
	}

	if (result > 0) {
		enviar_protocolo(job->sockfd, FINISHED_JOB);
		log_info(paranoid_log, "Finalizado con éxito Job IP: %s, Puerto: %i", ip_job, port_job);
	} else {
		enviar_protocolo(job->sockfd, ABORTED_JOB);
		log_error(paranoid_log, "No se pudo realizar el Job IP: %s, Puerto: %i", ip_job, port_job);
	}

	free(ip_job);
	free_info_job(info_job);
	list_destroy_and_destroy_elements(file_list, (void *) free_info_file);
	list_destroy_and_destroy_elements(temp_list, (void *) free_temp);
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

	sem_init(&conex_fs_ready, 0, 1);

	paranoid_log = log_create("./logMaRTA.log", "MaRTA", 1, LOG_LEVEL_TRACE);
}

//--------------------------------------------------------------------------
void end_var_globales() {

	pthread_mutex_destroy(&mutex_cerrar_marta);
	pthread_mutex_destroy(&mutex_ultimo_id);

	sem_destroy(&conex_fs_ready);

	log_destroy(paranoid_log);
}

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

	log_info(paranoid_log, "Conectando con FS...");
	if ((socket_fs = solicitarConexionCon(conf.ip_fs, conf.puerto_fs)) == -1) {
		log_error(paranoid_log, "No se pudo Conectar al FS");
		exit(-1);
	}
	log_info(paranoid_log, "Conectado a FS");

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

		log_info(paranoid_log, "Creación de Hilo Job");
		if (pthread_create(job->thr, NULL, (void *) hilo_conex_job, job) != 0) {
			log_error(paranoid_log, "No se pudo crear Hilo Job");
			exit(-1);
		}

		hilos_de_job++;

		if (hilos_de_job >= 3) { //XXX
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
