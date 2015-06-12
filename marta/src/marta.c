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
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/log.h>
#include "../../connectionlib/connectionlib.h"

//########################################  Estructuras  #########################################

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
	uint32_t combiner;
	char* path_result;
	char** paths_files;
} t_info_job;

typedef struct {
	uint32_t id_file;
	uint32_t amount_blocks;
} t_info_file;

typedef struct {
	uint32_t id_temp;
	char* path;
	uint32_t id_nodo;
	uint32_t id_file_origin;
	uint32_t block_origin;
//ip_nodo y puerto_nodo?
//	char result_from_map; //= 1 es el resultado de un map, es el resultado de un reduce
//	int id_origen; //id del bloque del cual resulto o ids de los temp del que resulto
} t_temp_map;

//************************* Estructura Por cada Map ************************

typedef struct {
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	uint32_t block;
} t_block_copy;

typedef struct {
	uint32_t id_map;
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	uint32_t block;
	char* temp_file_name;
} t_map_dest;

typedef struct {
	t_info_file* file;
	uint32_t block;
	t_map_dest* map_dest;
} t_pending_map;

typedef struct {
	uint32_t prot;
	uint32_t id_map;
} t_result_map;

//************************* Estructura de Carga de los Nodos (GLOBAL) ************************
typedef struct {
	uint32_t id_nodo;
	uint32_t cant_ops_en_curso;
} t_carga_nodo;

//########################################  Prototipos  #########################################

//************************************* Manejo Estructuras **************************************

void levantar_arch_conf_marta(t_conf_MaRTA* conf);
void free_conf_marta(t_conf_MaRTA* conf);

void free_hilo_job(t_hilo_job* job);
void free_info_job(t_info_job info_job);
void free_info_file(t_info_file* self);
void free_temp_map(t_temp_map* self);

void free_map_dest(t_map_dest* self);
void free_pending_map(t_pending_map* self);

void free_carga_nodo(t_carga_nodo* self);

void mostrar_info_file(t_info_file* self);
void mostrar_map_dest(t_map_dest* md);

//************************************* Interaccion Job **************************************

int receive_new_client_job(int sockjob);
int receive_info_job(t_hilo_job* job, t_info_job* info_job);
int order_map_to_job(t_map_dest* map_dest, int socket);
int recibir_info_resultado();
int receive_result_map(int sockjob, t_result_map* result_map);

int send_finished_job(int socket_job);
int send_aborted_job(int socket_job);

//************************************* Interaccion FS **************************************
int receive_info_file(int socket, t_info_file* info_file);
int solicitar_info_de_archivo(char *path_file, t_info_file* info_file);
int locate_block_in_FS(t_info_job info_job, uint32_t id_file, uint32_t block_number, t_list* block_copies);
int obtenerAceptacionDeFS(int socket);

//***************************************** Principal *******************************************
void terminar_hilos(); //XXX
int programa_terminado(); //XXX provisorio, en la entrega no se usa

int increment_and_take_last_job_id();

int get_info_files_from_FS(char **paths_files, t_list* file_list);

t_map_dest* planificar_map(t_info_job info_job, uint32_t id_file, uint32_t block_number, uint32_t* last_id_map, t_list* temp_list);

int plan_maps(t_info_job info_job, t_list* file_list, t_list* temp_list, int sockjob);
int plan_reduces();
int save_result_file_in_MDFS();

void hilo_conex_job(t_hilo_job *job);
void esperar_finalizacion_hilo_conex_job(t_hilo_job* job);

void init_var_globales();
void end_var_globales();

//######################################  Variables Globales  #######################################
pthread_mutex_t mutex_cerrar_marta;
int cerrar_marta = 0;

pthread_mutex_t mutex_ultimo_id;
int last_id_job = 0;

pthread_mutex_t conex_fs_ready;
int socket_fs;

t_log* paranoid_log;

pthread_mutex_t node_list_mutex;
t_list* carga_nodos;

//######################################  Funciones  #######################################

//************************************* Manejo Estructuras **************************************

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
void free_info_job(t_info_job info_job) {
	free(info_job.path_result);
	free_string_splits(info_job.paths_files);
}

//---------------------------------------------------------------------------
void free_info_file(t_info_file* self) {
	free(self);
}

//---------------------------------------------------------------------------
void free_temp_map(t_temp_map* self) {
	free(self->path);
	free(self);

}

//---------------------------------------------------------------------------
void mostrar_info_file(t_info_file* self) {

	log_debug(paranoid_log, "ID Archivo: %i Cantidad Bloques: %i", self->id_file, self->amount_blocks);
}

//---------------------------------------------------------------------------
void mostrar_map_dest(t_map_dest* md) {

	log_debug(paranoid_log, "Map ID: %i, Nodo: ID: %i, IP: %i, Port: %i, Block: %i Temp:%s", md->id_map, md->id_nodo, md->ip_nodo,
			md->puerto_nodo, md->block, md->temp_file_name);
}

//---------------------------------------------------------------------------
void free_map_dest(t_map_dest* self) {
	free(self->temp_file_name);
	free(self);
}

//---------------------------------------------------------------------------
void free_block_copy(t_block_copy* self) {
	free(self);
}

//---------------------------------------------------------------------------
void free_pending_map(t_pending_map* self) {
	free_map_dest(self->map_dest);
	free(self);
}

//---------------------------------------------------------------------------
void free_carga_nodo(t_carga_nodo* self) {
	free(self);
}

//-------------------------------------------------------------------
void free_hilo_job(t_hilo_job* job) {
	free(job->thr);
	free(job);
}

//---------------------------------------------------------------------------
void free_conf_marta(t_conf_MaRTA* conf) {
	free(conf->ip_fs);
}

//***************************************** Interaccion Job *******************************************

//---------------------------------------------------------------------------
int increment_and_take_last_job_id() {

	int aux_last_job_id;

	pthread_mutex_lock(&mutex_ultimo_id);
	aux_last_job_id = ++last_id_job;
	pthread_mutex_unlock(&mutex_ultimo_id);

	return aux_last_job_id;
}

//---------------------------------------------------------------------------
int receive_new_client_job(int sockjob) {
	uint32_t prot = 0;

	prot = receive_protocol_in_order(sockjob);

	switch (prot) {

	case NUEVO_JOB:
		log_debug(paranoid_log, "Nuevo Job aceptado");
		return 1;
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "Job se desconectó de forma inesperada");
		break;

	case -1:
		log_error(paranoid_log, "No se pudo recibir new Job del Job");
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", prot);
		break;

	}

	return 0;
}

//---------------------------------------------------------------------------
int receive_info_job(t_hilo_job* job, t_info_job* info_job) {

	char* aux_paths_to_apply_files;
	int result = 1;

	info_job->id_job = increment_and_take_last_job_id();

	result = (result > 0) ? receive_int_in_order(job->sockfd, &(info_job->combiner)) : result;
	result = (result > 0) ? receive_dinamic_array_in_order(job->sockfd, (void **) &(aux_paths_to_apply_files)) : result;
	result = (result > 0) ? receive_dinamic_array_in_order(job->sockfd, (void **) &(info_job->path_result)) : result;

	if (result > 0) {
		log_debug(paranoid_log, "Info De Job: ID: %i", info_job->id_job);
		log_debug(paranoid_log, "Info De Job Recibida: Combiner: %i", info_job->combiner);
		log_debug(paranoid_log, "Info De Job Recibida: Archivos: %s", aux_paths_to_apply_files);
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
int order_map_to_job(t_map_dest* map_dest, int socket) {

	int result;

	mostrar_map_dest(map_dest);

	t_buffer* map_order = buffer_create_with_protocol(ORDER_MAP);

	buffer_add_int(map_order, map_dest->id_map);
	buffer_add_int(map_order, map_dest->id_nodo);
	buffer_add_int(map_order, map_dest->ip_nodo);
	buffer_add_int(map_order, map_dest->puerto_nodo);
	buffer_add_int(map_order, map_dest->block);
	buffer_add_string(map_order, map_dest->temp_file_name);

	result = send_buffer_and_destroy(socket, map_order);

	if (result < 0) {
		log_error(paranoid_log, "No se Pudo enviar la Orden de Map al Job");
	}

	return result;
}

//---------------------------------------------------------------------------
int recibir_info_resultado() {
	return 0;
}

//---------------------------------------------------------------------------
int send_finished_job(int socket_job) {
	return send_protocol_in_order(socket_job, FINISHED_JOB);
}

//---------------------------------------------------------------------------
int send_aborted_job(int socket_job) {
	return send_protocol_in_order(socket_job, ABORTED_JOB);
}

//***************************************** Interaccion FS *******************************************

//---------------------------------------------------------------------------
int obtenerAceptacionDeFS(int socket) {

	int result = send_protocol_in_order(socket,MARTA_CONNECTION_REQUEST);

	result = (result > 0) ? receive_protocol_in_order(socket) : result;


	switch(result) {

		case MARTA_CONNECTION_ACCEPTED:
			log_debug(paranoid_log,"Conexión con FS Aceptada");
			break;

		case MARTA_CONNECTION_REFUSED:
			log_error(paranoid_log,"Conexión con FS Rechazada (Nodos insuficientes)");
			break;

		case DISCONNECTED:
			log_error(paranoid_log, "FS se desconectó de forma inesperada");
			break;

		case -1:
			log_error(paranoid_log, "No se pudo recibir aceptación de la Conexion");
			break;

		default:
			log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", result);
			return -1;
			break;
	}

	return result;
}

//---------------------------------------------------------------------------
int receive_info_file(int socket, t_info_file* info_file) {

	int prot = receive_protocol_in_order(socket);

	switch (prot) {
	case INFO_ARCHIVO:
		log_debug(paranoid_log, "Recibiendo info del Archivo");
		break;

	case ARCHIVO_NO_DISPONIBLE:
		log_error(paranoid_log, "El Archivo NO se encuentra Disponible");
		return -2;
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "FS se Desconectó de forma inesperada");
		return 0;
		break;

	case -1:
		log_error(paranoid_log, "No se pudo recibir el resultado del Map");
		return -1;
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", prot);
		break;
	}

	int result = receive_int_in_order(socket, &(info_file->id_file));
	result = (result > 0) ? receive_int_in_order(socket, &(info_file->amount_blocks)) : result;

	return result;
}

//---------------------------------------------------------------------------
int solicitar_info_de_archivo(char *path_file, t_info_file* info_file) {

	int result = 1;

	t_buffer* solicitud_archivo_buff;

	log_debug(paranoid_log, "Pidiendo info del file: %s", path_file);

	solicitud_archivo_buff = buffer_create_with_protocol(INFO_ARCHIVO_REQUEST);
	buffer_add_string(solicitud_archivo_buff, path_file);

	pthread_mutex_lock(&conex_fs_ready);
	result = send_buffer_and_destroy(socket_fs, solicitud_archivo_buff);

	result = (result > 0) ? receive_info_file(socket_fs, info_file) : result;
	pthread_mutex_unlock(&conex_fs_ready);

	if (result <= 0) {
		log_error(paranoid_log, "No se Pudo tomar info del Archivo: %s", path_file);
	}

	return result;
}

//---------------------------------------------------------------------------
int receive_block_location(int socket, t_list* block_copies, uint32_t id_file, uint32_t block_number) {

	int prot;

	prot = receive_protocol_in_order(socket);

	switch (prot) {

	case BLOCK_LOCATION:
		log_debug(paranoid_log, "Obteniendo Ubicación del Archivo: %i Bloque: %i", id_file, block_number);
		break;

	case LOST_BLOCK:
		log_debug(paranoid_log, "El bloque: %i del Archivo: %i no se encuentra en el MDFS", block_number, id_file);
		return -2;
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "FS se Desconectó de forma inesperada");
		return 0;
		break;

	case -1:
		return -1;
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", prot);
		break;
	}

	t_block_copy* block_copy;
	uint32_t amount_copies, i;

	int result = receive_int_in_order(socket, &amount_copies);

	for (i = 0; (i < amount_copies) && (result > 0); i++) {
		block_copy = malloc(sizeof(t_map_dest));

		result = (result > 0) ? receive_int_in_order(socket, &(block_copy->id_nodo)) : result;
		result = (result > 0) ? receive_int_in_order(socket, &(block_copy->ip_nodo)) : result;
		result = (result > 0) ? receive_int_in_order(socket, &(block_copy->puerto_nodo)) : result;
		result = (result > 0) ? receive_int_in_order(socket, &(block_copy->block)) : result;

		if (result > 0) {
			list_add(block_copies, block_copy);
		} else {
			free_block_copy(block_copy);
		}
	}

	return result;
}

//---------------------------------------------------------------------------
int locate_block_in_FS(t_info_job info_job, uint32_t id_file, uint32_t block_number, t_list* block_copies) {

	t_buffer* block_request = buffer_create_with_protocol(BLOCK_LOCATION_REQUEST);
	buffer_add_int(block_request, id_file);
	buffer_add_int(block_request, block_number);

	pthread_mutex_lock(&conex_fs_ready);
	send_buffer_and_destroy(socket_fs, block_request);

	int result = receive_block_location(socket_fs, block_copies, id_file, block_number);
	pthread_mutex_unlock(&conex_fs_ready);

	return result;
}

//***************************************** Principal *******************************************

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
int get_info_files_from_FS(char **paths_files, t_list* file_list) {

	int i, result = 1;
	t_info_file* info_file;

	for (i = 0; (paths_files[i] != NULL) && (result > 0); i++) {
		info_file = malloc(sizeof(t_info_file));
		result = solicitar_info_de_archivo(paths_files[i], info_file);

		if (result > 0) {
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
int score_block_copy(t_block_copy* block_copy, int combiner, t_list* temp_map_list) {

	t_carga_nodo* carga_nodo;
	uint32_t carga;

	int _isNodeSearched(t_carga_nodo* carga_nodo) {
		return carga_nodo->id_nodo == block_copy->id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);
	carga_nodo = list_find(carga_nodos, (void *) _isNodeSearched);

	if (carga_nodo == NULL) {
		carga = 0;
	} else {
		carga = carga_nodo->cant_ops_en_curso;
	}
	pthread_mutex_unlock(&node_list_mutex);

	int score = 100 - carga; //XXX revisar numero arbitrario, es la cuenta que quiero??

	int _isTempMapSearched(t_temp_map* temp_map) {
		return temp_map->id_nodo == block_copy->id_nodo;
	}

	if(combiner) {
		score += list_count_satisfying(temp_map_list,(void*) _isTempMapSearched);
	}

	return score;
}


//---------------------------------------------------------------------------
t_map_dest* planificar_map(t_info_job info_job, uint32_t id_file, uint32_t block_number, uint32_t* last_id_map, t_list* temp_list) {

	log_info(paranoid_log, "Planificando map del Archivo:%i Bloque: %i ", id_file, block_number);

	t_block_copy* selected_copy;
	t_map_dest* self;

	t_list* block_copies = list_create();

	if (locate_block_in_FS(info_job, id_file, block_number, block_copies) <= 0) {
		log_error(paranoid_log, "No se pudieron localizar las copias del Archivo: %i, Bloque: %i", id_file, block_number);
		list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);
		return NULL;
	}

//	selected_copy = list_get(block_copies, 0); //TODO TESTME Planificador

	int _score_block_copy(t_block_copy* block_copy) {
		return score_block_copy(block_copy, info_job.combiner, temp_list);
	}

	t_block_copy* _best_copy_option(t_block_copy* block_copy1, t_block_copy* block_copy2) {
		return mayorSegun(block_copy1, block_copy2,(void *) _score_block_copy);
	}

	selected_copy = foldl1((void *) _best_copy_option, block_copies);


	if(selected_copy != NULL) {
		self = malloc(sizeof(t_map_dest));
		self->id_map = ++(*last_id_map);
		self->id_nodo = selected_copy->id_nodo;
		self->ip_nodo = selected_copy->ip_nodo;
		self->puerto_nodo = selected_copy->puerto_nodo;
		self->block = selected_copy->block;
		self->temp_file_name = string_from_format("map_%i_%i.temp", info_job.id_job, self->block);
	} else {
		log_error(paranoid_log,"No hay copias Activas del Archivo:%i Bloque: %i ", id_file, block_number);
		list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);
		return NULL;
	}

	list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);

	return self;
}

//---------------------------------------------------------------------------
int receive_result_map(int sockjob, t_result_map* result_map) {

	result_map->prot = receive_protocol_in_order(sockjob);

	switch (result_map->prot) {
	case MAP_OK:
		log_info(paranoid_log, "Map Realizado con Exito");
		break;

	case NODO_NOT_FOUND:
		log_warning(paranoid_log, "No se encontró el NODO donde mapear");
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "Job se Desconectó de forma inesperada");
		return 0;
		break;

	case -1:
		log_error(paranoid_log, "No se pudo recibir el resultado del Map");
		return -1;
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", result_map->prot);
		return -1;
		break;
	}

	return receive_int_in_order(sockjob, &(result_map->id_map));
}

//---------------------------------------------------------------------------
void add_pending_map(t_list* pending_maps, t_info_file* file_info, uint32_t block_number, t_map_dest* map_dest) {

	t_pending_map* pending_map;

	t_carga_nodo* carga_nodo;

	pending_map = malloc(sizeof(t_pending_map));
	pending_map->file = file_info;
	pending_map->block = block_number;
	pending_map->map_dest = map_dest;

	list_add(pending_maps, pending_map);

	int _isNodeSearched(t_carga_nodo* carga_nodo) {
		return carga_nodo->id_nodo == map_dest->id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);

	carga_nodo = list_find(carga_nodos, (void *) _isNodeSearched);

	if (carga_nodo == NULL) {
		carga_nodo = malloc(sizeof(t_carga_nodo));
		carga_nodo->id_nodo = map_dest->id_nodo;
		carga_nodo->cant_ops_en_curso = 1;

		list_add(carga_nodos, carga_nodo);
	} else {
		(carga_nodo->cant_ops_en_curso)++;
	}

	pthread_mutex_unlock(&node_list_mutex);

}

//---------------------------------------------------------------------------
void remove_pending_map(t_list* pending_maps, uint32_t id_map, t_list* temp_list, uint32_t* last_id_temp) {

	int _isSearchedPendingMap(t_pending_map* pending_map) {
		return pending_map->map_dest->id_map == id_map;
	}

	t_pending_map* pending_map = list_find(pending_maps, (void *) _isSearchedPendingMap);
	t_carga_nodo* carga_nodo;

	int _isNodeSearched(t_carga_nodo* carga_nodo) {
		return carga_nodo->id_nodo == pending_map->map_dest->id_nodo;
	}

	t_temp_map* temp_map = malloc(sizeof(t_temp_map));

	temp_map->id_temp = ++(*last_id_temp);
	temp_map->path = strdup(pending_map->map_dest->temp_file_name);
	temp_map->id_nodo = pending_map->map_dest->id_nodo;
	temp_map->id_file_origin = pending_map->file->id_file;
	temp_map->block_origin = pending_map->block;

	list_add(temp_list, temp_map);

	pthread_mutex_lock(&node_list_mutex);

	carga_nodo = list_find(carga_nodos, (void *) _isNodeSearched);

	if (carga_nodo == NULL) {
		log_error(paranoid_log, "Nodo no está en la lista de cargas cuando debería!");
	} else {
		(carga_nodo->cant_ops_en_curso)--;
	}

	pthread_mutex_unlock(&node_list_mutex);

	list_remove_and_destroy_by_condition(pending_maps, (void *) _isSearchedPendingMap, (void *) free_pending_map);
}

//---------------------------------------------------------------------------
int plan_maps(t_info_job info_job, t_list* file_list, t_list* temp_list, int sockjob) {

	t_list* pending_maps = list_create();
	t_pending_map* pending_map;
	t_map_dest* map_dest;
	int result = 1;
	int i, j, amount_files = list_size(file_list);
	t_info_file* file_info;
	uint32_t last_id_map = 0;
	uint32_t last_id_temp = 0;

	for (i = 0; i < amount_files; i++) {
		file_info = list_get(file_list, i);

		for (j = 0; j < file_info->amount_blocks; j++) {

			map_dest = planificar_map(info_job, file_info->id_file, j, &last_id_map, temp_list);
			result = (map_dest != NULL) ? order_map_to_job(map_dest, sockjob) : -2;

			if (result > 0) {

				add_pending_map(pending_maps, file_info, j, map_dest);

			} else {
				list_destroy_and_destroy_elements(pending_maps, (void *) free_pending_map);
				return -1;
			}
		}
	}

	t_result_map result_map;

	int _isSearchedPendingMap(t_pending_map* pending_map) {
		return pending_map->map_dest->id_map == result_map.id_map;
	}

	while (!list_is_empty(pending_maps)) {

		result = receive_result_map(sockjob, &result_map);

		if (result > 0) {

			switch (result_map.prot) {

			case MAP_OK:
				remove_pending_map(pending_maps, result_map.id_map, temp_list, &last_id_temp);
				break;

			case NODO_NOT_FOUND:
				pending_map = list_find(pending_maps, (void *) _isSearchedPendingMap);

				free_map_dest(pending_map->map_dest);
				pending_map->map_dest = planificar_map(info_job, pending_map->file->id_file, pending_map->block, &last_id_map, temp_list);

				result = (pending_map->map_dest != NULL) ? order_map_to_job(pending_map->map_dest, sockjob) : -2;

				if (result <= 0) {
					list_destroy_and_destroy_elements(pending_maps, (void *) free_pending_map);
					return -1;
				}
				break;
			}

		} else {
			list_destroy_and_destroy_elements(pending_maps, (void *) free_pending_map);
			return -1;
		}
	}

	list_destroy_and_destroy_elements(pending_maps, (void *) free_pending_map);
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

	int result = 1;

	getFromSocketAddrStd(job->socketaddr_cli, &ip_job, &port_job);

	log_info(paranoid_log, "Comienzo Hilo Job");

	log_info(paranoid_log, "Obteniendo New Job del Job IP: %s, Puerto: %i", ip_job, port_job);

	result = (result > 0) ? receive_info_job(job, &info_job) : result;
	result = (result > 0) ? get_info_files_from_FS(info_job.paths_files, file_list) : result;
	result = (result > 0) ? plan_maps(info_job, file_list, temp_list, job->sockfd) : result;
	result = (result > 0) ? plan_reduces() : result; //XXX
	result = (result > 0) ? save_result_file_in_MDFS() : result; //XXX

	if (result > 0) {
		send_finished_job(job->sockfd);
		log_info(paranoid_log, "Finalizado con éxito Job IP: %s, Puerto: %i", ip_job, port_job);
	} else {
		send_aborted_job(job->sockfd);
		log_error(paranoid_log, "No se pudo realizar el Job IP: %s, Puerto: %i", ip_job, port_job);
	}

	free(ip_job);
	free_info_job(info_job);
	list_destroy_and_destroy_elements(file_list, (void *) free_info_file);
	list_destroy_and_destroy_elements(temp_list, (void *) free_temp_map);
}

//---------------------------------------------------------------------------
void esperar_finalizacion_hilo_conex_job(t_hilo_job* job) {
	void* ret_recep;

	if (pthread_join(*(job->thr), &ret_recep) != 0) {
		printf("Error al hacer join del hilo\n");
	}
}

//---------------------------------------------------------------------------
void init_var_globales() {

	pthread_mutex_init(&mutex_cerrar_marta, NULL);
	pthread_mutex_init(&mutex_ultimo_id, NULL);
	pthread_mutex_init(&conex_fs_ready, NULL);

	pthread_mutex_init(&node_list_mutex, NULL);

	paranoid_log = log_create("./logMaRTA.log", "MaRTA", 1, LOG_LEVEL_TRACE);

	carga_nodos = list_create();
}

//--------------------------------------------------------------------------
void end_var_globales() {

	pthread_mutex_destroy(&mutex_cerrar_marta);
	pthread_mutex_destroy(&mutex_ultimo_id);
	pthread_mutex_destroy(&conex_fs_ready);

	pthread_mutex_destroy(&node_list_mutex);

	log_destroy(paranoid_log);

	list_destroy_and_destroy_elements(carga_nodos,(void *)free_carga_nodo);
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

	if (obtenerAceptacionDeFS(socket_fs) != MARTA_CONNECTION_ACCEPTED) {
		log_error(paranoid_log, "No se pudo Comunicar con el FS");
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

		if ((job->sockfd = aceptarCliente(listener_jobs, &(job->socketaddr_cli))) == -1) {
			log_error(paranoid_log, "Error al Aceptar Nuevos Jobs");
			exit(-1);
		}

		if (receive_new_client_job(job->sockfd)) {
			list_add(lista_jobs, job);

			log_info(paranoid_log, "Creación de Hilo Job");
			if (pthread_create(job->thr, NULL, (void *) hilo_conex_job, job) != 0) {
				log_error(paranoid_log, "No se pudo crear Hilo Job");
				exit(-1);
			}

			hilos_de_job++;
		} else {
			free_hilo_job(job);
		}

		if (hilos_de_job >= 3) { //XXX
			terminar_hilos();
		}
	}

	log_info(paranoid_log, "Esperando finalizacion de hilos de Job");
	list_iterate(lista_jobs, (void *) esperar_finalizacion_hilo_conex_job);

	list_destroy_and_destroy_elements(lista_jobs, (void *) free_hilo_job);

	free_conf_marta(&conf);

	end_var_globales();

	return EXIT_SUCCESS;
}
