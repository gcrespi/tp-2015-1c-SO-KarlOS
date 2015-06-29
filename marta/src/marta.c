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
	char *path;
	uint32_t amount_blocks;
} t_info_file;

typedef struct {
	uint32_t id_temp;
	char* path;
	char* path_file_origin;
	uint32_t block_origin;
} t_temp_map;

typedef struct {
	uint32_t id_nodo;
	t_list* temps_map;
	//XXX falta temp total del nodo
} t_temp_nodo;

//************************* Estructura Por cada Map ************************

typedef struct {
	uint32_t id_nodo;
	uint32_t block;
} t_block_copy;

typedef struct {
	uint32_t id_map;
	uint32_t id_nodo;
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

typedef struct {
	uint32_t prot;
	uint32_t id_reduce;
} t_result_reduce;


//************************* Estructura de Info y Estado de los Nodos (GLOBAL) ************************
typedef struct {
	uint32_t id_nodo;
	uint32_t cant_ops_en_curso;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
} t_info_nodo;

//########################################  Prototipos  #########################################

//************************************* Manejo Estructuras **************************************

void levantar_arch_conf_marta(t_conf_MaRTA* conf);
void free_conf_marta(t_conf_MaRTA* conf);

void free_hilo_job(t_hilo_job* job);
void free_info_job(t_info_job info_job);
void free_info_file(t_info_file* self);
void free_temp_map(t_temp_map* self);
void free_temp_nodo(t_temp_nodo* self);

void free_map_dest(t_map_dest* self);
void free_pending_map(t_pending_map* self);

void free_info_nodo(t_info_nodo* self);

void mostrar_info_file(t_info_file* self);
void show_map_dest_and_nodo_location(t_map_dest* md, uint32_t ip_nodo, uint32_t puerto_nodo);

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
int solicitar_info_de_archivo(t_info_file* info_file);
int locate_block_in_FS(char* path_file, uint32_t block_number, t_list* block_copies);
int obtenerAceptacionDeFS(int socket);

//***************************************** Principal *******************************************
void terminar_hilos(); //XXX
int programa_terminado(); //XXX provisorio, en la entrega no se usa

int increment_and_take_last_job_id();

int get_info_files_from_FS(char **paths_files, t_list* file_list);

t_map_dest* planificar_map(t_info_job info_job, char* path_file, uint32_t block_number, uint32_t* last_id_map, t_list* temps_nodo, t_list* pending_maps);

int plan_maps(t_info_job info_job, t_list* file_list, t_list* temps_nodo, int sockjob);
int plan_reduces(t_info_job info_job, t_list* temp_map_list, int sockjob);
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
t_list* info_nodos;

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
	free(self->path);
	free(self);
}

//---------------------------------------------------------------------------
void free_temp_map(t_temp_map* self) {
	free(self->path);
	free(self->path_file_origin);
	free(self);

}

//---------------------------------------------------------------------------
void free_temp_nodo(t_temp_nodo* self) {

	list_destroy_and_destroy_elements(self->temps_map,(void *) free_temp_map);
	free(self);
}


//---------------------------------------------------------------------------
void mostrar_info_file(t_info_file* self) {

	log_debug(paranoid_log, "Archivo: %s Cantidad Bloques: %i", self->path, self->amount_blocks);
}

//---------------------------------------------------------------------------
void show_map_dest_and_nodo_location(t_map_dest* md, uint32_t ip_nodo, uint32_t puerto_nodo) {

	char *ip = from_int_to_inet_addr(ip_nodo);

	log_debug(paranoid_log, "Map ID: %i, Nodo: ID: %i, IP: %s, Port: %i, Block: %i Temp:%s", md->id_map, md->id_nodo, ip,
			puerto_nodo, md->block, md->temp_file_name);

	free(ip);
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
void free_info_nodo(t_info_nodo* self) {
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
int locate_nodo(uint32_t id_nodo, uint32_t *ip_nodo, uint32_t *puerto_nodo) {

	int result = 1,found;

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);

	t_info_nodo* info_nodo = list_find(info_nodos, (void *) _isNodeSearched);

	if ((found = (info_nodo != NULL))) {
		*ip_nodo = info_nodo->ip_nodo;
		*puerto_nodo = info_nodo->puerto_nodo;
	}
	pthread_mutex_unlock(&node_list_mutex);

	if(found)
		return 1;

//	result = locate_nodo_in_FS(id_nodo, ip_nodo, puerto_nodo); XXX Implementar en el FS

	*ip_nodo = inet_addr("127.0.0.1");
	*puerto_nodo = 3500 + id_nodo;

	if(result > 0) {
		pthread_mutex_lock(&node_list_mutex);

		info_nodo = list_find(info_nodos, (void *) _isNodeSearched);

		if (info_nodo != NULL) {
			*ip_nodo = info_nodo->ip_nodo;
			*ip_nodo = info_nodo->puerto_nodo;
		} else {
			info_nodo = malloc(sizeof(t_info_nodo));
			info_nodo->id_nodo = id_nodo;
			info_nodo->cant_ops_en_curso = 0;
			info_nodo->ip_nodo = (*ip_nodo);
			info_nodo->puerto_nodo = (*puerto_nodo);

			list_add(info_nodos, info_nodo);
		}
		pthread_mutex_unlock(&node_list_mutex);
	}

	return result;
}

//---------------------------------------------------------------------------
int update_nodo_location(uint32_t id_nodo) {

	int result = 1;

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == id_nodo;
	}

//	result = locate_nodo_in_FS(id_nodo, ip_nodo, puerto_nodo); XXX Implementar en el FS

	uint32_t ip_nodo = inet_addr("127.0.0.1");
	uint32_t puerto_nodo = 3500 + id_nodo;

	if(result > 0) {
		pthread_mutex_lock(&node_list_mutex);

		t_info_nodo* info_nodo = list_find(info_nodos, (void *) _isNodeSearched);

		if (info_nodo != NULL) {
			info_nodo->ip_nodo = ip_nodo;
			info_nodo->puerto_nodo = puerto_nodo;
		}
		pthread_mutex_unlock(&node_list_mutex);
	}

	return result;
}

//---------------------------------------------------------------------------
int order_map_to_job(t_map_dest* map_dest, int socket) {

	int result;

	uint32_t ip_nodo, puerto_nodo;

	result = locate_nodo(map_dest->id_nodo, &ip_nodo, &puerto_nodo);

	show_map_dest_and_nodo_location(map_dest, ip_nodo, puerto_nodo);

	t_buffer* map_order = buffer_create_with_protocol(ORDER_MAP);

	buffer_add_int(map_order, map_dest->id_map);
	buffer_add_int(map_order, map_dest->id_nodo);
	buffer_add_int(map_order, ip_nodo);
	buffer_add_int(map_order, puerto_nodo);
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
		return receive_int_in_order(socket, &(info_file->amount_blocks));
		break;

	case ARCHIVO_NO_DISPONIBLE:
		log_error(paranoid_log, "El Archivo NO se encuentra Disponible");
		return -2;
		break;

	case ARCHIVO_INEXISTENTE:
		log_error(paranoid_log, "El Archivo NO existe en el MDFS");
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
		return -1;
		break;
	}
}

//---------------------------------------------------------------------------
int solicitar_info_de_archivo(t_info_file* info_file) {

	int result = 1;

	t_buffer* solicitud_archivo_buff;

	log_debug(paranoid_log, "Pidiendo info del file: %s", info_file->path);

	solicitud_archivo_buff = buffer_create_with_protocol(INFO_ARCHIVO_REQUEST);
	buffer_add_string(solicitud_archivo_buff, info_file->path);

	pthread_mutex_lock(&conex_fs_ready);
	result = send_buffer_and_destroy(socket_fs, solicitud_archivo_buff);

	result = (result > 0) ? receive_info_file(socket_fs, info_file) : result;
	pthread_mutex_unlock(&conex_fs_ready);

	if (result <= 0) {
		log_error(paranoid_log, "No se Pudo tomar info del Archivo: %s", info_file->path);
	}

	return result;
}

//---------------------------------------------------------------------------
int receive_block_location(int socket, t_list* block_copies, char* path_file, uint32_t block_number) {

	int prot;

	prot = receive_protocol_in_order(socket);

	switch (prot) {

	case BLOCK_LOCATION:
		log_debug(paranoid_log, "Obteniendo Ubicación del Archivo: %s Bloque: %i", path_file, block_number);
		break;

	case LOST_BLOCK:
		log_debug(paranoid_log, "El bloque: %i del Archivo: %s no se encuentra en el MDFS", block_number, path_file);
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

		uint32_t ip,puerto;

		result = (result > 0) ? receive_int_in_order(socket, &(block_copy->id_nodo)) : result;
		result = (result > 0) ? receive_int_in_order(socket, &ip) : result; // XXX sacar del FS
		result = (result > 0) ? receive_int_in_order(socket, &puerto) : result; // XXX sacar del FS
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
int locate_block_in_FS(char* path_file, uint32_t block_number, t_list* block_copies) {

	t_buffer* block_request = buffer_create_with_protocol(BLOCK_LOCATION_REQUEST);
	buffer_add_string(block_request, path_file);
	buffer_add_int(block_request, block_number);

	pthread_mutex_lock(&conex_fs_ready);
	int result = send_buffer_and_destroy(socket_fs, block_request);

	result = (result > 0) ? receive_block_location(socket_fs, block_copies, path_file, block_number) : result;
	pthread_mutex_unlock(&conex_fs_ready);

	return result;
}


//---------------------------------------------------------------------------
int receive_nodo_location(int socket, uint32_t id_nodo, uint32_t *ip_nodo, uint32_t *puerto_nodo) {

	int prot;

	prot = receive_protocol_in_order(socket);

	switch (prot) {

	case NODO_LOCATION:
		log_debug(paranoid_log, "Obteniendo Ubicación del nodo ID: %i", id_nodo);
		break;

	case LOST_NODO:
		log_debug(paranoid_log, "El nodo ID: %i no se encuentra en el MDFS", id_nodo);
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

	int result = receive_int_in_order(socket, ip_nodo);
	result = (result > 0) ? receive_int_in_order(socket, puerto_nodo) : result;

	return result;
}

//---------------------------------------------------------------------------
int locate_nodo_in_FS(uint32_t id_nodo,uint32_t *ip_nodo,uint32_t *puerto_nodo) {
	t_buffer* nodo_request = buffer_create_with_protocol(NODO_LOCATION_REQUEST);
	buffer_add_int(nodo_request, id_nodo);

	pthread_mutex_lock(&conex_fs_ready);
	int result = send_buffer_and_destroy(socket_fs, nodo_request);

	result = (result > 0) ? receive_nodo_location(socket_fs, id_nodo, ip_nodo, puerto_nodo) : result;
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
		info_file->path = strdup(paths_files[i]);
		result = solicitar_info_de_archivo(info_file);

		if (result > 0) {
			list_add(file_list, info_file);
		} else {
			free_info_file(info_file);
		}
	}

	if (result > 0) {
		log_debug(paranoid_log, "Info de archivos solicitados con exito");
		list_iterate(file_list, (void *) mostrar_info_file);
	}
	return result;
}


//---------------------------------------------------------------------------
uint32_t count_map_temps_in_node(t_list* temps_nodo, uint32_t id_nodo) {

	int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->id_nodo == id_nodo;
	}

	t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

	if(temp_nodo == NULL)
		return 0;

	return list_size(temp_nodo->temps_map);
}

//---------------------------------------------------------------------------
uint32_t count_pending_maps_in_node(t_list* pending_maps, uint32_t id_nodo) {

	int _pendingMapHasTheSameIdNodo(t_pending_map* pending_map) {
		return pending_map->map_dest->id_nodo == id_nodo;
	}

	return list_count_satisfying(pending_maps, (void *) _pendingMapHasTheSameIdNodo);
}

//---------------------------------------------------------------------------
int score_block_copy(t_block_copy* block_copy, int combiner, t_list* temps_nodo, t_list* pending_maps) {

	t_info_nodo* info_nodo;
	uint32_t carga;
	uint32_t cant_mismo_nodo;

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == block_copy->id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);
	info_nodo = list_find(info_nodos, (void *) _isNodeSearched);

	if (info_nodo == NULL) {
		carga = 0;
	} else {
		carga = info_nodo->cant_ops_en_curso;
	}
	pthread_mutex_unlock(&node_list_mutex);

	int score = 100 - carga; //XXX revisar numero arbitrario, es la cuenta que quiero??

	if(combiner) {
		cant_mismo_nodo = 0;

		cant_mismo_nodo += count_map_temps_in_node(temps_nodo, block_copy->id_nodo);
//		cant_mismo_nodo += count_pending_maps_in_node(pending_maps, block_copy->id_nodo);XXX

		score += cant_mismo_nodo;
	}

	return score;
}


//---------------------------------------------------------------------------
t_map_dest* planificar_map(t_info_job info_job, char* path_file, uint32_t block_number, uint32_t* last_id_map, t_list* temps_nodo, t_list* pending_maps) {

	log_info(paranoid_log, "Planificando map del Archivo:%s Bloque: %i ", path_file, block_number);

	t_block_copy* selected_copy;
	t_map_dest* self;

	t_list* block_copies = list_create();

	if (locate_block_in_FS(path_file, block_number, block_copies) <= 0) {
		log_error(paranoid_log, "No se pudieron localizar las copias del Archivo: %s, Bloque: %i", path_file, block_number);
		list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);
		return NULL;
	}

	//TODO TESTME Planificador

	int _score_block_copy(t_block_copy* block_copy) {
		return score_block_copy(block_copy, info_job.combiner, temps_nodo, pending_maps);
	}

	t_block_copy* _best_copy_option(t_block_copy* block_copy1, t_block_copy* block_copy2) {
		return mayorSegun(block_copy1, block_copy2,(void *) _score_block_copy);
	}

	selected_copy = foldl1((void *) _best_copy_option, block_copies);


	if(selected_copy != NULL) {
		self = malloc(sizeof(t_map_dest));
		self->id_map = ++(*last_id_map);
		self->id_nodo = selected_copy->id_nodo;
		self->block = selected_copy->block;
		self->temp_file_name = string_from_format("map_%i_%i.temp", info_job.id_job, self->block);
	} else {
		log_error(paranoid_log,"No hay copias Activas del Archivo:%s Bloque: %i ", path_file, block_number);
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

	t_info_nodo* info_nodo;

	pending_map = malloc(sizeof(t_pending_map));
	pending_map->file = file_info;
	pending_map->block = block_number;
	pending_map->map_dest = map_dest;

	list_add(pending_maps, pending_map);

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == map_dest->id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);

	info_nodo = list_find(info_nodos, (void *) _isNodeSearched);

	if (info_nodo == NULL) {
		log_error(paranoid_log,"No Existe el Nodo cargado en lista cuando debería!!! (Intentando incrementar cant ops en curso)");
	} else {
		(info_nodo->cant_ops_en_curso)++;
	}

	pthread_mutex_unlock(&node_list_mutex);

}

//---------------------------------------------------------------------------
void remove_pending_map(t_list* pending_maps, uint32_t id_map, t_list* temps_nodo, uint32_t* last_id_temp) {

	int _isSearchedPendingMap(t_pending_map* pending_map) {
		return pending_map->map_dest->id_map == id_map;
	}

	t_pending_map* pending_map = list_find(pending_maps, (void *) _isSearchedPendingMap);
	t_info_nodo* info_nodo;

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == pending_map->map_dest->id_nodo;
	}

	int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->id_nodo == pending_map->map_dest->id_nodo;
	}

	t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

	if(temp_nodo == NULL) {
		temp_nodo = malloc(sizeof(t_temp_nodo));
		temp_nodo->id_nodo = pending_map->map_dest->id_nodo;
		temp_nodo->temps_map = list_create();
		list_add(temps_nodo, temp_nodo);
	}


	t_temp_map* temp_map = malloc(sizeof(t_temp_map));

	temp_map->id_temp = ++(*last_id_temp);
	temp_map->path = strdup(pending_map->map_dest->temp_file_name);
	temp_map->path_file_origin = strdup(pending_map->file->path);
	temp_map->block_origin = pending_map->block;

	list_add(temp_nodo->temps_map, temp_map);

	pthread_mutex_lock(&node_list_mutex);

	info_nodo = list_find(info_nodos, (void *) _isNodeSearched);

	if (info_nodo == NULL) {
		log_error(paranoid_log, "No Existe el Nodo cargado en lista cuando debería!!! (Intentando decrementar cant ops en curso)");
	} else {
		(info_nodo->cant_ops_en_curso)--;
	}

	pthread_mutex_unlock(&node_list_mutex);

	list_remove_and_destroy_by_condition(pending_maps, (void *) _isSearchedPendingMap, (void *) free_pending_map);
}

//---------------------------------------------------------------------------
int plan_maps(t_info_job info_job, t_list* file_list, t_list* temps_nodo, int sockjob) {

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

			map_dest = planificar_map(info_job, file_info->path, j, &last_id_map, temps_nodo, pending_maps);
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
				remove_pending_map(pending_maps, result_map.id_map, temps_nodo, &last_id_temp);
				break;

			case NODO_NOT_FOUND:
				pending_map = list_find(pending_maps, (void *) _isSearchedPendingMap);

				result = update_nodo_location(pending_map->map_dest->id_nodo);

				if(result > 0) {

					free_map_dest(pending_map->map_dest);
					pending_map->map_dest = planificar_map(info_job, pending_map->file->path, pending_map->block, &last_id_map, temps_nodo, pending_maps);

					result = (pending_map->map_dest != NULL) ? order_map_to_job(pending_map->map_dest, sockjob) : -2;
				}

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

////---------------------------------------------------------------------------
//int receive_result_reduce(int sockjob, t_result_reduce* result_reduce) {
//
//	result_reduce->prot = receive_protocol_in_order(sockjob);
//
//	switch (result_reduce->prot) {
//	case REDUCE_OK:
//		log_info(paranoid_log, "Map Realizado con Exito");
//		break;
//
//	case NODO_NOT_FOUND:
////		receive_int_in_order(sockjob, )
//		log_warning(paranoid_log, "No se encontró el NODO donde mapear");
//		break;
//
//	case TEMP_NOT_FOUND:
//		log_warning(paranoid_log, "No se encontró el Archivo Temporal a Reducir ");
//		break;
//
//	case DISCONNECTED:
//		log_error(paranoid_log, "Job se Desconectó de forma inesperada");
//		return 0;
//		break;
//
//	case -1:
//		log_error(paranoid_log, "No se pudo recibir el resultado del Map");
//		return -1;
//		break;
//
//	default:
//		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", result_reduce->prot);
//		return -1;
//		break;
//	}
//
//	return receive_int_in_order(sockjob, &(result_reduce->id_reduce));
//}

//---------------------------------------------------------------------------
int buffer_add_temp_node(t_buffer* order_reduce_buff, t_temp_nodo* temp_nodo) {

	void _buffer_add_temp_map(t_temp_map* temp_map) {
		buffer_add_int(order_reduce_buff, temp_map->id_temp);
		buffer_add_string(order_reduce_buff, temp_map->path);
	}

	uint32_t amount_files_in_node;
	amount_files_in_node = list_size(temp_nodo->temps_map);

	if(amount_files_in_node == 0) {
		log_warning(paranoid_log,"No se puede Realizar Reduce de 0 Maps en Nodo: %i !",temp_nodo->id_nodo);
	}

	buffer_add_int(order_reduce_buff, temp_nodo->id_nodo);
	uint32_t ip_nodo = 0, puerto_nodo = 0;

	int result = locate_nodo(temp_nodo->id_nodo, &ip_nodo, &puerto_nodo);

	if(result <= 0)
		return result;

	buffer_add_int(order_reduce_buff, ip_nodo);
	buffer_add_int(order_reduce_buff, puerto_nodo);
	buffer_add_int(order_reduce_buff,amount_files_in_node);

	list_iterate(temp_nodo->temps_map, (void *) _buffer_add_temp_map);

	return result;
}

//---------------------------------------------------------------------------
int plan_reduces(t_info_job info_job, t_list* temps_nodo, int sockjob) {

	uint32_t last_id_reduce = 0;
	int result = 1;

//	t_list* pending_reduce = list_create();
//	t_pending_reduce* pending_reduce;

//	if(info_job.combiner) {
//		//XXX Planear con Combiner
//
//		return 1;
//	} else {
		uint32_t nodes_amount = list_size(temps_nodo);
		if(nodes_amount == 0) {
			log_error(paranoid_log,"No se puede Realizar Reduce de 0 Nodos!!!");
			return -1;
		}

		t_buffer* order_reduce_buff = buffer_create_with_protocol(ORDER_REDUCE);
		buffer_add_int(order_reduce_buff,++(last_id_reduce));
		buffer_add_string(order_reduce_buff,info_job.path_result);
		buffer_add_int(order_reduce_buff, nodes_amount);

		void _buffer_add_temp_node(t_temp_nodo* temp_nodo) {
			result = (result > 0) ? buffer_add_temp_node(order_reduce_buff, temp_nodo) : result;
		}
		list_iterate(temps_nodo, (void *) _buffer_add_temp_node);

		result =  (result > 0) ? send_buffer_and_destroy(sockjob,order_reduce_buff) : result;

		if(result < 0) {
			log_error(paranoid_log, "No se Pudo enviar la Orden de Reduce al Job");
			return -1;
		}

//		result = (result > 0)? receive_result_reduce() : result;
//	}

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
	t_list* temps_nodo = list_create();

	int result = 1;

	getFromSocketAddrStd(job->socketaddr_cli, &ip_job, &port_job);

	log_info(paranoid_log, "Comienzo Hilo Job");

	log_info(paranoid_log, "Obteniendo New Job del Job IP: %s, Puerto: %i", ip_job, port_job);

	result = (result > 0) ? receive_info_job(job, &info_job) : result;
	result = (result > 0) ? get_info_files_from_FS(info_job.paths_files, file_list) : result;
	result = (result > 0) ? plan_maps(info_job, file_list, temps_nodo, job->sockfd) : result;
	result = (result > 0) ? plan_reduces(info_job, temps_nodo, job->sockfd) : result;
	result = (result > 0) ? save_result_file_in_MDFS() : result; //XXX falta Desarrollar

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
	list_destroy_and_destroy_elements(temps_nodo, (void *) free_temp_nodo);
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

	info_nodos = list_create();
}

//--------------------------------------------------------------------------
void end_var_globales() {

	pthread_mutex_destroy(&mutex_cerrar_marta);
	pthread_mutex_destroy(&mutex_ultimo_id);
	pthread_mutex_destroy(&conex_fs_ready);

	pthread_mutex_destroy(&node_list_mutex);

	log_destroy(paranoid_log);

	list_destroy_and_destroy_elements(info_nodos,(void *)free_info_nodo);
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

		if (hilos_de_job >= 3) { //XXX Provisorio
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
