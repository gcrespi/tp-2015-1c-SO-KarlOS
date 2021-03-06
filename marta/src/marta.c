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

typedef enum {
	SOURCE_MAP, SOURCE_REDUCE
} t_source;

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
	uint32_t id_nodo;
	uint32_t amount_maps;
	t_list* orphan_temps;
} t_temp_nodo;


//------------- Estructuras que componen el Árbol de Reduces ----------------

typedef struct {
	char* path_file;
	uint32_t block_number;
} t_src_block;

typedef struct {
	char* path_temp;
	t_source source;
	void* childs; //puede ser un t_list* de t_temps ó un t_src_block*
} t_temp;

typedef struct {
	uint32_t id_nodo;
	char* file_name;
} t_final_result;

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
	t_src_block* src_block;
	t_map_dest* map_dest;
} t_pending_map;

typedef struct {
	uint32_t prot;
	uint32_t id_map;
} t_result_map;

//************************* Estructura Por cada Reduce ************************

typedef struct {
	uint32_t id_reduce;
	uint32_t id_nodo;
	char* temp_file_name;
} t_pending_reduce;

typedef struct {
	uint32_t prot;
	uint32_t id_nodo;
} t_result_reduce;


//************************* Estructura de Info y Estado de los Nodos (GLOBAL) ************************
typedef struct {
	uint32_t id_nodo;
	uint32_t cant_ops_en_curso;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
} t_info_nodo;

//########################################  Prototipos  #########################################

//******************************* Manejo De Lista de Localización *******************************
int is_node_location_here(uint32_t id_nodo);
int locate_nodo_in_FS(uint32_t id_nodo,uint32_t *ip_nodo,uint32_t *puerto_nodo, int* found);
int update_location_node_from_FS(uint32_t id_nodo, int* found);
int update_location_node_ifnot_here(uint32_t id_nodo, int* found);
int receive_nodo_location(int socket, uint32_t id_nodo, uint32_t *ip_nodo, uint32_t *puerto_nodo, int* found);
int locate_nodo(uint32_t id_nodo, uint32_t *ip_nodo, uint32_t *puerto_nodo);

//************************************* Manejo Estructuras **************************************

void levantar_arch_conf_marta(t_conf_MaRTA* conf);
void free_conf_marta(t_conf_MaRTA* conf);

void free_info_job(t_info_job info_job);
void free_info_file(t_info_file* self);

void free_temp_nodo(t_temp_nodo* self);
void free_src_block(t_src_block* self);
void free_temp_recursive(t_temp* self);
void free_temp_alone(t_temp* self);

void free_final_result(t_final_result* self);

void free_map_dest(t_map_dest* self);
void free_block_copy(t_block_copy* self);
void free_pending_map(t_pending_map* self);
void free_pending_reduce(t_pending_reduce* self);


void free_info_nodo(t_info_nodo* self);
void free_hilo_job(t_hilo_job* job);

void mostrar_info_file(t_info_file* self);
void show_map_dest_and_nodo_location(t_map_dest* md, uint32_t ip_nodo, uint32_t puerto_nodo);

//************************************* Interaccion Job **************************************

int receive_new_client_job(int sockjob);
int receive_info_job(t_hilo_job* job, t_info_job* info_job);
int order_map_to_job(t_map_dest* map_dest, int socket);
int receive_result_map(int sockjob, t_result_map* result_map);

int send_finished_job(int socket_job);
int send_aborted_job(int socket_job);

//************************************* Interaccion FS **************************************
int obtenerAceptacionDeFS(int socket);
int receive_info_file(int socket, t_info_file* info_file);
int solicitar_info_de_archivo(t_info_file* info_file);
int receive_block_location(int socket, t_list* block_copies, char* path_file, uint32_t block_number);
int locate_block_in_FS(char* path_file, uint32_t block_number, t_list* block_copies);

//***************************************** Principal *******************************************
void terminar_hilos();
int programa_terminado();

int increment_and_take_last_job_id();

int get_info_files_from_FS(char **paths_files, t_list* file_list, t_list* unmapped_blocks);

t_map_dest* planificar_map(t_info_job info_job, t_src_block* src_block, uint32_t* last_id_map, t_list* temps_nodo, t_list* pending_maps);

int plan_maps(t_info_job info_job, t_list* unmapped_blocks, t_list* temps_nodo, int sockjob);
int plan_combined_reduces(t_info_job info_job, t_list* temps_nodo, int sockjob, t_final_result* final_result);
int plan_unique_reduce(t_info_job info_job, t_list* temps_nodo, int sockjob, t_final_result* final_result);
int save_result_file_in_MDFS(t_info_job info_job, t_final_result* final_result);

void hilo_conex_job(t_hilo_job *job);
void esperar_finalizacion_hilo_conex_job(t_hilo_job* job);

void init_var_globales();
void end_var_globales();

void compute_lost_maps(t_list* unmapped_blocks, t_list* lost_temps);

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

//************************************* Manejo De Lista de Localización *************************

//---------------------------------------------------------------------------
int is_node_location_here(uint32_t id_nodo) {

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);
	t_info_nodo* info_nodo = list_find(info_nodos, (void *) _isNodeSearched);
	pthread_mutex_unlock(&node_list_mutex);

	return info_nodo != NULL;
}

//---------------------------------------------------------------------------
int locate_nodo_in_FS(uint32_t id_nodo,uint32_t *ip_nodo,uint32_t *puerto_nodo, int* found) {
	t_buffer* nodo_request = buffer_create_with_protocol(NODO_LOCATION_REQUEST);
	buffer_add_int(nodo_request, id_nodo);

	pthread_mutex_lock(&conex_fs_ready);
	int result = send_buffer_and_destroy(socket_fs, nodo_request);

	result = (result > 0) ? receive_nodo_location(socket_fs, id_nodo, ip_nodo, puerto_nodo, found) : result;
	pthread_mutex_unlock(&conex_fs_ready);

	return result;
}

//---------------------------------------------------------------------------
int update_location_node_from_FS(uint32_t id_nodo, int* found) {

	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	t_info_nodo* info_nodo;

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == id_nodo;
	}

	int result = locate_nodo_in_FS(id_nodo, &ip_nodo, &puerto_nodo, found);

	if((result > 0) && (*found)) {
		pthread_mutex_lock(&node_list_mutex);
		info_nodo = list_find(info_nodos, (void *) _isNodeSearched);
		if (info_nodo != NULL) {
			if((ip_nodo != info_nodo->ip_nodo) || (puerto_nodo != info_nodo->puerto_nodo)) {
				info_nodo->ip_nodo = ip_nodo;
				info_nodo->puerto_nodo = puerto_nodo;
			}
		} else {
			info_nodo = malloc(sizeof(t_info_nodo));
			info_nodo->id_nodo = id_nodo;
			info_nodo->cant_ops_en_curso = 0;
			info_nodo->ip_nodo = ip_nodo;
			info_nodo->puerto_nodo = puerto_nodo;

			list_add(info_nodos, info_nodo);
		}
		pthread_mutex_unlock(&node_list_mutex);
	}
	return result;
}

//---------------------------------------------------------------------------
int update_location_node_ifnot_here(uint32_t id_nodo, int* found) {

	int result = 1;
	*found = 0;

	if(is_node_location_here(id_nodo)) {
		*found = 1;
		return 1;
	}

	result = update_location_node_from_FS(id_nodo, found);

	return result;
}

//---------------------------------------------------------------------------
int receive_nodo_location(int socket, uint32_t id_nodo, uint32_t *ip_nodo, uint32_t *puerto_nodo, int* found) {

	int prot;
	*found = 0;

	prot = receive_protocol_in_order(socket);

	switch (prot) {

	case NODO_LOCATION:
		log_debug(paranoid_log, "Obteniendo Ubicación del nodo ID: %i", id_nodo);
		*found = 1;
		break;

	case LOST_NODO:
		log_debug(paranoid_log, "El nodo ID: %i no se encuentra en el MDFS", id_nodo);
		return 1;
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
		return -1;
		break;
	}

	int result = receive_int_in_order(socket, ip_nodo);
	result = (result > 0) ? receive_int_in_order(socket, puerto_nodo) : result;

	return result;
}

//---------------------------------------------------------------------------
int locate_nodo(uint32_t id_nodo, uint32_t *ip_nodo, uint32_t *puerto_nodo) {

	int found;

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

	return found;
}

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
void free_conf_marta(t_conf_MaRTA* conf) {
	free(conf->ip_fs);
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
void free_temp_nodo(t_temp_nodo* self) {

	list_destroy_and_destroy_elements(self->orphan_temps,(void *) free_temp_recursive);
	free(self);
}

//---------------------------------------------------------------------------
void free_src_block(t_src_block* self) {
//	free(self->path_file); NO se libera porque apunta al path correspondiente en info_file
	free(self);
}

//---------------------------------------------------------------------------
void free_temp_recursive(t_temp* self) {
	free(self->path_temp);
	if(self->source == SOURCE_REDUCE) {
		list_destroy_and_destroy_elements(self->childs,(void *) free_temp_recursive);
	} else {
		if(self->childs != NULL)
			free_src_block(self->childs);
	}
	free(self);
}

//---------------------------------------------------------------------------
void free_temp_alone(t_temp* self) {
	free(self->path_temp);
	if(self->source == SOURCE_REDUCE) {
		list_destroy(self->childs);
	}
	free(self);
}

//---------------------------------------------------------------------------
void free_final_result(t_final_result* self) {
	free(self->file_name);
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
void free_pending_reduce(t_pending_reduce* self) {
	free(self->temp_file_name);
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

//***************************************** Interaccion Job *******************************************

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
int receive_result_map(int sockjob, t_result_map* result_map) {

	result_map->prot = receive_protocol_in_order(sockjob);

	switch (result_map->prot) {
	case MAP_OK:
		log_info(paranoid_log, "Map Realizado con Exito");
		break;

	case MAP_NOT_OK:
		log_error(paranoid_log, "No se Realizo el Map");
		break;

	case ERROR_IN_CONNECTION:
		log_error(paranoid_log, "Error con la conexion job nodo");
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

	int result = receive_int_in_order(sockjob, &(result_map->id_map));

	log_info(paranoid_log,"Map ID: %i",result_map->id_map);

	return result;
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

 		result = (result > 0) ? receive_int_in_order(socket, &(block_copy->id_nodo)) : result;
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


//***************************************** Principal *******************************************

//---------------------------------------------------------------------------
int increment_and_take_last_job_id() {

	int aux_last_job_id;

	pthread_mutex_lock(&mutex_ultimo_id);
	aux_last_job_id = ++last_id_job;
	pthread_mutex_unlock(&mutex_ultimo_id);

	return aux_last_job_id;
}

//---------------------------------------------------------------------------
void cambiar_carga_nodo(uint32_t id_nodo, int cambio) {

	t_info_nodo* info_nodo;

	int _isNodeSearched(t_info_nodo* info_nodo) {
		return info_nodo->id_nodo == id_nodo;
	}

	pthread_mutex_lock(&node_list_mutex);
	info_nodo = list_find(info_nodos, (void *) _isNodeSearched);
	if (info_nodo == NULL) {
		log_error(paranoid_log, "No Existe el Nodo cargado en lista cuando debería!!!");
		if(cambio > 0) {
			log_error(paranoid_log, "(Intentando Incrementar Carga de Ops en curso)");
		} else {
			log_error(paranoid_log, "(Intentando Decrementar Carga de Ops en curso)");
		}
	} else {
		(info_nodo->cant_ops_en_curso)+=cambio;
	}
	pthread_mutex_unlock(&node_list_mutex);
}

//---------------------------------------------------------------------------
void incrementar_carga_nodo(uint32_t id_nodo) {
	cambiar_carga_nodo(id_nodo,1);
}

//---------------------------------------------------------------------------
void decrementar_carga_nodo(uint32_t id_nodo) {
	cambiar_carga_nodo(id_nodo,-1);
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
void generear_unmapped_blocks(t_info_file* info_file, t_list* unmapped_blocks) {
	int i;

	t_src_block* src_block;

	for(i=0; i< info_file->amount_blocks; i++) {
		src_block = malloc(sizeof(t_src_block));
		src_block->path_file = info_file->path;
		src_block->block_number = i;

		list_add(unmapped_blocks, src_block);
	}
}

//---------------------------------------------------------------------------
int get_info_files_from_FS(char **paths_files, t_list* file_list, t_list* unmapped_blocks) {

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
		void _mostrar_info_file_Y_generar_unmapped_blocks(t_info_file* info_file) {
			mostrar_info_file(info_file);
			generear_unmapped_blocks(info_file, unmapped_blocks);
		}

		log_debug(paranoid_log, "Info de archivos solicitados con exito");
		list_iterate(file_list, (void *) _mostrar_info_file_Y_generar_unmapped_blocks);
	}
	return result;
}


//---------------------------------------------------------------------------
uint32_t count_maps_in_node(t_list* temps_nodo, uint32_t id_nodo) {

	int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->id_nodo == id_nodo;
	}

	t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

	if(temp_nodo == NULL)
		return 0;

	return temp_nodo->amount_maps;
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

	int score = - carga * 2;

	if(combiner) {
		cant_mismo_nodo = 0;

		cant_mismo_nodo += count_maps_in_node(temps_nodo, block_copy->id_nodo) * 3;
		cant_mismo_nodo += count_pending_maps_in_node(pending_maps, block_copy->id_nodo);

		score += cant_mismo_nodo;
	}

	return score;
}


//---------------------------------------------------------------------------
t_map_dest* planificar_map(t_info_job info_job, t_src_block* src_block, uint32_t* last_id_map, t_list* temps_nodo, t_list* pending_maps) {

	log_info(paranoid_log, "Planificando map del Archivo:%s Bloque: %i ", src_block->path_file, src_block->block_number);

	t_block_copy* selected_copy;
	t_map_dest* self;

	t_list* block_copies = list_create();

	if (locate_block_in_FS(src_block->path_file, src_block->block_number, block_copies) <= 0) {
		log_error(paranoid_log, "No se pudieron localizar las copias del Archivo: %s, Bloque: %i", src_block->path_file, src_block->block_number);
		list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);
		return NULL;
	}

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
		self->temp_file_name = string_from_format("map_%i_%i.tmp", info_job.id_job, self->block);
	} else {
		log_error(paranoid_log,"No hay copias Activas del Archivo:%s Bloque: %i ", src_block->path_file, src_block->block_number);
		list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);
		return NULL;
	}

	list_destroy_and_destroy_elements(block_copies, (void *) free_block_copy);

	int found = 0;
	int result = update_location_node_ifnot_here(self->id_nodo,&found);

	if(result <= 0) {
		log_error(paranoid_log,"No se pudo obtener la dirección del Nodo a Mapear");
		return NULL;
	}

	if(found) {
		return self;
	}

	(*last_id_map)--;
	return planificar_map(info_job, src_block, last_id_map, temps_nodo, pending_maps); //planifica de nuevo porque se calló el nodo
}

//---------------------------------------------------------------------------
void add_pending_map(t_list* pending_maps, t_src_block* src_block, t_map_dest* map_dest) {

	t_pending_map* pending_map;

	pending_map = malloc(sizeof(t_pending_map));
	pending_map->src_block = src_block;
	pending_map->map_dest = map_dest;

	list_add(pending_maps, pending_map);

	incrementar_carga_nodo(map_dest->id_nodo);
}

//---------------------------------------------------------------------------
void remove_pending_map(t_list* pending_maps, uint32_t id_map, t_list* temps_nodo, t_list* unmapped_blocks) {

	int _isSearchedPendingMap(t_pending_map* pending_map) {
		return pending_map->map_dest->id_map == id_map;
	}

	t_pending_map* pending_map = list_find(pending_maps, (void *) _isSearchedPendingMap);

	int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->id_nodo == pending_map->map_dest->id_nodo;
	}

	t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

	if(temp_nodo == NULL) {
		temp_nodo = malloc(sizeof(t_temp_nodo));
		temp_nodo->id_nodo = pending_map->map_dest->id_nodo;
		temp_nodo->orphan_temps = list_create();
		temp_nodo->amount_maps = 0;
		list_add(temps_nodo, temp_nodo);
	}

	t_temp* orphan_temp = malloc(sizeof(t_temp));
	orphan_temp->source = SOURCE_MAP;
	orphan_temp->path_temp = strdup(pending_map->map_dest->temp_file_name);
	orphan_temp->childs = pending_map->src_block;

	int _is_mapped_src_block(t_src_block* src_block){
		return src_block == pending_map->src_block;
	}
	list_remove_by_condition(unmapped_blocks,(void *) _is_mapped_src_block);

	(temp_nodo->amount_maps)++;
	list_add(temp_nodo->orphan_temps, orphan_temp);

	decrementar_carga_nodo(pending_map->map_dest->id_nodo);

	list_remove_and_destroy_by_condition(pending_maps, (void *) _isSearchedPendingMap, (void *) free_pending_map);
}

//---------------------------------------------------------------------------
int plan_maps(t_info_job info_job, t_list* unmapped_blocks, t_list* temps_nodo, int sockjob) {

	t_list* pending_maps = list_create();
	t_pending_map* pending_map;
	t_map_dest* map_dest;
	int result = 1;
	uint32_t last_id_map = 0;

	void _map_actions(t_src_block* unmapped_block) {
		if(result > 0) {
			map_dest = planificar_map(info_job, unmapped_block, &last_id_map, temps_nodo, pending_maps);
			result = (map_dest != NULL) ? order_map_to_job(map_dest, sockjob) : -2;

			if (result > 0) {
				add_pending_map(pending_maps, unmapped_block, map_dest);
			}
		}
	}
	list_iterate(unmapped_blocks, (void *) _map_actions);

	t_result_map result_map;

	int _isSearchedPendingMap(t_pending_map* pending_map) {
		return pending_map->map_dest->id_map == result_map.id_map;
	}

	while ((!list_is_empty(pending_maps)) && (result > 0)) {

		result = receive_result_map(sockjob, &result_map);

		if (result > 0) {
			switch (result_map.prot) {
				case MAP_OK:
					remove_pending_map(pending_maps, result_map.id_map, temps_nodo, unmapped_blocks);
					break;

				case NODO_NOT_FOUND: case MAP_NOT_OK: case ERROR_IN_CONNECTION:
					pending_map = list_find(pending_maps, (void *) _isSearchedPendingMap);

					int found;
					result =update_location_node_from_FS(pending_map->map_dest->id_nodo,&found);
					if((result > 0) && (!found)) {
						int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
							return temp_nodo->id_nodo == pending_map->map_dest->id_nodo;
						}

						t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

						compute_lost_maps(unmapped_blocks, temp_nodo->orphan_temps);
						list_remove_element(temps_nodo, temp_nodo);
						free_temp_nodo(temp_nodo);
					}

					if(result > 0) {
						decrementar_carga_nodo(pending_map->map_dest->id_nodo);
						free_map_dest(pending_map->map_dest);
						pending_map->map_dest = planificar_map(info_job, pending_map->src_block, &last_id_map, temps_nodo, pending_maps);
						result = (pending_map->map_dest != NULL) ? order_map_to_job(pending_map->map_dest, sockjob) : -2;
						if (result > 0) {
							incrementar_carga_nodo(pending_map->map_dest->id_nodo);
						}
					}
					break;
			}
		}
	}

	list_destroy_and_destroy_elements(pending_maps, (void *) free_pending_map);
	return result;
}

//---------------------------------------------------------------------------
int receive_result_reduce(int sockjob, t_result_reduce* result_reduce) {

	result_reduce->prot = receive_protocol_in_order(sockjob);

	switch (result_reduce->prot) {
	case REDUCE_OK:
		log_info(paranoid_log, "Reduce Realizado con Exito");
		break;

	case REDUCE_NOT_OK:
		log_warning(paranoid_log, "No se realizo el reduce, se volverá a intentar");
		break;

	case NODO_NOT_FOUND:
		log_warning(paranoid_log, "No se encontró el NODO donde mapear");
		break;

	case TEMP_NOT_FOUND:
		log_warning(paranoid_log, "No se encontraron Archivos Temporales a Reducir ");
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
		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", result_reduce->prot);
		return -1;
		break;
	}

	int result = receive_int_in_order(sockjob, &(result_reduce->id_nodo));
	if(result > 0)
		log_info(paranoid_log, "Nodo ID: %i",result_reduce->id_nodo);

	return result;
}

//---------------------------------------------------------------------------
int buffer_add_temp_node(t_buffer* order_reduce_buff, t_temp_nodo* temp_nodo) {

	void _buffer_add_temp(t_temp* temp) {
		buffer_add_string(order_reduce_buff, temp->path_temp);
	}

	uint32_t amount_files_in_node;
	amount_files_in_node = list_size(temp_nodo->orphan_temps);

	buffer_add_int(order_reduce_buff, temp_nodo->id_nodo);
	uint32_t ip_nodo = 0, puerto_nodo = 0;

	int result = locate_nodo(temp_nodo->id_nodo, &ip_nodo, &puerto_nodo);

	if(result <= 0)
		return result;

	buffer_add_int(order_reduce_buff, ip_nodo);
	buffer_add_int(order_reduce_buff, puerto_nodo);
	buffer_add_int(order_reduce_buff,amount_files_in_node);

	list_iterate(temp_nodo->orphan_temps, (void *) _buffer_add_temp);

	return result;
}

//---------------------------------------------------------------------------
uint32_t id_from_nodo_host(t_list* temps_nodo) {

	int _amount_maps_in_nodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->amount_maps;
	}

	t_temp_nodo* _mayorSegunCantidadTemps(t_temp_nodo* nodo1, t_temp_nodo* nodo2) {
		return mayorSegun(nodo1, nodo2, (void *) _amount_maps_in_nodo);
	}

	t_temp_nodo* nodo_host = foldl1((void *) _mayorSegunCantidadTemps,temps_nodo);

	return nodo_host->id_nodo;
}

//---------------------------------------------------------------------------
void add_pending_reduce(t_temp_nodo* temp_nodo, t_list* pending_reduces, char* path_name, uint32_t id_reduce) {

	t_pending_reduce* pending_reduce = malloc(sizeof(t_pending_reduce));
	pending_reduce->id_reduce = id_reduce;
	pending_reduce->id_nodo = temp_nodo->id_nodo;
	pending_reduce->temp_file_name = strdup(path_name);

	incrementar_carga_nodo(temp_nodo->id_nodo);
	list_add(pending_reduces, pending_reduce);
}

//---------------------------------------------------------------------------
int order_partial_reduce(t_info_job info_job, t_temp_nodo* temp_nodo, int sockjob, uint32_t id_reduce, char* partial_name) {

	uint32_t ip_nodo = 0;
	uint32_t puerto_nodo = 0;
	t_buffer* order_reduce_buff;

	void _buffer_add_temp(t_temp* temp) {
		buffer_add_string(order_reduce_buff, temp->path_temp);
	}

	int result = locate_nodo(temp_nodo->id_nodo, &ip_nodo, &puerto_nodo);

	if(result <= 0) {
		return -1;
	}
	uint32_t amount_files_in_node;
	amount_files_in_node = list_size(temp_nodo->orphan_temps);

	order_reduce_buff = buffer_create_with_protocol(ORDER_PARTIAL_REDUCE);
	buffer_add_int(order_reduce_buff, id_reduce);
	buffer_add_string(order_reduce_buff, partial_name);
	buffer_add_int(order_reduce_buff, temp_nodo->id_nodo);
	buffer_add_int(order_reduce_buff, ip_nodo);
	buffer_add_int(order_reduce_buff, puerto_nodo);
	buffer_add_int(order_reduce_buff,amount_files_in_node);

	list_iterate(temp_nodo->orphan_temps, (void *) _buffer_add_temp);

	log_info(paranoid_log,"Enviando Orden de Reduce Parcial Nodo ID:%i, Nombre Temporal: %s",temp_nodo->id_nodo, partial_name);

	result = send_buffer_and_destroy(sockjob,order_reduce_buff);

	if (result <= 0) {
		log_error(paranoid_log, "No se Pudo enviar la Orden de Reduce Parcial al Job");
	}

	return result;
}

//---------------------------------------------------------------------------
t_pending_reduce* pending_reduce_with_id_nodo(t_list* pending_reduces, uint32_t id_nodo) {
	int _isSearchedPendingReduce(t_pending_reduce* pending_reduce) {
		return pending_reduce->id_nodo == id_nodo;
	}

	return list_find(pending_reduces, (void *) _isSearchedPendingReduce);
}

//---------------------------------------------------------------------------
t_temp* create_temp_with_all_orphans(t_list* pending_reduces, uint32_t id_nodo, t_temp_nodo* temp_nodo) {

	t_pending_reduce* pending_reduce = pending_reduce_with_id_nodo(pending_reduces, id_nodo);

	t_temp* new_temp = malloc(sizeof(t_temp));
	new_temp->source = SOURCE_REDUCE;
	new_temp->path_temp = strdup(pending_reduce->temp_file_name);
	new_temp->childs = temp_nodo->orphan_temps;

	temp_nodo->orphan_temps = list_create();
	return new_temp;
}


//---------------------------------------------------------------------------
t_temp* create_temp_with_reduced_orphans(t_list* pending_reduces, uint32_t id_nodo, t_temp_nodo* temp_nodo, t_list* reduced_paths) {

	t_pending_reduce* pending_reduce = pending_reduce_with_id_nodo(pending_reduces, id_nodo);

	t_temp* new_temp = malloc(sizeof(t_temp));
	new_temp->source = SOURCE_REDUCE;
	new_temp->path_temp = strdup(pending_reduce->temp_file_name);
	new_temp->childs = temp_nodo->orphan_temps;

	int _has_path_reduced(t_temp* temp) {
		int _same_path_as_temp(char* path) {
			return strcmp(path,temp->path_temp) == 0;
		}
		return list_any_satisfy(reduced_paths, (void *) _same_path_as_temp);
	}
	new_temp->childs = list_filter(temp_nodo->orphan_temps, (void *) _has_path_reduced);
	list_remove_all_elements_in(temp_nodo->orphan_temps, new_temp->childs);

	return new_temp;
}

//---------------------------------------------------------------------------
int allNodesWithSingleTemp(t_list* temps_nodo) {

	int _hasOnlyOneOrphanTemp(t_temp_nodo* temp_nodo) {
		return list_size(temp_nodo->orphan_temps) == 1;
	}

	return list_all_satisfy(temps_nodo, (void *) _hasOnlyOneOrphanTemp);
}

//---------------------------------------------------------------------------
int readyToFinalReduce(t_list* unmapped_blocks, t_list* temps_nodo) {
	return list_is_empty(unmapped_blocks) && allNodesWithSingleTemp(temps_nodo);
}

//---------------------------------------------------------------------------
void compute_lost_maps(t_list* unmapped_blocks, t_list* lost_temps) {

	void _add_src_blocks_to_unmapped_blocks(t_temp* temp) {
		if(temp->source == SOURCE_REDUCE) {
			list_iterate(temp->childs, (void *) _add_src_blocks_to_unmapped_blocks);
		} else {
			list_add(unmapped_blocks, temp->childs);
			temp->childs = NULL;
		}

	}
	list_iterate(lost_temps, (void *) _add_src_blocks_to_unmapped_blocks);
}

//---------------------------------------------------------------------------
int receive_lost_paths(int sockjob, t_list** lost_temps) {
	*lost_temps = list_create();
	int result;
	uint32_t amount_paths;
	int i;
	result = receive_int_in_order(sockjob, &amount_paths);

	for(i=0; (i<amount_paths) && (result > 0); i++) {
		char* path;
		result = receive_dinamic_array_in_order(sockjob,(void **) &path);
		if(result > 0) {
			list_add(*lost_temps, path);
		} else {
			free(path);
		}
	}

	return result;
}

//---------------------------------------------------------------------------
void replan_not_found_temp(t_temp* temp, t_list* orphan_temps, t_list* unmapped_blocks) {

	if(temp->source == SOURCE_REDUCE) {
		list_add_all(orphan_temps, temp->childs);
	} else {
		list_add(unmapped_blocks, temp->childs);
	}
}

//---------------------------------------------------------------------------
void mostrar_src_block(t_src_block* src_block) {
	log_debug(paranoid_log,"Src Block: %s %i", src_block->path_file, src_block->block_number);
}

//---------------------------------------------------------------------------
void mostrar_temp(t_temp* temp) {
	log_debug(paranoid_log,"Temporal: %s",temp->path_temp);
	if(temp->source == SOURCE_REDUCE) {
		log_debug(paranoid_log,"Temporal De: Reduce");
		log_debug(paranoid_log,"Hijos: ");
		list_iterate(temp->childs, (void *) mostrar_temp);
	} else {
		log_debug(paranoid_log,"Temporal De: Map");
		mostrar_src_block(temp->childs);
	}
	log_debug(paranoid_log,"");
}

//---------------------------------------------------------------------------
int receive_result_final_reduce(int sockjob, int* result_prot) {

	*result_prot = receive_protocol_in_order(sockjob);

	switch (*result_prot) {
	case REDUCE_OK:
		log_info(paranoid_log, "Final Reduce Realizado con Exito");
		break;

	case REDUCE_NOT_OK:
		log_error(paranoid_log, "Final Reduce no se pudo Realizar");
		break;

	case NODO_NOT_FOUND:
		log_warning(paranoid_log, "No se encontraron todos los NODOS donde Reducir");
		break;

	case TEMP_NOT_FOUND:
		log_warning(paranoid_log, "No se encontraron todos los Archivos Temporales a Reducir ");
		break;

	case DISCONNECTED:
		log_error(paranoid_log, "Job se Desconectó de forma inesperada");
		return 0;
		break;

	case -1:
		log_error(paranoid_log, "No se pudo recibir el resultado del Reduce");
		return -1;
		break;

	default:
		log_error(paranoid_log, "Protocolo Inesperado %i (MaRTA PANIC!)", *result_prot);
		return -1;
		break;
	}

	return 1;

}

//---------------------------------------------------------------------------
int plan_final_reduce(t_info_job info_job, t_list* temps_nodo, int sockjob, t_final_result* final_result, t_list* unmapped_blocks, uint32_t* last_id_reduce) {

	uint32_t nodes_amount = list_size(temps_nodo);
	if(nodes_amount == 0) {
		log_error(paranoid_log,"No se puede Realizar Reduce de 0 Nodos!!!");
		return -1;
	}

	(*last_id_reduce)++;


	final_result->id_nodo = id_from_nodo_host(temps_nodo);
	final_result->file_name = string_from_format("final_result_job_%i_%i.tmp",*last_id_reduce,info_job.id_job);

	log_info(paranoid_log,"Comienzo de Final Reduce con resultado en Nodo: %i, Temp: %s",final_result->id_nodo, final_result->file_name);

	t_buffer* order_reduce_buff = buffer_create_with_protocol(ORDER_REDUCE);
	buffer_add_int(order_reduce_buff,*last_id_reduce);
	buffer_add_string(order_reduce_buff,final_result->file_name);
	buffer_add_int(order_reduce_buff, final_result->id_nodo);
	buffer_add_int(order_reduce_buff, nodes_amount);

	int result = 1;

	void _buffer_add_temp_node(t_temp_nodo* temp_nodo) {
		if((result > 0) && (list_size(temp_nodo->orphan_temps)>0)) {
			result = buffer_add_temp_node(order_reduce_buff, temp_nodo);
		}
	}
	list_iterate(temps_nodo, (void *) _buffer_add_temp_node);

	result =  (result > 0) ? send_buffer_and_destroy(sockjob,order_reduce_buff) : result;

	if(result < 0) {
		log_error(paranoid_log, "No se Pudo enviar la Orden de Reduce al Job");
		free_final_result(final_result);
		return -1;
	}

	int result_prot;
	result = receive_result_final_reduce(sockjob, &result_prot);

	if(result_prot == REDUCE_OK){
		return 1;
	}

	if(result_prot == REDUCE_NOT_OK) {
		return 2;
	}

	free_final_result(final_result);


	uint32_t id_nodo;
	int found;

	int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->id_nodo == id_nodo;
	}

	if(result_prot == NODO_NOT_FOUND){
		int i;
		uint32_t amount_nodos;
		result = receive_int_in_order(sockjob,&amount_nodos);

		for(i=0; (i<amount_nodos) && (result > 0); i++) {
			result = receive_int_in_order(sockjob,&id_nodo);
			t_temp_nodo* temp_nodo = (result > 0) ? list_find(temps_nodo, (void *) _isSearchedTempNodo) : NULL;

			result = (result > 0) ? update_location_node_from_FS(id_nodo, &found) : result;

			if((result > 0) && (!found)) {
				compute_lost_maps(unmapped_blocks, temp_nodo->orphan_temps);
				list_remove_element(temps_nodo, temp_nodo);
				free_temp_nodo(temp_nodo);
			}
		}


		if(result <= 0) {
			return -1;
		}

		return 2;
	}

	if(result_prot == TEMP_NOT_FOUND) {

		int i;
		uint32_t amount_temps;
		result = receive_int_in_order(sockjob,&amount_temps);
		char* path_temp_not_found;

		for(i=0; (i<amount_temps) && (result > 0); i++) {
			result = (result > 0) ? receive_int_in_order(sockjob,&id_nodo) : result;
			result = receive_dinamic_array_in_order(sockjob,(void **) &path_temp_not_found);

			if(result > 0) {
				log_warning(paranoid_log, "Temporal no encontrado: %s", path_temp_not_found);
				getchar();
				t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

				int _has_path_not_found(t_temp* temp) {
					return strcmp(path_temp_not_found,temp->path_temp) == 0;
				}
				t_temp* temp_not_found = list_find(temp_nodo->orphan_temps, (void *) _has_path_not_found);

				replan_not_found_temp(temp_not_found,temp_nodo->orphan_temps, unmapped_blocks);
				list_remove_element(temp_nodo->orphan_temps, temp_not_found);
				free_temp_alone(temp_not_found);
			}
			free(path_temp_not_found);
		}

		if(result <= 0) {
			return -1;
		}

		return 2;
	}


	return result;
}

//---------------------------------------------------------------------------
void take_not_found_temps_from_orphan(t_list* not_found_temps,t_list* lost_paths,t_list* orphan_temps){

	void _take_not_found_temp(char* path) {
		int _has_path_not_found(t_temp* temp) {
			return strcmp(path,temp->path_temp) == 0;
		}
		t_temp* temp = list_remove_by_condition(orphan_temps, (void *) _has_path_not_found);
		list_add(not_found_temps, temp);
	}
	list_iterate(lost_paths, (void *)_take_not_found_temp);
}

//---------------------------------------------------------------------------
int plan_combined_reduces(t_info_job info_job, t_list* temps_nodo, int sockjob, t_final_result* final_result) {

	uint32_t last_id_reduce = 0;
	int result = 1;
	int reduces_finished = 0;
	t_result_reduce result_reduce;
	t_list* pending_reduces = list_create();
	t_list* unmapped_blocks = list_create();

	int _isSearchedPendingReduce(t_pending_reduce* pending_reduce) {
		return pending_reduce->id_nodo == result_reduce.id_nodo;
	}

	int _isSearchedTempNodo(t_temp_nodo* temp_nodo) {
		return temp_nodo->id_nodo == result_reduce.id_nodo;
	}

	log_debug(paranoid_log,"Comenzando Reduces Combinados Job ID: %i",info_job.id_job);

	while((result > 0) && (!reduces_finished)) {

		while((result > 0) && (!readyToFinalReduce(unmapped_blocks, temps_nodo))) {
			if(!list_is_empty(unmapped_blocks)) {
				result = plan_maps(info_job, unmapped_blocks, temps_nodo, sockjob);
			}

			void _plan_partial_reduce(t_temp_nodo* temp_nodo) {
				if((result > 0) && (list_size(temp_nodo->orphan_temps)>0)) {
					uint32_t id = last_id_reduce++;
					char* partial_name = string_from_format("partial_reduce_%i_job_%i_.tmp", id, info_job.id_job);
					result = order_partial_reduce(info_job, temp_nodo, sockjob, last_id_reduce, partial_name);

					if(result > 0) {
						add_pending_reduce(temp_nodo, pending_reduces, partial_name, id);
					}
					free(partial_name);
				}
			}
			if(!allNodesWithSingleTemp(temps_nodo)) {
				list_iterate(temps_nodo, (void *) _plan_partial_reduce);
			}

			while((result > 0) && (!list_is_empty(pending_reduces))) {
				result = receive_result_reduce(sockjob, &result_reduce);

				t_temp_nodo* temp_nodo = list_find(temps_nodo, (void *) _isSearchedTempNodo);

				if (result > 0) {
					switch (result_reduce.prot) {
						int found;
						t_list* lost_paths;
						t_temp* new_temp;

						case REDUCE_OK:
							new_temp = create_temp_with_all_orphans(pending_reduces, result_reduce.id_nodo, temp_nodo);
							list_add(temp_nodo->orphan_temps, new_temp);
							break;

						case REDUCE_NOT_OK:
							break;

						case NODO_NOT_FOUND:
							result = update_location_node_from_FS(result_reduce.id_nodo, &found);

							if(result <= 0) {
								break;
							}
							if(!found) {
								compute_lost_maps(unmapped_blocks, temp_nodo->orphan_temps);
								list_remove_element(temps_nodo, temp_nodo);
								free_temp_nodo(temp_nodo);
							}
							break;

						case TEMP_NOT_FOUND:
							log_warning(paranoid_log,
									"No se Encontraron todos los temporales para realizar el Reduce en Nodo ID: %i, Replanificando...",
									result_reduce.id_nodo);

							result = receive_lost_paths(sockjob, &lost_paths);
							if(result > 0) {
								t_list* not_found_temps = list_create();
								take_not_found_temps_from_orphan(not_found_temps, lost_paths, temp_nodo->orphan_temps);

								void _replan_not_found_temp(t_temp* temp) {
									replan_not_found_temp(temp,temp_nodo->orphan_temps, unmapped_blocks);
								}
								list_iterate(not_found_temps, (void *) _replan_not_found_temp);
								list_destroy_and_destroy_elements(not_found_temps, (void *) free_temp_alone);
								list_destroy_and_destroy_elements(lost_paths, (void *) free);
							}
							break;
					}

					decrementar_carga_nodo(result_reduce.id_nodo);
					list_remove_and_destroy_by_condition(pending_reduces, (void *) _isSearchedPendingReduce, (void *) free_pending_reduce);
				}
			}
		}

//XXX si ya quedaron todos en uno saltearse ultimo reduce?
		result = plan_final_reduce(info_job, temps_nodo, sockjob, final_result, unmapped_blocks, &last_id_reduce);

		if(result == 1) {
			reduces_finished = 1;
		}
	}


	list_destroy_and_destroy_elements(unmapped_blocks,(void *) free_src_block);
	list_destroy_and_destroy_elements(pending_reduces,(void *) free_pending_reduce);

	if(result <= 0) {
		log_error(paranoid_log,"Fallo operación de Reduces Combinados");
		return -1;
	}

	return 1;
}

//---------------------------------------------------------------------------
int plan_unique_reduce(t_info_job info_job, t_list* temps_nodo, int sockjob, t_final_result* final_result) {

	uint32_t last_id_reduce = 0;

	int final_reduce_success= 0;
	int result = 1;
	t_list* unmapped_blocks = list_create();

	while((!final_reduce_success)&&(result > 0)) {
		if(!list_is_empty(unmapped_blocks)) {
			result = plan_maps(info_job, unmapped_blocks, temps_nodo, sockjob);
		}

		result = plan_final_reduce(info_job, temps_nodo, sockjob, final_result, unmapped_blocks, &last_id_reduce);

		if(result == 1) {
			final_reduce_success = 1;
		}
	}

	list_destroy_and_destroy_elements(unmapped_blocks,(void *) free_src_block);

	return 1;
}


//---------------------------------------------------------------------------
int receive_save_result(int socket, char* dest_path) {

	int prot;

	prot = receive_protocol_in_order(socket);

	switch (prot) {

	case SAVE_OK:
		log_debug(paranoid_log, "Se Guardó Correctamente el Resultado Final en: %s", dest_path);
		break;

	case SAVE_ABORT:
		log_error(paranoid_log, "No se pudo guardar el Resultado Final en: %s", dest_path);
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
		return -1;
		break;
	}
	return prot;
}

//---------------------------------------------------------------------------
int save_result_file_in_MDFS(t_info_job info_job, t_final_result* final_result) {


	t_buffer* save_result_request_buff = buffer_create_with_protocol(SAVE_RESULT_REQUEST);
	buffer_add_int(save_result_request_buff,final_result->id_nodo);
	buffer_add_string(save_result_request_buff,final_result->file_name);
	buffer_add_string(save_result_request_buff,info_job.path_result);
	free_final_result(final_result);

	pthread_mutex_lock(&conex_fs_ready);
	int result = send_buffer_and_destroy(socket_fs, save_result_request_buff);

	result = (result > 0) ? receive_save_result(socket_fs, info_job.path_result) : result;
	pthread_mutex_unlock(&conex_fs_ready);

	return result;
}

//---------------------------------------------------------------------------
void hilo_conex_job(t_hilo_job *job) {
	char *ip_job;
	int port_job;
	t_info_job info_job;
	t_list* file_list = list_create();
	t_list* temps_nodo = list_create();
	t_final_result final_result;

	int result = 1;

	getFromSocketAddrStd(job->socketaddr_cli, &ip_job, &port_job);

	log_info(paranoid_log, "Comienzo Hilo Job");

	log_info(paranoid_log, "Obteniendo New Job del Job IP: %s, Puerto: %i", ip_job, port_job);

	result = (result > 0) ? receive_info_job(job, &info_job) : result;


	t_list* unmapped_blocks = list_create();
	result = (result > 0) ? get_info_files_from_FS(info_job.paths_files, file_list, unmapped_blocks) : result;
	result = (result > 0) ? plan_maps(info_job, unmapped_blocks, temps_nodo, job->sockfd) : result;
	list_destroy_and_destroy_elements(unmapped_blocks, (void *) free_src_block);

	if(info_job.combiner){
		result = (result > 0) ? plan_combined_reduces(info_job, temps_nodo, job->sockfd, &final_result) : result;
	} else {
		result = (result > 0) ? plan_unique_reduce(info_job, temps_nodo, job->sockfd, &final_result) : result;
	}
	result = (result > 0) ? save_result_file_in_MDFS(info_job, &final_result) : result;

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
	levantar_arch_conf_marta(&conf);
//	int hilos_de_job = 0;

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

		} else {
			free_hilo_job(job);
		}

	}

	log_info(paranoid_log, "Esperando finalizacion de hilos de Job");
	list_iterate(lista_jobs, (void *) esperar_finalizacion_hilo_conex_job);

	list_destroy_and_destroy_elements(lista_jobs, (void *) free_hilo_job);

	free_conf_marta(&conf);

	end_var_globales();

	return EXIT_SUCCESS;
}
