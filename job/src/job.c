/*
 ============================================================================
 Name        : job.c
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
#include <commons/collections/list.h>
#include <commons/log.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h> //Esta la agregeue para poder definir int con tamaño especifico (uint32_t)
#include "../../connectionlib/connectionlib.h"
#include <pthread.h>

typedef struct {
	char* ip_MaRTA;
	int port_MaRTA;

	char* path_map;
	char* path_reduce;

	int combiner;
	char* paths_to_apply_files;
	char* path_result_file;
} conf_job;

typedef struct {
	uint32_t combiner;
	char* paths_to_apply_files;
	char* path_result_file;
} info_new_job;

typedef struct {
	uint32_t prot;
	uint32_t id_reduce;
	char* temp_file_name;
	uint32_t id_nodo_host;
	t_list *list_nodos;
}t_reduce_dest;

typedef struct {
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	t_list* path_temps;
}t_reduce_nodo_dest;

typedef struct {
	uint32_t id;
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	uint32_t block;
	char* temp_file_name;
} t_map_dest;

typedef enum {
	REDUCE, MAP
} t_operation;

typedef struct {
	t_operation operation;
	uint32_t id;
	pthread_t* thr;
} t_hilo_op;


//########################################  Prototipos  #########################################
void free_conf_job();
void free_info_job(info_new_job* info);

void free_map_dest(t_map_dest* self);
void free_hilo_op(t_hilo_op* self);
void free_reduce_dest(t_reduce_dest* self);
void free_reduce_nodo_dest(t_reduce_nodo_dest* self);

void mostrar_reduce_dest(t_reduce_dest* self);

void levantar_arch_conf_job(); // devuelve una estructura con toda la info del archivo de configuracion "job.cfg"
int enviar_nuevo_job_a_MaRTA(info_new_job info_job);
void esperar_instrucciones_de_MaRTA();
void set_new_job(info_new_job* new_job);
int solicitarConexionConNodo(char* ip_nodo, uint32_t puerto_nodo, uint32_t id_nodo);
int enviar_infoMap_job(int socket_job,t_map_dest* map_dest);
void hilo_map_job(t_map_dest* map_dest);
void hilo_reduce_job(t_reduce_dest* reduce_dest);
int enviar_infoReduce_job(int socket_job,t_reduce_dest* reduce_dest, t_reduce_nodo_dest* nodo_host);

//######################################  Variables Globales  #######################################
t_log* paranoid_log;

pthread_mutex_t conex_marta_ready;
int socket_marta;

conf_job* conf;
char *codigoMap;


//######################################  Funciones  #######################################
//---------------------------------------------------------------------------
void free_info_job(info_new_job* info) {
	free(info->path_result_file);
	free(info->paths_to_apply_files);
}

//---------------------------------------------------------------------------
void free_conf_job() {
	free(conf->ip_MaRTA);
	free(conf->path_map);
	free(conf->path_reduce);
	free(conf->paths_to_apply_files);
	free(conf->path_result_file);
	free(conf);
}


//-------------------------------------------------------------------------------------
void free_map_dest(t_map_dest* self) {
	free(self->temp_file_name);
	free(self);
}

//-------------------------------------------------------------------------------------
void free_hilo_op(t_hilo_op* self) {
	free(self->thr);
	free(self);
}

//-------------------------------------------------------------------------------------
int solicitarConexionConMarta() {

	log_debug(paranoid_log, "Solicitando conexión con MaRTA...");
	int socketfd_MaRTA = solicitarConexionCon(conf->ip_MaRTA, conf->port_MaRTA);

	if (socketfd_MaRTA != -1) {
		log_info(paranoid_log, "Conexión con MaRTA establecida IP: %s, Puerto: %i", conf->ip_MaRTA, conf->port_MaRTA);
	} else {
		log_error(paranoid_log, "Conexión con MaRTA FALLIDA!!! IP: %s, Puerto: %i", conf->ip_MaRTA, conf->port_MaRTA);
		exit(-1);
	}

	return socketfd_MaRTA;
}

//---------------------------------------------------------------------------
void set_new_job(info_new_job* new_job) {
	new_job->combiner = conf->combiner;
	new_job->path_result_file = strdup(conf->path_result_file);
	new_job->paths_to_apply_files = strdup(conf->paths_to_apply_files);
}

//---------------------------------------------------------------------------
void esperar_hilo_op(t_hilo_op* hilo_op) {

	void* ret_recep;

	if (pthread_join(*(hilo_op->thr), &ret_recep) != 0) {
		if(hilo_op->operation == MAP) {
			log_error(paranoid_log,"Error al hacer join del hilo Map ID: %i\n",hilo_op->id);
		} else {
			log_error(paranoid_log,"Error al hacer join del hilo Reduce ID: %i\n",hilo_op->id);
		}
	}
}


//---------------------------------------------------------------------------
int  receive_answer_map(int socket,uint32_t *answer_map) {

	*answer_map = receive_protocol_in_order(socket);

	switch(*answer_map) {

		case MAP_OK:
			log_debug(paranoid_log, "Se Realizó el Map Correctamente");
			break;

		case MAP_NOT_OK:
			log_error(paranoid_log, "No Se pudo realizar el Map");
			break;

		case INCORRECT_NODO:
			log_error(paranoid_log, "El nodo conectado no es el Buscado");
			break;

		case DISCONNECTED:
			log_error(paranoid_log, "Nodo se Desconectó de forma inesperada");
			return 0;
			break;

		case -1:
			return -1;
			break;

		default:
			log_error(paranoid_log, "Protocolo Inesperado %i (Job PANIC!)", *answer_map);
			return -1;
			break;
	}

	return *answer_map;
}

//---------------------------------------------------------------------------
int  receive_answer_reduce(int socket,uint32_t *answer_reduce) {

	*answer_reduce = receive_protocol_in_order(socket);

	switch(*answer_reduce) {

		case REDUCE_OK:
			log_debug(paranoid_log, "Se Realizó el Reduce Correctamente");
			break;

		case REDUCE_NOT_OK:
			log_error(paranoid_log, "No Se pudo realizar el Map");
			break;

		case INCORRECT_NODO:
			log_error(paranoid_log, "El nodo conectado no es el Buscado");
			break;

		case NODO_NOT_FOUND:
			log_error(paranoid_log, "No se encontraron los nodos correspondientes");
			break;

		case TEMP_NOT_FOUND:
			log_error(paranoid_log, "No se encontraron algunos temporales");
			break;

		case DISCONNECTED:
			log_error(paranoid_log, "Nodo se Desconectó de forma inesperada");
			return 0;
			break;

		case -1:
			return -1;
			break;

		default:
			log_error(paranoid_log, "Protocolo Inesperado %i (Job PANIC!)", *answer_reduce);
			return -1;
			break;
	}

	return *answer_reduce;
}


//---------------------------------------------------------------------------
void hilo_map_job(t_map_dest* map_dest) {

	t_buffer* result_map_buff;
	char *ip_nodo = from_int_to_inet_addr(map_dest->ip_nodo);
	int result;

	log_info(paranoid_log, "Realizando Operación de Map ID:%i, en Nodo: %i, IP: %s, Port: %i, Block: %i Temp: %s",
			map_dest->id, map_dest->id_nodo, ip_nodo, map_dest->puerto_nodo, map_dest->block,
			map_dest->temp_file_name);

	int socket_nodo = solicitarConexionConNodo(ip_nodo, map_dest->puerto_nodo, map_dest->id_nodo);

	result = (socket_nodo != -1) ? enviar_infoMap_job(socket_nodo, map_dest) : -1;
	uint32_t answer_map = 0;
	result = (result > 0) ? receive_answer_map(socket_nodo, &answer_map) : result;

	if(socket_nodo != -1) {
		if(result > 0) {
			if(answer_map == INCORRECT_NODO) {
				result_map_buff = buffer_create_with_protocol(NODO_NOT_FOUND);
			} else {
				result_map_buff = buffer_create_with_protocol(answer_map);
			}
			close(socket_nodo);
		} else {
			result_map_buff = buffer_create_with_protocol(ERROR_IN_CONNECTION);
		}
	} else {
		result_map_buff = buffer_create_with_protocol(NODO_NOT_FOUND);
	}
	buffer_add_int(result_map_buff,map_dest->id);
	pthread_mutex_lock(&conex_marta_ready);
	result = send_buffer_and_destroy(socket_marta,result_map_buff);
	pthread_mutex_unlock(&conex_marta_ready);

	if(result <= 0) {
		log_error(paranoid_log,"No se pudo enviar respuesta de Map a MaRTA");
	}

	free(ip_nodo);
	free_map_dest(map_dest);
}


//---------------------------------------------------------------------------
void hilo_reduce_job(t_reduce_dest* reduce_dest) {

	int result = 1;
	uint32_t answer_reduce = 0;

	mostrar_reduce_dest(reduce_dest);

	int _isNodoHost(t_reduce_nodo_dest* nodo) {
		return reduce_dest->id_nodo_host == nodo->id_nodo;
	}
	t_reduce_nodo_dest* nodo_host = list_remove_by_condition(reduce_dest->list_nodos, (void *)_isNodoHost);

	char *ip_nodo = from_int_to_inet_addr(nodo_host->ip_nodo);
	int socket_nodo = solicitarConexionConNodo(ip_nodo, nodo_host->puerto_nodo, nodo_host->id_nodo);

	if(socket_nodo != 1) {
		result = enviar_infoReduce_job(socket_nodo, reduce_dest, nodo_host);
		result = (result > 0) ? receive_answer_reduce(socket_nodo, &answer_reduce) : result;
	} else {
		answer_reduce = INCORRECT_NODO;
	}

	if(result <= 0) {
		answer_reduce = REDUCE_NOT_OK;
	}
	t_buffer* reduce_result_buff;

	switch(answer_reduce) {

		case REDUCE_OK: case REDUCE_NOT_OK:
			reduce_result_buff = buffer_create_with_protocol(answer_reduce);
			if(reduce_dest->prot == ORDER_PARTIAL_REDUCE) {
				buffer_add_int(reduce_result_buff, reduce_dest->id_nodo_host);
			}
			break;

		case INCORRECT_NODO:
			reduce_result_buff = buffer_create_with_protocol(NODO_NOT_FOUND);
			if(reduce_dest->prot == ORDER_REDUCE) {
				buffer_add_int(reduce_result_buff, 1);
				buffer_add_int(reduce_result_buff, nodo_host->id_nodo);
			}
			break;

		case NODO_NOT_FOUND:
			reduce_result_buff = buffer_create_with_protocol(NODO_NOT_FOUND);
			if(reduce_dest->prot == ORDER_REDUCE) {
				uint32_t i,amount_nodes = 0;
				result = receive_int_in_order(socket_nodo, &amount_nodes);
				buffer_add_int(reduce_result_buff, amount_nodes);

				for(i=0;(i<amount_nodes) && (result > 0); i++) {
					uint32_t id_nodo = 0;
					result = receive_int_in_order(socket_nodo, &id_nodo);
					buffer_add_int(reduce_result_buff, id_nodo);
				}

				if(result <= 0) {
					buffer_destroy(reduce_result_buff);
					reduce_result_buff = buffer_create_with_protocol(REDUCE_NOT_OK);
				}
			}
			break;

		case TEMP_NOT_FOUND:
			reduce_result_buff = buffer_create_with_protocol(TEMP_NOT_FOUND);
			uint32_t i,amount_temps = 0;
			result = receive_int_in_order(socket_nodo, &amount_temps);
			buffer_add_int(reduce_result_buff, amount_temps);
			for(i=0;(i<amount_temps) && (result > 0); i++) {
				uint32_t id_nodo = 0;
				result = receive_int_in_order(socket_nodo, &id_nodo);
				if(reduce_dest->prot == ORDER_REDUCE) {
					buffer_add_int(reduce_result_buff, id_nodo);
				}
				char* path = NULL;
				result = (result > 0) ? receive_dinamic_array_in_order(socket_nodo, (void **) &path) : result;
				if(path != NULL) {
					buffer_add_string(reduce_result_buff, path);
					free(path);
				}
			}

			if(result <= 0) {
				buffer_destroy(reduce_result_buff);
				reduce_result_buff = buffer_create_with_protocol(REDUCE_NOT_OK);
			}
			break;

		default:
			reduce_result_buff = buffer_create_with_protocol(REDUCE_NOT_OK);
			break;


	}

	if((socket_nodo != -1) && (result > 0)) {
		close(socket_nodo);
	}

	pthread_mutex_lock(&conex_marta_ready);
	result = (result > 0) ? send_buffer_and_destroy(socket_marta, reduce_result_buff) : result;
	pthread_mutex_unlock(&conex_marta_ready);


	free(ip_nodo);
	free_reduce_nodo_dest(nodo_host);
	free_reduce_dest(reduce_dest);
}


//---------------------------------------------------------------------------
int enviar_infoMap_job(int socket_job,t_map_dest* map_dest){
	int result = 1;

	t_buffer* map_to_Nodo_buff;
	map_to_Nodo_buff = buffer_create_with_protocol(EXECUTE_MAP);


	buffer_add_int(map_to_Nodo_buff, map_dest->id_nodo);
	buffer_add_int(map_to_Nodo_buff, map_dest->block);
	buffer_add_string(map_to_Nodo_buff, map_dest->temp_file_name);
	result = send_buffer_and_destroy(socket_job, map_to_Nodo_buff);

	result = (result > 0) ? send_entire_file_by_parts(socket_job, conf->path_map, MAX_PART_SIZE) : result;


	if(result<= 0) {
		log_error(paranoid_log, "No se pudo enviar Instrucciones de Map");
	} else {
		log_info(paranoid_log, "Se envio correctamente Instrucciones de Map");
	}

	return result;
}

//---------------------------------------------------------------------------
int enviar_infoReduce_job(int socket_job,t_reduce_dest* reduce_dest, t_reduce_nodo_dest* nodo_host) {
	int result = 1;

	t_buffer* reduce_to_Nodo_buff;
	reduce_to_Nodo_buff = buffer_create_with_protocol(EXECUTE_REDUCE);

	buffer_add_int(reduce_to_Nodo_buff, nodo_host->id_nodo);
	buffer_add_string(reduce_to_Nodo_buff, reduce_dest->temp_file_name);
	buffer_add_int(reduce_to_Nodo_buff, list_size(nodo_host->path_temps));

	void _buffer_add_path(char* path) {
		buffer_add_string(reduce_to_Nodo_buff, path);
	}
	list_iterate(nodo_host->path_temps, (void *) _buffer_add_path);

	buffer_add_int(reduce_to_Nodo_buff, list_size(reduce_dest->list_nodos));

	void _buffer_add_nodo_guest(t_reduce_nodo_dest* nodo_guest) {
		buffer_add_int(reduce_to_Nodo_buff, nodo_guest->id_nodo);
		buffer_add_int(reduce_to_Nodo_buff, nodo_guest->ip_nodo);
		buffer_add_int(reduce_to_Nodo_buff, nodo_guest->puerto_nodo);

		buffer_add_int(reduce_to_Nodo_buff, list_size(nodo_guest->path_temps));

		list_iterate(nodo_guest->path_temps, (void *) _buffer_add_path);
	}
	list_iterate(reduce_dest->list_nodos, (void *) _buffer_add_nodo_guest);
	result = send_buffer_and_destroy(socket_job, reduce_to_Nodo_buff);
	result = (result > 0) ? send_entire_file_by_parts(socket_job, conf->path_reduce, MAX_PART_SIZE) : result;


	if(result<= 0) {
		log_error(paranoid_log, "No se pudo enviar Instrucciones de Reduce");
	} else {
		log_info(paranoid_log, "Se envio correctamente Instrucciones de Reduce");
	}
	return result;
}

//---------------------------------------------------------------------------}
void free_reduce_nodo_dest(t_reduce_nodo_dest* self) {
	list_destroy_and_destroy_elements(self->path_temps, (void *) free);
	free(self);
}

//---------------------------------------------------------------------------}
void free_reduce_dest(t_reduce_dest* self) {
	free(self->temp_file_name);
	list_destroy_and_destroy_elements(self->list_nodos, (void *) free_reduce_nodo_dest);
	free(self);
}

//---------------------------------------------------------------------------}
void mostrar_paths(char* self) {
	log_debug(paranoid_log, "		%s",self);
}

//---------------------------------------------------------------------------}
void mostrar_reduce_nodo_dest(t_reduce_nodo_dest* self) {
	char* ip = from_int_to_inet_addr(self->ip_nodo);
	log_debug(paranoid_log, "	Nodo: ID: %i, Ip: %s, Puerto: %i ",self->id_nodo, ip, self->puerto_nodo);
	free(ip);

	list_iterate(self->path_temps, (void *) mostrar_paths);
}

//---------------------------------------------------------------------------}
void mostrar_reduce_dest(t_reduce_dest* self) {
	log_debug(paranoid_log, "Reduce ID: %i, path_temp_result: %s ID NODO HOST: %i", self->id_reduce, self->temp_file_name, self->id_nodo_host);
	list_iterate(self->list_nodos, (void *) mostrar_reduce_nodo_dest);
}

//---------------------------------------------------------------------------
void esperar_instrucciones_de_MaRTA() {

	uint32_t prot;
	int finished = 0, error = 0;
	t_hilo_op* hilo_op;
	t_map_dest* map_dest;
	t_list* hilos_op = list_create();

	log_debug(paranoid_log, "Me Pongo a disposición de Ordenes de MaRTA");

	while ((!finished) && (!error)) {
		uint32_t nodes_amount = 0;
		int result = 1;
		uint32_t amount_files_in_node = 0;
		t_reduce_dest* reduce_dest;

		prot = receive_protocol_in_order(socket_marta);

		switch (prot) {

		case ORDER_MAP:
			map_dest = malloc(sizeof(t_map_dest));

			receive_int_in_order(socket_marta, &(map_dest->id));
			receive_int_in_order(socket_marta, &(map_dest->id_nodo));
			receive_int_in_order(socket_marta, &(map_dest->ip_nodo));
			receive_int_in_order(socket_marta, &(map_dest->puerto_nodo));
			receive_int_in_order(socket_marta, &(map_dest->block));
			receive_dinamic_array_in_order(socket_marta, (void **) &(map_dest->temp_file_name));

			hilo_op = malloc(sizeof(t_hilo_op));
			hilo_op->thr = malloc(sizeof(pthread_t));
			hilo_op->id = map_dest->id;
			hilo_op->operation = MAP;

			list_add(hilos_op,hilo_op);

			log_info(paranoid_log, "Creación de Hilo de Map - Job");
			if (pthread_create(hilo_op->thr, NULL, (void *) hilo_map_job, map_dest) != 0) {
				log_error(paranoid_log, "No se pudo crear Hilo de Map ID: %i",hilo_op->id);
				exit(-1);
			}
			break;

		case ORDER_PARTIAL_REDUCE:
			reduce_dest = malloc(sizeof(t_reduce_dest));
			reduce_dest->prot = prot;
			result = receive_int_in_order(socket_marta, &(reduce_dest->id_reduce));
			result = (result > 0) ? receive_dinamic_array_in_order(socket_marta, (void **) &(reduce_dest->temp_file_name)) : result;
			reduce_dest->list_nodos = list_create();

			t_reduce_nodo_dest* reduce_nodo_dest = malloc(sizeof(t_reduce_nodo_dest));
			result = (result > 0) ? receive_int_in_order(socket_marta, &(reduce_nodo_dest->id_nodo)) : result;
			result = (result > 0) ? receive_int_in_order(socket_marta, &(reduce_nodo_dest->ip_nodo)) : result;
			result = (result > 0) ? receive_int_in_order(socket_marta, &(reduce_nodo_dest->puerto_nodo)) : result;
			result = (result > 0) ? receive_int_in_order(socket_marta, &amount_files_in_node) : result;

			reduce_dest->id_nodo_host = reduce_nodo_dest->id_nodo;

			reduce_nodo_dest->path_temps = list_create();
			int i,j;
			for(i=0; i<amount_files_in_node; i++) {
				char* path_temp;
				result = (result > 0) ? receive_dinamic_array_in_order(socket_marta, (void **) &path_temp) : result;
				list_add(reduce_nodo_dest->path_temps, path_temp);
			}

			list_add(reduce_dest->list_nodos, reduce_nodo_dest);

			if(result > 0) {
				hilo_op = malloc(sizeof(t_hilo_op));
				hilo_op->thr = malloc(sizeof(pthread_t));
				hilo_op->id = reduce_dest->id_reduce;
				hilo_op->operation = REDUCE;

				list_add(hilos_op,hilo_op);

				log_info(paranoid_log, "Creación de Hilo de Reduce - Job");
				if (pthread_create(hilo_op->thr, NULL, (void *) hilo_reduce_job, reduce_dest) != 0) {
					log_error(paranoid_log, "No se pudo crear Hilo de Reduce ID: %i",hilo_op->id);
					exit(-1);
				}
			} else {
				free_reduce_dest(reduce_dest);
			}
			break;

		case ORDER_REDUCE:
			reduce_dest = malloc(sizeof(t_reduce_dest));
			reduce_dest->prot = prot;
			result = (result > 0)? receive_int_in_order(socket_marta, &(reduce_dest->id_reduce)) : result;
			result = (result > 0)? receive_dinamic_array_in_order(socket_marta, (void **) &(reduce_dest->temp_file_name)) : result;
			result = (result > 0)? receive_int_in_order(socket_marta, &(reduce_dest->id_nodo_host)) : result;
			result = (result > 0)? receive_int_in_order(socket_marta, &nodes_amount) : result;

			reduce_dest->list_nodos = list_create();

			for(i=0; (i<nodes_amount) && (result > 0); i++) {
				t_reduce_nodo_dest* reduce_nodo_dest = malloc(sizeof(t_reduce_nodo_dest));
				uint32_t amount_files_in_node = 0;

				result = (result > 0)? receive_int_in_order(socket_marta, &(reduce_nodo_dest->id_nodo)) : result;
				result = (result > 0)? receive_int_in_order(socket_marta, &(reduce_nodo_dest->ip_nodo)) : result;
				result = (result > 0)? receive_int_in_order(socket_marta, &(reduce_nodo_dest->puerto_nodo)) : result;
				result = (result > 0)? receive_int_in_order(socket_marta, &amount_files_in_node) : result;

				reduce_nodo_dest->path_temps = list_create();

				for(j=0; (j< amount_files_in_node)  && (result > 0); j++) {
					char* path_temp;
					result = (result > 0)? receive_dinamic_array_in_order(socket_marta,(void **) &path_temp) : result;
					list_add(reduce_nodo_dest->path_temps, path_temp);
				}

				list_add(reduce_dest->list_nodos, reduce_nodo_dest);
			}

			if(result > 0) {
				hilo_op = malloc(sizeof(t_hilo_op));
				hilo_op->thr = malloc(sizeof(pthread_t));
				hilo_op->id = reduce_dest->id_reduce;
				hilo_op->operation = REDUCE;

				list_add(hilos_op,hilo_op);

				log_info(paranoid_log, "Creación de Hilo de Reduce - Job");
				if (pthread_create(hilo_op->thr, NULL, (void *) hilo_reduce_job, reduce_dest) != 0) {
					log_error(paranoid_log, "No se pudo crear Hilo de Reduce ID: %i",hilo_op->id);
					exit(-1);
				}
			} else {
				free_reduce_dest(reduce_dest);
			}
			break;

		case FINISHED_JOB:
			log_info(paranoid_log, "Se Terminó Exitosamente el Job");
			finished = 1;
			break;

		case ABORTED_JOB:
			log_error(paranoid_log, "Se Produjo un Error en MaRTA y se Abortó el Job");
			error = 1;
			break;

		case DISCONNECTED:
			log_error(paranoid_log, "MaRTA se desconectó de forma inesperada");
			error = 2;
			break;

		case -1:
			log_error(paranoid_log, "No se pudo recibir instrucciones de MaRTA");
			error = 1;
			break;

		default:
			log_error(paranoid_log, "Instruccion de MaRTA Desconocida");
			error = 1;
			break;
		}
	}

	list_iterate(hilos_op, (void *) esperar_hilo_op);
	list_destroy_and_destroy_elements(hilos_op, (void *) free_hilo_op);
}

//---------------------------------------------------------------------------
int enviar_nuevo_job_a_MaRTA(info_new_job info_job) {

	int result = 0;
	t_buffer* new_job_buff;

	log_debug(paranoid_log, "Enviando Target del Job");

	new_job_buff = buffer_create_with_protocol(NUEVO_JOB);


	buffer_add_int(new_job_buff, info_job.combiner);
	buffer_add_string(new_job_buff, info_job.paths_to_apply_files);
	buffer_add_string(new_job_buff, info_job.path_result_file);

	result = send_buffer_and_destroy(socket_marta, new_job_buff);

	if (result == -1) {
		log_error(paranoid_log, "No se pudo enviar Target del Job");
		exit(-1);
	}

	return result;
}

//---------------------------------------------------------------------------
void levantar_arch_conf_job() {

	char** properties = string_split("IP_MARTA,PUERTO_MARTA,MAPPER,REDUCE,COMBINER,ARCHIVOS,RESULTADO", ",");
	t_config* conf_arch = config_create("job.cfg");

	if (has_all_properties(7, properties, conf_arch)) {
		conf->ip_MaRTA = strdup(config_get_string_value(conf_arch, properties[0]));
		conf->port_MaRTA = config_get_int_value(conf_arch, properties[1]);
		conf->path_map = strdup(config_get_string_value(conf_arch, properties[2]));
		conf->path_reduce = strdup(config_get_string_value(conf_arch, properties[3]));
		conf->combiner = config_get_int_value(conf_arch, properties[4]);
		conf->paths_to_apply_files = strdup(config_get_string_value(conf_arch, properties[5]));
		conf->path_result_file = strdup(config_get_string_value(conf_arch, properties[6]));
	} else {
		log_error(paranoid_log, "Faltan propiedades en archivo de Configuración");
		exit(-1);
	}
	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------

int solicitarConexionConNodo(char* ip_nodo, uint32_t puerto_nodo, uint32_t id_nodo) {

		log_debug(paranoid_log, "Solicitando conexión con nodo ID: %i...", id_nodo);
		int socketNodo = solicitarConexionCon(ip_nodo, puerto_nodo);

		if (socketNodo != -1) {
			log_info(paranoid_log, "Conexión con nodo establecida IP: %s, Puerto: %i", ip_nodo, puerto_nodo);
		} else {
			log_error(paranoid_log, "Conexión con nodo FALLIDA! IP: %s, Puerto: %i", ip_nodo, puerto_nodo);
		}

		return socketNodo;
}

//---------------------------------------------------------------------------
void init_var_globales() {

	pthread_mutex_init(&conex_marta_ready, NULL);

	conf = malloc(sizeof(conf_job));

	paranoid_log = log_create("./logJob.log", "JOB", 1, LOG_LEVEL_TRACE);
}

//--------------------------------------------------------------------------
void end_var_globales() {

	pthread_mutex_destroy(&conex_marta_ready);

	log_destroy(paranoid_log);
}


//###########################################################################
int main(void) {

	init_var_globales();

	levantar_arch_conf_job();

	socket_marta = solicitarConexionConMarta();

	info_new_job new_job;
	set_new_job(&new_job);

	enviar_nuevo_job_a_MaRTA(new_job);

	esperar_instrucciones_de_MaRTA();

	free_conf_job();
	free_info_job(&new_job);

	end_var_globales();
	return EXIT_SUCCESS;
}
