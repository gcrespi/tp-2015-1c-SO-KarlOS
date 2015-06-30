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
#include <netinet/in.h>
#include <arpa/inet.h>
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
	uint32_t id_map;
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	t_list *temp_files_names;
	t_list *list_nodos;
	char* temp_file_name;

}t_reduce_dest;

typedef struct {
	uint32_t id_reduce;
	pthread_t *thr;

}t_hilo_reduce;

typedef struct {
	uint32_t id_map;
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	uint32_t block;
	char* temp_file_name;
} t_map_dest;

typedef struct {
	uint32_t id_map;
	pthread_t* thr;
} t_hilo_map;


//########################################  Prototipos  #########################################
void free_conf_job();
void free_info_job(info_new_job* info);

void free_map_dest(t_map_dest* self);
void free_hilo_map(t_hilo_map* self);

void levantar_arch_conf_job(); // devuelve una estructura con toda la info del archivo de configuracion "job.cfg"
int enviar_nuevo_job_a_MaRTA(info_new_job info_job);
void esperar_instrucciones_de_MaRTA();
void set_new_job(info_new_job* new_job);
//int establecer_conexion_nodo(int socket_nodo);
//void enviar_script_a_nodo()
int solicitarConexionConNodo(char* ip_nodo, uint32_t puerto_nodo, uint32_t id_nodo);
//int recibir_info_map(int socket_job);
int enviar_infoMap_job(int socket_job,t_map_dest* map_dest);
void hilo_map_job(t_map_dest* map_dest);
int enviar_infoReduce_job(int socket_job,t_reduce_dest* map_dest);
void hilo_reduce_job(t_reduce_dest* reduce_dest);

//######################################  Variables Globales  #######################################
t_log* paranoid_log;

pthread_mutex_t conex_marta_ready;
int socket_marta;

conf_job* conf;


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
void free_hilo_map(t_hilo_map* self) {
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
void esperar_hilo_map(t_hilo_map* hilo_map) {

	void* ret_recep;

	if (pthread_join(*(hilo_map->thr), &ret_recep) != 0) {
		log_error(paranoid_log,"Error al hacer join del hilo Map ID: %i\n",hilo_map->id_map);
	}
}


//---------------------------------------------------------------------------
void hilo_map_job(t_map_dest* map_dest) {

	t_buffer* result_map_buff;
	char *ip_nodo = from_int_to_inet_addr(map_dest->ip_nodo);


	log_info(paranoid_log, "Realizando Operación de Map ID:%i, en Nodo: %i, IP: %s, Port: %i, Block: %i Temp: %s",
			map_dest->id_map, map_dest->id_nodo, ip_nodo, map_dest->puerto_nodo, map_dest->block,
			map_dest->temp_file_name);

	int socket_nodo = solicitarConexionConNodo(ip_nodo, map_dest->puerto_nodo, map_dest->id_nodo);

	if(socket_nodo != -1) {

		//XXX Realización del Map aquí
		if (enviar_infoMap_job(socket_nodo, map_dest) <= 0){
			log_error(paranoid_log, "no se pudo enviar el info job");
		}
		else {
			log_info(paranoid_log, "Se envio correctamente info job");
		}

	}

	pthread_mutex_lock(&conex_marta_ready);

	if(socket_nodo != -1) {
		result_map_buff = buffer_create_with_protocol(MAP_OK);
		buffer_add_int(result_map_buff,map_dest->id_map);
		send_buffer_and_destroy(socket_marta,result_map_buff);
	} else {
		result_map_buff = buffer_create_with_protocol(NODO_NOT_FOUND);
		buffer_add_int(result_map_buff,map_dest->id_map);
		send_buffer_and_destroy(socket_marta,result_map_buff);
	}

	pthread_mutex_unlock(&conex_marta_ready);

	free(ip_nodo);
	free_map_dest(map_dest);
}

//---------------------------------------------------------------------------
int enviar_infoMap_job(int socket_job,t_map_dest* map_dest){
	int result = 1;
	t_buffer* map_to_Nodo_buff;
    char *map_script = conf->path_map;
    char *path_result_map = conf->path_result_file;
    char* path_map = conf->paths_to_apply_files;
	map_to_Nodo_buff = buffer_create_with_protocol(ORDER_MAP);

	buffer_add_int(map_to_Nodo_buff, map_dest->ip_nodo);
	buffer_add_int(map_to_Nodo_buff, map_dest->id_nodo);
	buffer_add_int(map_to_Nodo_buff, map_dest->block);
	buffer_add_string(map_to_Nodo_buff, map_dest->temp_file_name);
	buffer_add_string(map_to_Nodo_buff, map_script);
	buffer_add_string(map_to_Nodo_buff, path_map);
	buffer_add_string(map_to_Nodo_buff, path_result_map);

	send_buffer_and_destroy(socket_job, map_to_Nodo_buff);

	return result;
}

//--------------------------------------------------------------------------
void hilo_reduce_job(t_reduce_dest* reduce_dest){

}

//--------------------------------------------------------------------------
void esperar_hilo_reduce(t_hilo_reduce* hilo_reduce) {

	void* ret_recep;

	if (pthread_join(*(hilo_reduce->thr), &ret_recep) != 0) {
		log_error(paranoid_log,"Error al hacer join del hilo Map ID: %i\n",hilo_reduce->id_reduce);
	}
}

//--------------------------------------------------------------------------
int enviar_infoReduce_job(int socket_job,t_reduce_dest* map_dest){
	return 0;
}

//---------------------------------------------------------------------------
void esperar_instrucciones_de_MaRTA() {

	uint32_t prot;
	int finished = 0, error = 0;
	t_hilo_map* hilo_map;
	t_map_dest* map_dest;
	t_reduce_dest* reduce_dest;
	t_list* hilos_map = list_create();
	//char *pathMapFile = conf->path_map;

	log_debug(paranoid_log, "Me Pongo a disposición de Ordenes de MaRTA");

	while ((!finished) && (!error)) {
		uint32_t id_reduce;
		char* path_temp_result;
		uint32_t nodes_amount = 0;
		int result = 1;
		uint32_t id_nodo_host;

		prot = receive_protocol_in_order(socket_marta);

		switch (prot) {

		case ORDER_MAP:
			map_dest = malloc(sizeof(t_map_dest));

			receive_int_in_order(socket_marta, &(map_dest->id_map));
			receive_int_in_order(socket_marta, &(map_dest->id_nodo));
			receive_int_in_order(socket_marta, &(map_dest->ip_nodo));
			receive_int_in_order(socket_marta, &(map_dest->puerto_nodo));
			receive_int_in_order(socket_marta, &(map_dest->block));
			receive_dinamic_array_in_order(socket_marta, (void **) &(map_dest->temp_file_name));


			hilo_map = malloc(sizeof(t_hilo_map));
			hilo_map->thr = malloc(sizeof(pthread_t));
			hilo_map->id_map = map_dest->id_map;

			list_add(hilos_map,hilo_map);

			log_info(paranoid_log, "Creación de Hilo de Map - Job");
			if (pthread_create(hilo_map->thr, NULL, (void *) hilo_map_job, map_dest) != 0) {
				log_error(paranoid_log, "No se pudo crear Hilo de Map ID: %i",hilo_map->id_map);
				exit(-1);
			}
			break;

		case ORDER_REDUCE:

			//abrir hilo de reduce
			reduce_dest = malloc(sizeof(t_reduce_dest));
			free(reduce_dest);
			//Falta desarrollar instrucciones de reduce.

			//XXX abrir hilo de reduce
			result = (result > 0)? receive_int_in_order(socket_marta, &id_reduce) : result;
			result = (result > 0)? receive_dinamic_array_in_order(socket_marta,(void **) &path_temp_result) : result;
			result = (result > 0)? receive_int_in_order(socket_marta, &id_nodo_host) : result;
			result = (result > 0)? receive_int_in_order(socket_marta, &nodes_amount) : result;

			if(result > 0) {
				log_info(paranoid_log, "Realizando Operación de Reduce ID: %i, Result: %s, ID Nodo Host: %i, Cantidad Nodos: %i",
						id_reduce, path_temp_result, id_nodo_host, nodes_amount);
			}
			int i,j;
			free(path_temp_result);

			for(i=0; (i<nodes_amount) && (result > 0); i++) {

				uint32_t id_nodo;
				uint32_t ip_nodo;
				uint32_t puerto_nodo;
				uint32_t amount_files_in_node = 0;

				result = (result > 0)? receive_int_in_order(socket_marta, &id_nodo) : result;
				result = (result > 0)? receive_int_in_order(socket_marta, &ip_nodo) : result;
				result = (result > 0)? receive_int_in_order(socket_marta, &puerto_nodo) : result;
				result = (result > 0)? receive_int_in_order(socket_marta, &amount_files_in_node) : result;

				if(result > 0) {
					char* ip = from_int_to_inet_addr(ip_nodo);

					log_info(paranoid_log, "Nodo: ID: %i, IP: %s, Puerto: %i, Cantidad Temporales: %i", id_nodo, ip, puerto_nodo,
							amount_files_in_node);
					free(ip);
				}

				for(j=0; (j< amount_files_in_node)  && (result > 0); j++) {
					uint32_t id_temp;
					char* path;

					result = (result > 0)? receive_int_in_order(socket_marta, &id_temp) : result;
					result = (result > 0)? receive_dinamic_array_in_order(socket_marta,(void **) &path) : result;

					if(result > 0) {
						log_info(paranoid_log, "Temp ID: %i, Path: %s", id_temp,path);
					}
					free(path);
				}
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
			log_error(paranoid_log, "No se pudo recivir instrucciones de MaRTA");
			exit(-1);
			break;

		default:
			log_error(paranoid_log, "Instruccion de MaRTA Desconocida");
			error = 1;
			break;
		}
	}

	list_iterate(hilos_map, (void *) esperar_hilo_map);
	list_destroy_and_destroy_elements(hilos_map, (void *) free_hilo_map);
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

//------------------------------------------------------------------------------

	/*log_debug(paranoid_log, "Solicitando conexión con nodo...");
		//int socketfd_Nodo = solicitarConexionCon((char*)map_dest.ip_nodo,(int) map_dest.puerto_nodo);
		int socketfd_Nodo = solicitarConexionCon("127.0.0.1",3500);


		if (socketfd_Nodo != -1) {
			log_info(paranoid_log, "Conexión con nodo establecida IP: %s, Puerto: %i", map_dest.ip_nodo, map_dest.puerto_nodo);
		} else {
			log_error(paranoid_log, "Conexión con nodo FALLIDA!!! IP: %s, Puerto: %i", map_dest.ip_nodo, map_dest.puerto_nodo);
			exit(-1);
		}

		int result = 0;
		t_buffer* new_job_buff;

		log_debug(paranoid_log, "Enviando Target del Job");

		result = send_protocol_in_order(socketfd_Nodo, NUEVO_JOB);

			if (result == -1) {
				log_error(paranoid_log, "No se pudo enviar Target del Job");
				exit(-1);
			} else {


					t_buffer* info_nodo_buffer = buffer_create_with_protocol(ORDER_MAP);

					buffer_add_int(info_nodo_buffer,map_dest.block);
					buffer_add_int(info_nodo_buffer,map_dest.id_nodo);
					buffer_add_string(info_nodo_buffer,map_dest.temp_file_name);
					send_buffer_and_destroy(socketfd_Nodo,info_nodo_buffer);
			}




	return 1;
}
*/

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

	//conf_job conf; // estructura que contiene la info del arch de conf
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
