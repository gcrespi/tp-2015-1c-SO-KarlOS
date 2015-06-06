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

typedef struct {
	char* ip_MaRTA;
	int port_MaRTA;

	char* path_map;
	char* path_reduce;

	int combiner;
	char* paths_to_apply_files;
	char* path_result_file;
}conf_job;

typedef struct {
	uint32_t combiner;
	char* paths_to_apply_files;
	char* path_result_file;
}info_new_job;

typedef struct {
	uint32_t id_nodo;
	uint32_t ip_nodo;
	uint32_t puerto_nodo;
	uint32_t block;
} t_map_dest;

void levantar_arch_conf_job(conf_job* conf); // devuelve una estructura con toda la info del archivo de configuracion "job.cfg"
void free_conf_job(conf_job* conf);
int enviar_nuevo_job_a_MaRTA(int socket, info_new_job info_job);
void esperar_instrucciones_de_MaRTA(int socket);
void free_info_job(info_new_job* info);
void set_new_job(conf_job conf, info_new_job* new_job);

t_log* paranoid_log;

//-------------------------------------------------------------------------------------
int solicitarConexionConMarta(conf_job conf) {

	log_debug(paranoid_log, "Solicitando conexión con MaRTA...");
	int socketfd_MaRTA = solicitarConexionCon(conf.ip_MaRTA, conf.port_MaRTA);

	if (socketfd_MaRTA != -1) {
		log_info(paranoid_log, "Conexión con MaRTA establecida IP: %s, Puerto: %i", conf.ip_MaRTA, conf.port_MaRTA);
	} else {
		log_error(paranoid_log, "Conexión con MaRTA FALLIDA!!! IP: %s, Puerto: %i", conf.ip_MaRTA, conf.port_MaRTA);
		exit(-1);
	}

	return socketfd_MaRTA;
}


//---------------------------------------------------------------------------
void set_new_job(conf_job conf, info_new_job* new_job) {
	new_job->combiner = conf.combiner;
	new_job->path_result_file = strdup(conf.path_result_file);
	new_job->paths_to_apply_files = strdup(conf.paths_to_apply_files);
}

//---------------------------------------------------------------------------
void esperar_instrucciones_de_MaRTA(int socket) {

	uint32_t prot;
	int finished = 0, error = 0;
	t_map_dest map_dest;

	log_debug(paranoid_log, "Me Pongo a disposición de Ordenes de MaRTA");

	while ((!finished) && (!error)) {

		prot = receive_protocol_in_order(socket);

		switch (prot) {

		case ORDER_MAP:
			//TODO abrir hilo de map

			receive_int_in_order(socket,&(map_dest.id_nodo));
			receive_int_in_order(socket,&(map_dest.ip_nodo));
			receive_int_in_order(socket,&(map_dest.puerto_nodo));
			receive_int_in_order(socket,&(map_dest.block));

			log_info(paranoid_log,"Realizando Operación de Map en Nodo: %i, IP: %i, Port: %i, Block: %i",map_dest.id_nodo,map_dest.ip_nodo,map_dest.puerto_nodo,map_dest.block);

			send_protocol_in_order(socket,MAP_OK);
			break;

		case ORDER_REDUCE:
			//abrir hilo de reduce
			break;

		case FINISHED_JOB:
			log_info(paranoid_log,"Se Terminó Exitosamente el Job");
			finished = 1;
			break;

		case ABORTED_JOB:
			log_error(paranoid_log,"Se Produjo un Error en MaRTA y se Abortó el Job");
			error = 1;
			break;

		case DISCONNECTED:
			log_error(paranoid_log,"MaRTA se desconectó de forma inesperada");
			error = 2;
			break;

		case -1:
			log_error(paranoid_log,"No se pudo recivir instrucciones de MaRTA");
			exit(-1);
			break;

		default:
			log_error(paranoid_log,"Instruccion de MaRTA Desconocida");
			error = 1;
			break;
		}
	}
}

//---------------------------------------------------------------------------
int enviar_nuevo_job_a_MaRTA(int socket, info_new_job info_job) {

	int result = 0;
	t_buffer* new_job_buff;

	log_debug(paranoid_log, "Enviando Target del Job");

	new_job_buff = buffer_create_with_protocol(NUEVO_JOB);

	buffer_add_int(new_job_buff, info_job.combiner);
	buffer_add_string(new_job_buff, info_job.paths_to_apply_files);
	buffer_add_string(new_job_buff, info_job.path_result_file);

	result = send_buffer_and_destroy(socket,new_job_buff);

	if (result == -1) {
		log_error(paranoid_log, "No se pudo enviar Target del Job");
		exit(-1);
	}

	return result;
}

//---------------------------------------------------------------------------
void levantar_arch_conf_job(conf_job* conf) {

	char** properties = string_split("IP_MARTA,PUERTO_MARTA,MAPPER,REDUCE,COMBINER,ARCHIVOS,RESULTADO", ",");
	t_config* conf_arch = config_create("job.cfg");

	if(has_all_properties(7, properties, conf_arch)) {
		conf->ip_MaRTA = strdup(config_get_string_value(conf_arch, properties[0]));
		conf->port_MaRTA = config_get_int_value(conf_arch, properties[1]);
		conf->path_map = strdup(config_get_string_value(conf_arch, properties[2]));
		conf->path_reduce = strdup(config_get_string_value(conf_arch, properties[3]));
		conf->combiner = config_get_int_value(conf_arch, properties[4]);
		conf->paths_to_apply_files = strdup(config_get_string_value(conf_arch, properties[5]));
		conf->path_result_file = strdup(config_get_string_value(conf_arch, properties[6]));
	} else {
		log_error(paranoid_log,"Faltan propiedades en archivo de Configuración");
		exit(-1);
	}
	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------
void free_info_job(info_new_job* info) {
	free(info->path_result_file);
	free(info->paths_to_apply_files);
}

//---------------------------------------------------------------------------
void free_conf_job(conf_job* conf) {
	free(conf->ip_MaRTA);
	free(conf->path_map);
	free(conf->path_reduce);
	free(conf->paths_to_apply_files);
	free(conf->path_result_file);
}

//###########################################################################
int main(void) {

	conf_job conf; // estructura que contiene la info del arch de conf
	levantar_arch_conf_job(&conf);
	paranoid_log = log_create("./logJob.log", "JOB", 1, LOG_LEVEL_TRACE);

	int socketfd_MaRTA = solicitarConexionConMarta(conf);

	info_new_job new_job;
	set_new_job(conf, &new_job);

	enviar_nuevo_job_a_MaRTA(socketfd_MaRTA, new_job);

	esperar_instrucciones_de_MaRTA(socketfd_MaRTA);

	free_conf_job(&conf);
	free_info_job(&new_job);

	log_destroy(paranoid_log);

	return EXIT_SUCCESS;
}
