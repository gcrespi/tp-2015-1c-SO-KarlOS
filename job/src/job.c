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

struct conf_job {
	char* ip_MaRTA;
	int port_MaRTA;

	char* path_map;
	char* path_reduce;

	int combiner;
	char* paths_to_apply_files;
	char* path_result_file;
};

struct info_new_job {
	uint32_t combiner;
	char* paths_to_apply_files;
	char* path_result_file;
};

void levantar_arch_conf_job(struct conf_job* conf); // devuelve una estructura con toda la info del archivo de configuracion "job.cfg"
void free_conf_job(struct conf_job* conf);
int enviar_nuevo_job_a_MaRTA(int socket, struct info_new_job info_job);
void esperar_instrucciones_de_MaRTA(int socket);
void free_info_job(struct info_new_job* info);
void set_new_job(struct conf_job conf, struct info_new_job* new_job);

//-------------------------------------------------------------------------------------
int main(void) {

	struct conf_job conf; // estructura que contiene la info del arch de conf
	levantar_arch_conf_job(&conf);


	int socketfd_MaRTA = solicitarConexionCon(conf.ip_MaRTA,conf.port_MaRTA);

	struct info_new_job new_job;
	set_new_job(conf, &new_job);

	enviar_nuevo_job_a_MaRTA(socketfd_MaRTA, new_job);

	esperar_instrucciones_de_MaRTA(socketfd_MaRTA);

	free_conf_job(&conf);
	free_info_job(&new_job);

	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
void set_new_job(struct conf_job conf, struct info_new_job* new_job) {
	new_job->combiner = conf.combiner;
	new_job->path_result_file = strdup(conf.path_result_file);
	new_job->paths_to_apply_files = strdup(conf.paths_to_apply_files);
}

//---------------------------------------------------------------------------
void esperar_instrucciones_de_MaRTA(int socket) {

	enum protocolo prot;
	int finished = 0, error = 0;

	while ((!finished) && (!error)) {

		prot = recibir_protocolo(socket);

		switch (prot) {

		case ORDER_MAP:
			//abrir hilo de map
			break;

		case ORDER_REDUCE:
			//abrir hilo de reduce
			break;

		case FINISHED_JOB:
			//termino el job exitosamente
			finished = 1;
			break;

		case DISCONNECTED:
			//se desconectó marta de forma inesperada
			error = 2;
			break;

		default:
			//se produjo un error inesperado
			error = 1;
			break;
		}
	}
}

//---------------------------------------------------------------------------
int enviar_nuevo_job_a_MaRTA(int socket, struct info_new_job info_job) {

	int result = 0;

	enviar_protocolo(socket, NUEVO_JOB);

	enviar_int(socket, info_job.combiner);
	enviar_string(socket, info_job.paths_to_apply_files);
	enviar_string(socket, info_job.path_result_file);

	return result;
}


//---------------------------------------------------------------------------
void comprobar_existan_properties(int cant_properties, char** properties,
		t_config* conf_arch) {
	int i;

	for (i = 0;
			(i < cant_properties) && (config_has_property(conf_arch, properties[i]));
			i++)
		;

	if(i<cant_properties)
	{
		for (i = 0; i < cant_properties; i++) {
			if (!config_has_property(conf_arch, properties[i])) {
				printf("Error: el archivo de conf no tiene %s\n",
						properties[i]);
			}
		}
		exit(-1);
	}
}

//---------------------------------------------------------------------------
void free_string_splits(char** strings) {
	char **aux = strings;

	while (*aux != NULL) {
		free(*aux);
		aux++;
	}
	free(strings);
}



//---------------------------------------------------------------------------
void levantar_arch_conf_job(struct conf_job* conf) {

	char** properties = string_split("IP_MARTA,PUERTO_MARTA,MAPPER,REDUCE,COMBINER,ARCHIVOS,RESULTADO",",");
	t_config* conf_arch = config_create("job.cfg");

	comprobar_existan_properties(7,properties,conf_arch);

	conf->ip_MaRTA = strdup(
			config_get_string_value(conf_arch, properties[0]));
	conf->port_MaRTA = config_get_int_value(conf_arch, properties[1]);
	conf->path_map = strdup(
			config_get_string_value(conf_arch, properties[2]));
	conf->path_reduce = strdup(
			config_get_string_value(conf_arch, properties[3]));
	conf->combiner = config_get_int_value(conf_arch, properties[4]);
	conf->paths_to_apply_files = strdup(
			config_get_string_value(conf_arch, properties[5]));
	conf->path_result_file = strdup(
			config_get_string_value(conf_arch, properties[6]));

	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------
void free_info_job(struct info_new_job* info) {
	free(info->path_result_file);
	free(info->paths_to_apply_files);
}

//---------------------------------------------------------------------------
void free_conf_job(struct conf_job* conf) {
	free(conf->ip_MaRTA);
	free(conf->path_map);
	free(conf->path_reduce);
	free(conf->paths_to_apply_files);
	free(conf->path_result_file);
}
