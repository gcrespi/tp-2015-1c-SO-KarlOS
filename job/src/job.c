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

struct conf_job conf; // estructura que contiene la info del arch de conf

void levantar_arch_conf(); // devuelve una estructura con toda la info del archivo de configuracion "job.cfg"
void free_conf_job();


int main(void) {
	puts("!!!Hello World!!!"); /* prints !!!Hello World!!! */

	levantar_arch_conf();

	struct sockaddr_in socketaddr_MaRTA;
	setSocketAddrStd(&socketaddr_MaRTA, conf.ip_MaRTA, conf.port_MaRTA);


//	solicitarConexionConMaRTA(&socketaddr_fs,&info_envio);

	free_conf_job();

//	printf("%s, %i, %s, %s, %i, %s, %s",conf.ip_MaRTA,conf.puerto_MaRTA,conf.path_map,conf.path_reduce,conf.combiner,conf.paths_to_apply_files, conf.path_result_file);

	return EXIT_SUCCESS;
}


//---------------------------------------------------------------------------
void levantar_arch_conf(){
	t_config* conf_arch;
	conf_arch = config_create("job.cfg");
	if (config_has_property(conf_arch,"IP_MARTA")){
		conf.ip_MaRTA = strdup(config_get_string_value(conf_arch,"IP_MARTA"));
	} else printf("Error: el archivo de conf no tiene IP_MARTA\n");
	if (config_has_property(conf_arch,"PUERTO_MARTA")){
		conf.port_MaRTA = config_get_int_value(conf_arch,"PUERTO_MARTA");
	} else printf("Error: el archivo de conf no tiene PUERTO_MARTA\n");
	if (config_has_property(conf_arch,"MAPPER")){
		conf.path_map= strdup(config_get_string_value(conf_arch,"MAPPER"));
	} else printf("Error: el archivo de conf no tiene MAPPER\n");
	if (config_has_property(conf_arch,"REDUCE")){
		conf.path_reduce = strdup(config_get_string_value(conf_arch,"REDUCE"));
	} else printf("Error: el archivo de conf no tiene REDUCE\n");
	if (config_has_property(conf_arch,"COMBINER")){
		conf.combiner = config_get_int_value(conf_arch,"COMBINER");
	} else printf("Error: el archivo de conf no tiene COMBINER\n");
	if (config_has_property(conf_arch,"ARCHIVOS")){
		conf.paths_to_apply_files = strdup(config_get_string_value(conf_arch,"ARCHIVOS"));
	} else printf("Error: el archivo de conf no tiene ARCHIVOS\n");
	if (config_has_property(conf_arch,"RESULTADO")){
		conf.path_result_file = strdup(config_get_string_value(conf_arch,"RESULTADO"));
	} else printf("Error: el archivo de conf no tiene RESULTADO\n");
	config_destroy(conf_arch);
}


//---------------------------------------------------------------------------
void free_conf_job(){
	free(conf.ip_MaRTA);
	free(conf.path_map);
	free(conf.path_reduce);
	free(conf.paths_to_apply_files);
	free(conf.path_result_file);
}
