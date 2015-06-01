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
#include "../../connectionlib/connectionlib.h"

#define LISTEN_JOB_PORT 5555


struct conf_MaRTA {
	int puerto_listen;
	char* ip_fs;
	int puerto_fs;
};

//Prototipos
void levantar_arch_conf_marta(struct conf_MaRTA* conf);

//Variables Globales
struct conf_MaRTA conf; //Configuracion del fs

struct t_job {
	int sockfd;
	pthread_t* thr;
	struct sockaddr_in socketaddr_cli;
};

pthread_mutex_t mutex_end;
int cerrar_marta = 0;
t_log* paranoid_log;

//---------------------------------------------------------------------------
void levantar_arch_conf_marta(struct conf_MaRTA* conf) {

	char** properties = string_split("PUERTO_LISTEN,IP_FS,PUERTO_FS", ",");
	t_config* conf_arch = config_create("marta.cfg");

	if(has_all_properties(3, properties, conf_arch)) {
		conf->puerto_listen = config_get_int_value(conf_arch, properties[0]);

		conf->ip_fs = strdup(config_get_string_value(conf_arch, properties[1]));
		conf->puerto_fs = config_get_int_value(conf_arch, properties[2]);
	} else {
		log_error(paranoid_log,"Faltan propiedades en archivo de Configuración");
		exit(-1);
	}
	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------
void terminar_hilos() {

	pthread_mutex_lock(&mutex_end);
	cerrar_marta = 1;
	pthread_mutex_unlock(&mutex_end);

}

//---------------------------------------------------------------------------
int programa_terminado() {
	int endLocal;

	pthread_mutex_lock(&mutex_end);
	endLocal = cerrar_marta;
	pthread_mutex_unlock(&mutex_end);

	return endLocal;
}



//-------------------------------------------------------------------
void hilo_conex_job(struct t_job *job)
{
	log_info(paranoid_log,"Comienzo Hilo Job");

	recibir_protocolo(job->sockfd);

	enviar_protocolo(job->sockfd,FINISHED_JOB);

	sleep(10);

	terminar_hilos();

	log_info(paranoid_log,"Termina Hilo Job");
}

//-------------------------------------------------------------------
void liberar_job(struct t_job* job)
{
	free(job->thr);
	free(job);
}

//---------------------------------------------------------------------------
void crear_hilo_para_un_job(struct t_job* job) {

	log_info(paranoid_log,"Creación de Hilo Job");
	if (pthread_create(job->thr, NULL, (void*) hilo_conex_job, job) != 0) {
		log_error(paranoid_log, "No se pudo crear Hilo Job");
		exit(-1);
	}
}

//---------------------------------------------------------------------------
void esperar_finalizacion_hilo_conex_job(struct t_job* job)
{
	void* ret_recep;

	if (pthread_join(*job->thr, &ret_recep) != 0) {
		printf("Error al hacer join del hilo\n");
	}
}

//###########################################################################
int main(void) {

	struct conf_MaRTA conf;
	levantar_arch_conf_marta(&conf); //Levanta el archivo de configuracion "marta.cfg"

	pthread_mutex_init(&mutex_end,NULL);

	paranoid_log = log_create("./logMaRTA.log", "MaRTA", 1, LOG_LEVEL_TRACE);

	int listener_jobs;

	t_list* lista_jobs;
	struct t_job* job;

	lista_jobs = list_create();

	log_debug(paranoid_log, "Obteniendo Puerto para Escuchar Jobs...");
	if((listener_jobs = escucharConexionesDesde("",LISTEN_JOB_PORT)) == -1) {
		log_error(paranoid_log,"No se pudo obtener Puerto para Escuchar Jobs");
		exit(-1);
	} else {
		log_debug(paranoid_log, "Puerto para Escuchar Jobs Obtenido");
	}

	while(!programa_terminado())
	{
		job = malloc(sizeof(struct t_job));
		job->thr = malloc(sizeof(pthread_t));

		list_add(lista_jobs,job);

		if((job->sockfd = aceptarCliente(listener_jobs,&(job->socketaddr_cli))) == -1) {
			log_error(paranoid_log,"Error al Aceptar Nuevos Jobs");
			exit(-1);
		}

		crear_hilo_para_un_job(job);
	}

	log_info(paranoid_log,"Esperando finalizacion de hilos de Job");
	list_iterate(lista_jobs,(void *)esperar_finalizacion_hilo_conex_job);

	list_destroy_and_destroy_elements(lista_jobs,(void *)liberar_job);

	free(lista_jobs);
	pthread_mutex_destroy(&mutex_end);

	log_destroy(paranoid_log);

	return EXIT_SUCCESS;
}
