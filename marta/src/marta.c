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

struct conf_MaRTA {
	int puerto_listen;
	char* ip_fs;
	int puerto_fs;
};

//Prototipos
void levantar_arch_conf_marta(struct conf_MaRTA* conf);
int recivir_bajo_protocolo(int);
void hilo_listener();

//Variables Globales
struct conf_MaRTA conf; //Configuracion del fs
char end; //Indicador de que deben terminar todos los hilos
t_list *list_info_nodo; //Lista de nodos que solicitan conectarse al FS
int listener;

int main(void) {                                         //TODO aca esta el main

	struct conf_MaRTA conf;
	levantar_arch_conf_marta(&conf); //Levanta el archivo de configuracion "marta.cfg"

//	pthread_t t_listener;
//	pthread_create(&t_listener, NULL, (void*) hilo_listener, NULL);
//
//	pthread_join(t_listener, NULL);

	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
void hilo_listener() {
//	list_info_nodo = list_create();
//
//	fd_set master; // Nuevo set principal
//	fd_set read_fds; // Set temporal para lectura
//	FD_ZERO(&master); // Vacio los sets
//	FD_ZERO(&read_fds);
//	int fd_max; // Va a ser el maximo de todos los descriptores de archivo del select
//	int i; // para los for
//	struct sockaddr_in sockaddr_listener, sockaddr_cli;
//	setSocketAddr(&sockaddr_listener);
//	listener = socket(AF_INET, SOCK_STREAM, 0);
//	int socketfd_cli;
//	int yes = 1;
//	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
//			== -1) {
//		perror("setsockopt");
//		exit(1);
//	}
//	if (bind(listener, (struct sockaddr*) &sockaddr_listener,
//			sizeof(sockaddr_listener)) == -1) {
//		perror("Error binding");
//		exit(-1);
//	}
//	if (listen(listener, 100) == -1) {
//		perror("Error listening");
//		exit(-1);
//	}
//
//	FD_SET(listener, &master);
//	FD_SET(STDIN_FILENO, &master);
//	fd_max = listener;
//	int sin_size = sizeof(struct sockaddr_in);
//
//	do {
//		read_fds = master; // Cada iteracion vuelvo a copiar del principal al temporal
//		select(fd_max + 1, &read_fds, NULL, NULL, NULL); // El select se encarga de poner en los temp los fds que recivieron algo
//		for (i = 0; i <= fd_max; i++) {
//			if (FD_ISSET(i, &read_fds)) {
//				if (i == listener) {
//					socketfd_cli = accept(listener,
//							(struct sockaddr*) &sockaddr_cli,
//							(socklen_t*) &sin_size);
//					recivir_bajo_protocolo(socketfd_cli);
//					FD_SET(socketfd_cli, &master);
//					if (socketfd_cli > fd_max)
//						fd_max = socketfd_cli;
//				}
//			}
//		}
//	} while (!end);
//
//	list_destroy_and_destroy_elements(list_info_nodo,
//			(void*) info_nodo_destroy);
}

//---------------------------------------------------------------------------
int recivir_bajo_protocolo(int socket) {
	uint32_t prot;
	if (recv(socket, &prot, sizeof(uint32_t), 0) == -1) {
		return -1;
	}
	switch (prot) {
	case INFO_NODO:
//		recivir_info_nodo(socket);
		break;
	default:
		return -1;
	}
	return prot;
}
////---------------------------------------------------------------------------
//void recivir_info_nodo(int socket) {
//
//	struct info_nodo* info_nodo;
//	info_nodo = malloc(sizeof(struct info_nodo));
//
//	if (recibir(socket, &(info_nodo->id)) == -1) { //recive id del nodo
//		perror("Error reciving id");
//		exit(-1);
//	}
//	if (recibir(socket, &(info_nodo->cant_bloques)) == -1) { //recive cantidad de bloques del nodo
//		perror("Error reciving cant_bloques");
//		exit(-1);
//	}
//	if (recibir(socket, &(info_nodo->nodo_nuevo)) == -1) { //recive si el nodo es nuevo o no
//		perror("Error reciving nodo_nuevo");
//		exit(-1);
//	}
//
//	list_add(list_info_nodo, info_nodo);
//
//}

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
void levantar_arch_conf_marta(struct conf_MaRTA* conf) {

	char** properties = string_split("PUERTO_LISTEN,IP_FS,PUERTO_FS",",");
	t_config* conf_arch = config_create("marta.cfg");

	comprobar_existan_properties(3, properties, conf_arch);

	conf->puerto_listen = config_get_int_value(conf_arch, properties[0]);

	conf->ip_fs = strdup(config_get_string_value(conf_arch, properties[1]));
	conf->puerto_fs = config_get_int_value(conf_arch, properties[2]);

	config_destroy(conf_arch);
	free_string_splits(properties);
}
