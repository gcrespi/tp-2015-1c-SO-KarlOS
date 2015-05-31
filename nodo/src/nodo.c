/*
 ============================================================================
 Name        : nodo.c
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



// Estructuras
   // La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	int id;
	int nodo_nuevo;
	int cant_bloques;
};

   // Una estructura que contiene todos los datos del arch de conf
struct conf_nodo {
	int id;
	char* ip_fs;
	int puerto_fs;
	char* archivo_bin;
	char* dir_temp;
	int nodo_nuevo;
	char* ip_nodo;
	int puerto_nodo;
	int cant_bloques;
};

//Variables Globales
struct conf_nodo conf; // estructura que contiene la info del arch de conf
int socketfd_fs; // file descriptor del FS


//Prototipos
<<<<<<< HEAD
void levantar_arch_conf_nodo(); // devuelve una estructura con toda la info del archivo de configuracion "nodo.cfg"
void setSocketAddr(struct sockaddr_in*); // setea el socketaddr segun la info del nodo
=======
void levantar_arch_conf(); // devuelve una estructura con toda la info del archivo de configuracion "nodo.cfg"
>>>>>>> branch 'master' of https://github.com/sisoputnfrba/tp-2015-1c-karlos
void setNodoToSend(struct info_nodo *); // setea la estructura que va a ser enviada al fs al iniciar el nodo
void solicitarConexionConFS(struct sockaddr_in*, struct info_nodo*); //conecta con el FS
int enviar_info_nodo (int, struct info_nodo*);
void free_conf_nodo();

//Main
int main(void) {
	levantar_arch_conf_nodo();

	struct info_nodo info_envio;
	setNodoToSend(&info_envio);

	struct sockaddr_in socketaddr_fs;
	setSocketAddrStd(&socketaddr_fs,conf.ip_fs,conf.puerto_fs);

	solicitarConexionConFS(&socketaddr_fs,&info_envio);

	free_conf_nodo();

	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
void levantar_arch_conf_nodo(){
	t_config* conf_arch;
	conf_arch = config_create("nodo.cfg");
	if (config_has_property(conf_arch,"ID")){
			conf.id = config_get_int_value(conf_arch,"ID");
		} else printf("Error: el archivo de conf no tiene IP_FS\n");
	if (config_has_property(conf_arch,"IP_FS")){
		conf.ip_fs = strdup(config_get_string_value(conf_arch,"IP_FS"));
	} else printf("Error: el archivo de conf no tiene IP_FS\n");
	if (config_has_property(conf_arch,"PUERTO_FS")){
		conf.puerto_fs = config_get_int_value(conf_arch,"PUERTO_FS");
	} else printf("Error: el archivo de conf no tiene PUERTO_FS\n");
	if (config_has_property(conf_arch,"ARCHIVO_BIN")){
		conf.archivo_bin= strdup(config_get_string_value(conf_arch,"ARCHIVO_BIN"));
	} else printf("Error: el archivo de conf no tiene ARCHIVO_BIN\n");
	if (config_has_property(conf_arch,"DIR_TEMP")){
		conf.dir_temp = strdup(config_get_string_value(conf_arch,"DIR_TEMP"));
	} else printf("Error: el archivo de conf no tiene DIR_TEMP\n");
	if (config_has_property(conf_arch,"NODO_NUEVO")){
		conf.nodo_nuevo = config_get_int_value(conf_arch,"NODO_NUEVO");
	} else printf("Error: el archivo de conf no tiene NODO_NUEVO\n");
	if (config_has_property(conf_arch,"IP_NODO")){
		conf.ip_nodo = strdup(config_get_string_value(conf_arch,"IP_NODO"));
	} else printf("Error: el archivo de conf no tiene IP_NODO\n");
	if (config_has_property(conf_arch,"PUERTO_NODO")){
		conf.puerto_nodo = config_get_int_value(conf_arch,"PUERTO_NODO");
	} else printf("Error: el archivo de conf no tiene PUERTO_NODO\n");
	if (config_has_property(conf_arch,"CANT_BLOQUES")){
		conf.cant_bloques = config_get_int_value(conf_arch,"CANT_BLOQUES");
	} else printf("Error: el archivo de conf no tiene CANT_BLOQUES\n");
	config_destroy(conf_arch);
}

//---------------------------------------------------------------------------
void setNodoToSend(struct info_nodo *info_envio){
	info_envio->id = conf.id;
	info_envio->nodo_nuevo = conf.nodo_nuevo;
	info_envio->cant_bloques = conf.cant_bloques;
}

//---------------------------------------------------------------------------
void solicitarConexionConFS(struct sockaddr_in *direccionDestino, struct info_nodo *info_envio) {

	if ((socketfd_fs = socket(AF_INET, SOCK_STREAM, 0)) == -1) { // crea un Socket
		perror("Error while socket()");
		exit(-1);
	}

	if (connect(socketfd_fs, (struct sockaddr*) direccionDestino,
			sizeof(struct sockaddr)) == -1) { // conecta con el servidor
		perror("Error while connect()");
		exit(-1);
	}

	if (enviar_info_nodo(socketfd_fs, info_envio) == -1) { // envia la estructura al FS
		perror("Error while send()");
		exit(-1);
	}

}

//---------------------------------------------------------------------------
int enviar_info_nodo (int socket, struct info_nodo *info_nodo){

	int result=0;
	uint32_t prot = INFO_NODO;
	if ((result += send(socket, &prot, sizeof(uint32_t), 0)) == -1) { //envia el protocolo
		perror("Error sending");
		exit(-1);
	}
	if ((result += enviar(socket, &(info_nodo->id), sizeof(info_nodo->id))) == -1) { //envia el segundo campo
		perror("Error sending");
		exit(-1);
	}
	if ((result += enviar(socket, &(info_nodo->cant_bloques), sizeof(info_nodo->cant_bloques))) == -1) { //envia el primer campo
		perror("Error sending");
		exit(-1);
	}
	if ((result += enviar(socket, &(info_nodo->nodo_nuevo), sizeof(info_nodo->nodo_nuevo))) == -1) { //envia el segundo campo
		perror("Error sending");
		exit(-1);
	}
	return result;
}

//---------------------------------------------------------------------------
void free_conf_nodo(){
	free(conf.ip_fs);
	free(conf.archivo_bin);
	free(conf.dir_temp);
	free(conf.ip_nodo);
}

//---------------------------------------------------------------------------
