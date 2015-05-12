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
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// Estructuras
   // La estructura que envia el nodo al FS al iniciarse
struct nodo_to_send {
	int nodo_nuevo;
	int cant_bloques;
	char* saludo; // TODO eliminar esta linea
};

   // Una estructura que contiene todos los datos del arch de conf
struct info_nodo {
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
struct info_nodo *conf; // estructura que contiene la info del arch de conf
int socketfd_fs; // file descriptor del FS


//Prototipos
void levantar_arch_conf(); // devuelve una estructura con toda la info del archivo de configuracion "nodo.cfg"
void setSocketAddr(struct sockaddr_in*); // setea el socketaddr segun la info del nodo
void setNodoToSend(struct nodo_to_send *); // setea la estructura que va a ser enviada al fs al iniciar el nodo
void solicitarConexionConFS(struct sockaddr_in*, struct nodo_to_send*); //conecta con el FS

//Main
int main(void) {
	struct nodo_to_send *info_envio;
	levantar_arch_conf();
	setNodoToSend(info_envio);

	struct sockaddr_in *socketaddr_fs;
	setSocketAddr(socketaddr_fs);
	solicitarConexionConFS(socketaddr_fs,info_envio);

	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
void levantar_arch_conf(){
	t_config* conf_arch;
	conf_arch = config_create("nodo.cfg");
	if (config_has_property(conf_arch,"IP_FS")){
		conf->ip_fs = config_get_string_value(conf_arch,"IP_FS");
	} else printf("Error: el archivo de conf no tiene IP_FS\n");
	if (config_has_property(conf_arch,"PUERTO_FS")){
		conf->puerto_fs = config_get_int_value(conf_arch,"PUERTO_FS");
	} else printf("Error: el archivo de conf no tiene PUERTO_FS\n");
	if (config_has_property(conf_arch,"ARCHIVO_BIN")){
		conf->archivo_bin = config_get_string_value(conf_arch,"ARCHIVO_BIN");
	} else printf("Error: el archivo de conf no tiene ARCHIVO_BIN\n");
	if (config_has_property(conf_arch,"DIR_TEMP")){
		conf->dir_temp = config_get_string_value(conf_arch,"DIR_TEMP");
	} else printf("Error: el archivo de conf no tiene DIR_TEMP\n");
	if (config_has_property(conf_arch,"IP_NODO")){
		conf->ip_nodo = config_get_string_value(conf_arch,"IP_NODO");
	} else printf("Error: el archivo de conf no tiene IP_NODO\n");
	if (config_has_property(conf_arch,"PUERTO_NODO")){
		conf->puerto_nodo = config_get_int_value(conf_arch,"PUERTO_NODO");
	} else printf("Error: el archivo de conf no tiene PUERTO_NODO\n");
	if (config_has_property(conf_arch,"CANT_BLOQUES")){
		conf->cant_bloques = config_get_int_value(conf_arch,"CANT_BLOQUES");
	} else printf("Error: el archivo de conf no tiene CANT_BLOQUES\n");
	config_destroy(conf_arch);
}

//---------------------------------------------------------------------------
void setSocketAddr(struct sockaddr_in* direccionDestino) {
	direccionDestino->sin_family = AF_INET; // familia de direcciones (siempre AF_INET)
	direccionDestino->sin_port = htons(conf->puerto_fs); // setea Puerto a conectarme
	direccionDestino->sin_addr.s_addr = inet_addr(conf->ip_fs); // Setea Ip a conectarme
	memset(&(direccionDestino->sin_zero), '\0', 8); // pone en ceros los bits que sobran de la estructura
}

//---------------------------------------------------------------------------
void setNodoToSend(struct nodo_to_send *info_envio){
	info_envio->nodo_nuevo = conf->nodo_nuevo;
	info_envio->cant_bloques = conf->cant_bloques;
	info_envio->saludo = "TODO SE MANDO CORRECTO"; // TODO eliminar esta linea
}

//---------------------------------------------------------------------------
void solicitarConexionConFS(struct sockaddr_in *direccionDestino, struct nodo_to_send *info_envio) {

	if ((socketfd_fs = socket(AF_INET, SOCK_STREAM, 0)) == -1) { // crea un Socket
		perror("Error while socket()");
		exit(-1);
	}

	if (connect(socketfd_fs, (struct sockaddr*) direccionDestino,
			sizeof(struct sockaddr)) == -1) { // conecta con el servidor
		perror("Error while connect()");
		exit(-1);
	}

	if (send(socketfd_fs, info_envio, sizeof(struct nodo_to_send), 0) == -1) { // envia la estructura al FS
		printf("Error while send()\n");
	}

}

//---------------------------------------------------------------------------

