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
#include <stdint.h> //Esta la agregeue para poder definir int con tamaño especifico


// Estructuras
   // La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	int nodo_nuevo;
	int cant_bloques;
	char* saludo; // TODO eliminar esta linea
};

   // Una estructura que contiene todos los datos del arch de conf
struct conf_nodo {
	char* ip_fs;
	int puerto_fs;
	char* archivo_bin;
	char* dir_temp;
	int nodo_nuevo;
	char* ip_nodo;
	int puerto_nodo;
	int cant_bloques;
};

	//Enum del protocolo
enum protocolo {INFO_NODO};

//Variables Globales
struct conf_nodo *conf; // estructura que contiene la info del arch de conf
int socketfd_fs; // file descriptor del FS


//Prototipos
void levantar_arch_conf(); // devuelve una estructura con toda la info del archivo de configuracion "nodo.cfg"
void setSocketAddr(struct sockaddr_in*); // setea el socketaddr segun la info del nodo
void setNodoToSend(struct info_nodo *); // setea la estructura que va a ser enviada al fs al iniciar el nodo
void solicitarConexionConFS(struct sockaddr_in*, struct info_nodo*); //conecta con el FS
int enviar_info_nodo (int, struct info_nodo*);
int enviar(int , void*);

//Main
int main(void) {
	levantar_arch_conf();

	struct info_nodo info_envio;
	setNodoToSend(&info_envio);

	struct sockaddr_in socketaddr_fs;
	setSocketAddr(&socketaddr_fs);

	solicitarConexionConFS(&socketaddr_fs,&info_envio);

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
void setNodoToSend(struct info_nodo *info_envio){
	info_envio->nodo_nuevo = conf->nodo_nuevo;
	info_envio->cant_bloques = conf->cant_bloques;
	info_envio->saludo = "TODO SE MANDO CORRECTO"; // TODO eliminar esta linea
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
		printf("Error while send()\n");
	}

}

//---------------------------------------------------------------------------
int enviar_info_nodo (int socket, struct info_nodo *info_nodo){

	int result=0;
	uint32_t prot = INFO_NODO;

	if ((result += send(socket, &prot, sizeof(uint32_t), 0)) == -1) { //envia el protocolo
		return -1;
	}

	if ((result += enviar(socket, &(info_nodo->cant_bloques))) == -1) { //envia el primer campo
		return -1;
	}

	if ((result += enviar(socket, &(info_nodo->nodo_nuevo))) == -1) { //envia el segundo campo
		return -1;
	}

	return result;
}

//---------------------------------------------------------------------------
int enviar(int socket, void *buffer) {
	int result=0;
	uint32_t size_buffer; //el tamaño del buffer como maximo va a ser de 4 gigas (32bits)
	size_buffer = sizeof(*buffer);
	if ((result += send(socket, &size_buffer, sizeof(uint32_t), 0)) == -1) {
		return -1;
	}
	if ((result += send(socket, buffer, size_buffer, 0)) == -1) {
		return -1;
	}
	return result;
}

//---------------------------------------------------------------------------

