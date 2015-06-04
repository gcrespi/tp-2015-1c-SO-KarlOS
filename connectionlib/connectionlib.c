/*
 * connectionlib.c
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h> //Esta la agregeue para poder definir int con tamaño especifico (uint32_t)
#include <ifaddrs.h>
#include <string.h>
#include <commons/config.h>
#include "../connectionlib/connectionlib.h"

#define BACKLOG 5

//---------------------------------------------------------------------------
void mostrar_error(int number, char* cause) {
	perror(cause);
//	quizas debería distinguirse error insalvable de error ignorable
//	getchar();
}

//---------------------------------------------------------------------------
int solicitarConexionCon(char* server_ip, int server_port) {
	struct sockaddr_in socketaddr_server;

	setSocketAddrStd(&socketaddr_server, server_ip, server_port);

	int socketfd_server;

	if ((socketfd_server = socket(AF_INET, SOCK_STREAM, 0)) == -1) { // crea un Socket
		mostrar_error(-1, "Error while socket()");
		return -1;
	}

	if (connect(socketfd_server, (struct sockaddr*) &socketaddr_server, sizeof(struct sockaddr)) == -1) { // conecta con el servidor
		mostrar_error(-1, "Error while connect()");
		return -1;
	}
	return socketfd_server;
}

//---------------------------------------------------------------------------
int escucharConexionesDesde(char* server_ip, int server_port) {
	struct sockaddr_in socketaddr_server;

	setSocketAddrStd(&socketaddr_server, server_ip, server_port);

	int listener;

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) { //Crea un Socket
		mostrar_error(-1, "Error while socket()");
		return -1;
	}

	int yes = 1;
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
		mostrar_error(-1, "setsockopt");
		return -1;
	}

	if (bind(listener, (struct sockaddr*) &socketaddr_server, sizeof(struct sockaddr)) == -1) { //Asignar puerto de escucha
		mostrar_error(-1, "Error while bind()");
		return -1;
	}

	if (listen(listener, BACKLOG) == -1) { //escuchar por clientes
		mostrar_error(-1, "Error while listen()");
		return -1;
	}

	return listener;
}

//--------------------------------------------------------------------
int aceptarCliente(int listener, struct sockaddr_in* direccionCliente) {
	int sin_size = sizeof(struct sockaddr_in);
	int nuevo_socket;

	if ((nuevo_socket = accept(listener, (struct sockaddr*) direccionCliente, (socklen_t*) &sin_size)) == -1) { //Crea un Socket
		mostrar_error(-1, "Error while accept()");
		return -1;
	}

	return nuevo_socket;
}

//---------------------------------------------------------------------------
int enviar_protocolo(int socket, uint32_t protocolo) {
	uint32_t prot = protocolo;
	int result = 0;

	if ((result = send(socket, &prot, sizeof(uint32_t), 0)) == -1) { //envia el protocolo
		mostrar_error(-1, "Error sending");
		return -1;
	}

	return result;
}

//---------------------------------------------------------------------------
uint32_t recibir_protocolo(int socket) {
	uint32_t prot;

	int result;

	if ((result = recv(socket, &prot, sizeof(uint32_t), 0)) == -1) {
		mostrar_error(-1, "Error reciving");
		return -1;
	}

	if (result == 0) {
		return DISCONNECTED;
	}

	return prot;
}

//---------------------------------------------------------------------------
int enviar_int(int socket, uint32_t numero) {
	uint32_t aux_numero = numero;
	return enviar(socket, &aux_numero, sizeof(uint32_t));
}

//---------------------------------------------------------------------------
int enviar_string(int socket, char *string) {
	return enviar(socket, string, strlen(string) + 1);
}

//---------------------------------------------------------------------------
int enviar(int socket, void *buffer, uint32_t size_buffer) {

	if (send(socket, &size_buffer, sizeof(uint32_t), 0) == -1) {
		mostrar_error(-1, "Error sending");
		return -1;
	}

	//TODO TESTME
	uint32_t size_sended = 0;
	while (size_sended < size_buffer) {
		uint32_t sending;

		if ((sending = send(socket, (buffer + size_sended), (size_buffer - size_sended), 0)) == -1) {
			mostrar_error(-1, "Error sending");
			return -1;
		}

		size_sended += sending;
	}

	return 0;
}

////---------------------------------------------------------------------------
//int recibir(int socket, void *buffer) {
//
//	uint32_t size_received = 0;
//	uint32_t receiving;
//
//	uint32_t size_buffer; //el tamaño del buffer como maximo va a ser de 4 gigas (32bits)
//	if ((receiving = recv(socket, &size_buffer, sizeof(uint32_t), 0)) == -1) {
//		mostrar_error(-1, "Error reciving");
//		return -1;
//	}
//
//	if(receiving == 0)
//	{	return 0;}
//
//	//TODO TESTME
//	while (size_received < size_buffer) {
//		if ((receiving = recv(socket, (buffer + size_received), (size_buffer - size_received), 0)) == -1) {
//			mostrar_error(-1, "Error receiving");
//			return -1;
//		}
//
//		if(receiving == 0)
//		{	return 0;}
//
//		size_received += receiving;
//	}
//	return size_received;
//}

//---------------------------------------------------------------------------
int recibir(int socket, void *buffer) {

	void* aux_buffer;

	int result = recibir_dinamic_buffer(socket, &aux_buffer);

	//TODO TESTME

	if (result > 0) {
		memcpy(buffer, aux_buffer, result);
	}

	free(aux_buffer);
	return result;
}

//---------------------------------------------------------------------------
int recibir_dinamic_buffer(int socket, void** buffer) {

	uint32_t size_received = 0;
	uint32_t receiving;

	uint32_t size_buffer; //el tamaño del buffer como maximo va a ser de 4 gigas (32bits)

	*buffer = malloc(sizeof(char)); //para que se tome en cuenta de que cada vez que esta funcion es llamada hace un malloc

	if ((receiving = recv(socket, &size_buffer, sizeof(uint32_t), 0)) == -1) {
		mostrar_error(-1, "Error reciving");
		return -1;
	}

	if (receiving == 0) {
		return 0;
	}

	free(*buffer);
	*buffer = malloc(size_buffer);

	//TODO TESTME
	while (size_received < size_buffer) {

		if ((receiving = recv(socket, ((*buffer) + size_received), (size_buffer - size_received), 0)) == -1) {
			mostrar_error(-1, "Error receiving");
			return -1;
		}

		if (receiving == 0) {
			return 0;
		}

		size_received += receiving;
	}
	return size_received;
}

//---------------------------------------------------------------------------
char* get_IP() { //ojala sirva para algo jaja
	struct ifaddrs *interface_addr;
	struct sockaddr_in* sock_addr;
	char* addr;

	getifaddrs(&interface_addr);
	while (interface_addr) {
		if (interface_addr->ifa_addr->sa_family == AF_INET && strcmp(interface_addr->ifa_name, "eth0") == 0) {
			sock_addr = (struct sockaddr_in*) interface_addr->ifa_addr;
			addr = inet_ntoa(sock_addr->sin_addr);
		}
		interface_addr = interface_addr->ifa_next;
	}
	freeifaddrs(interface_addr);
	return addr;
}

//---------------------------------------------------------------------------
void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port) {
	address->sin_family = AF_INET; // familia de direcciones (siempre AF_INET)
	address->sin_port = htons(port); // setea Puerto a conectarme

	if (strlen(ip) != 0) {
		address->sin_addr.s_addr = inet_addr(ip); // Setea Ip a conectarme
	} else {
		address->sin_addr.s_addr = htonl(INADDR_ANY); // escucha todas las conexiones
	}

	memset(&(address->sin_zero), '\0', 8); // pone en ceros los bits que sobran de la estructura
}

//---------------------------------------------------------------------------
void getFromSocketAddrStd(struct sockaddr_in address, char** ip, int* port) {

	*port = ntohs(address.sin_port);

	if (htonl(INADDR_ANY) != address.sin_addr.s_addr) {
		*ip = strdup(inet_ntoa(address.sin_addr)); // Ip especificada
	} else {
		*ip = strdup("ANY IP"); // Cualquier Ip
	}
}

//-------------------*********************** Deberian ir en otra lib *******************
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
int has_all_properties(int cant_properties, char** properties, t_config* conf_arch) {
	int i;

	for (i = 0; (i < cant_properties) && (config_has_property(conf_arch, properties[i])); i++)
		;

	if (i < cant_properties) {
		for (i = 0; i < cant_properties; i++) {
			if (!config_has_property(conf_arch, properties[i])) {
				printf("Error: el archivo de conf no tiene %s\n", properties[i]);
			}
		}
		return 0;
	}
	return 1;
}

//-------------------------------------------------------------------------------------
void leerStdin(char *leido, int maxLargo) {
	fgets(leido, maxLargo, stdin);
	if ((strlen(leido) > 0) && (leido[strlen(leido) - 1] == '\n')) {
		leido[strlen(leido) - 1] = '\0';
	}
}
