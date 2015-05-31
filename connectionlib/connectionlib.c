/*
 * connectionlib.c
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#include "../connectionlib/connectionlib.h"

//---------------------------------------------------------------------------
void mostrar_error(int number, char* cause) {
	perror(cause);
//	quizas debería distinguirse error insalvable de error ignorable
	getchar();
}


int solicitarConexionCon(char* server_ip,int server_port) {
	struct sockaddr_in socketaddr_server;

	setSocketAddrStd(&socketaddr_server,server_ip,server_port);
	return solicitarConexionConServer(&socketaddr_server);
}

//---------------------------------------------------------------------------
int solicitarConexionConServer(struct sockaddr_in *direccionDestino) {

	int socketfd_server;

	if ((socketfd_server = socket(AF_INET, SOCK_STREAM, 0)) == -1) { // crea un Socket
		mostrar_error(-1, "Error while socket()");
		return -1;
	}

	if (connect(socketfd_server, (struct sockaddr*) direccionDestino,
			sizeof(struct sockaddr)) == -1) { // conecta con el servidor
		mostrar_error(-1, "Error while connect()");
		return -1;
	}
	return socketfd_server;
}


//---------------------------------------------------------------------------
int enviar_protocolo(int socket, uint32_t protocolo) {
	uint32_t prot = protocolo;

	if ((send(socket, &prot, sizeof(uint32_t), 0)) == -1) { //envia el protocolo
		mostrar_error(-1, "Error sending");
		return -1;
	}

	return 0;
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
	return enviar(socket,&aux_numero,sizeof(uint32_t));
}

//---------------------------------------------------------------------------
int enviar_string(int socket, char *string) {
	return enviar(socket,string,strlen(string));
}

//---------------------------------------------------------------------------
int enviar(int socket, void *buffer, uint32_t size_buffer) {
	int result = 0;
	if (send(socket, &size_buffer, sizeof(uint32_t), 0) == -1) {
		mostrar_error(-1, "Error sending");
		return -1;
	}
	if ((result += send(socket, buffer, size_buffer, 0)) == -1) {
		mostrar_error(-1, "Error sending");
		return -1;
	}
	return result;
}

//---------------------------------------------------------------------------
int recibir(int socket, void *buffer) {
	int result = 0;
	uint32_t size_buffer; //el tamaño del buffer como maximo va a ser de 4 gigas (32bits)
	if (recv(socket, &size_buffer, sizeof(uint32_t), 0) == -1) {
		mostrar_error(-1, "Error reciving");
		return -1;
	}
	if ((result += recv(socket, buffer, size_buffer, 0)) == -1) {
		mostrar_error(-1, "Error reciving");
		return -1;
	}
	return result;
}

//---------------------------------------------------------------------------
char* get_IP() { //ojala sirva para algo jaja
	struct ifaddrs *interface_addr;
	struct sockaddr_in* sock_addr;
	char* addr;

	getifaddrs(&interface_addr);
	while (interface_addr) {
		if (interface_addr->ifa_addr->sa_family == AF_INET
				&& strcmp(interface_addr->ifa_name, "eth0") == 0) {
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
	address->sin_addr.s_addr = inet_addr(ip); // Setea Ip a conectarme
	memset(&(address->sin_zero), '\0', 8); // pone en ceros los bits que sobran de la estructura
}



//-------------------*********************** Deberian ir en otra lib *******************


