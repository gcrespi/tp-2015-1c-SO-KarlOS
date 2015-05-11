/*
 ============================================================================
 Name        : chat_multit_cliente.c
 Author      : karlOS
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

//Includes
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>

//Defines
#define MAX_MSG 100	//maximo largo de un mensaje
#define DEST_PORT 3490 //puerto de destino
#define DEST_IP "192.168.0.50" //Ip de destino

int socketfd_srv;	//file descriptor del Socket
int finalizar_chat = 0;
//Prototypes

//-------------------------------------------------------------------------------------
void leerStdin(char *leido, int maxLargo) {
	fgets(leido, maxLargo, stdin);
	if ((strlen(leido) > 0) && (leido[strlen(leido) - 1] == '\n')) {
		leido[strlen(leido) - 1] = '\0';
	}
}

//-------------------------------------------------------------------------------------
void setSocketAddr(struct sockaddr_in* direccionDestino) {

	direccionDestino->sin_family = AF_INET; //familia de direcciones (siempre AF_INET)
	direccionDestino->sin_port = htons(DEST_PORT);	//setea Puerto a conectarme
	direccionDestino->sin_addr.s_addr = inet_addr(DEST_IP); //Setea Ip a conectarme
	memset(&(direccionDestino->sin_zero), '\0', 8); //pone en ceros los bits que sobran de la estructura
}

//-------------------------------------------------------------------------------------
void abrirConexionConElServidor(struct sockaddr_in* direccionDestino) {

	if ((socketfd_srv = socket(AF_INET, SOCK_STREAM, 0)) == -1) { //Crea un Socket
		perror("Error while socket()");
		exit(-1);
	}

	if (connect(socketfd_srv, (struct sockaddr*) direccionDestino,
			sizeof(struct sockaddr)) == -1) { //conecta con el servidor
		perror("Error while connect()");
		exit(-1);
	}

}

//-------------------------------------------------------------------------------------
int recepcionarMsg(int primeraIteracion) {
	char msg_recv[MAX_MSG + 1]; //mensaje que voy a recibir
	int len_msg;				//largo del mensaje

	if ((len_msg = recv(socketfd_srv, msg_recv, MAX_MSG + 1, 0)) == -1) { //recive del servidor
		printf("Error while recv()\n");
	}

	msg_recv[len_msg] = '\0';

	if (len_msg == 0) {
		printf("El servidor te ha desconectado\n");
		return 0;
	}

	printf("Servidor dice: %s\n>", msg_recv);
	fflush(stdout);

	if ((strcmp(msg_recv, "El servidor no ha aceptado tu solicitud.") == 0)
			&& (primeraIteracion)) {
		return 0;
	}

	return 1;
}

//-------------------------------------------------------------------------------------
void hiloReceptor() {
	do {

		if (!recepcionarMsg(0)) {
			finalizar_chat = 1;
		}

	} while (!finalizar_chat);
}

//-------------------------------------------------------------------------------------
int mandar_msg() {
	char msg_send[MAX_MSG + 1]; //mensaje que voy a mandar/recibir

	leerStdin(msg_send, MAX_MSG + 1);
	printf(">");

	if (finalizar_chat) {
		return 2;
	}

	if(strcmp(msg_send,":exit")==0) {
		printf("Solicitó salir!\n>");
		fflush(stdout);
	}

	if (send(socketfd_srv, msg_send, strlen(msg_send), 0) == -1) { //envia mensaje escrito al servidor
		printf("Error while send()\n");
	}


	return 1;
}

//-------------------------------------------------------------------------------------
int main(void) {

	struct sockaddr_in socketaddr_srv; //estructura con puerto y direccion ip de Destino

	pthread_t thr_recep;
	void* ret_recep;

	setSocketAddr(&socketaddr_srv);

	abrirConexionConElServidor(&socketaddr_srv);

	if (recepcionarMsg(1)) {
		if (pthread_create(&thr_recep, NULL, (void*) hiloReceptor, NULL) != 0) {
			printf("Error al Intentar crear hilo Receptor!!!\n");
			return -1;
		}

		do {

			if (!mandar_msg()) {
				finalizar_chat = 2;
			}

		} while (!finalizar_chat);

		if (pthread_join(thr_recep, &ret_recep) != 0) {
			printf("Error al hacer join del hilo\n");
			return -1;
		}
	}

	return EXIT_SUCCESS;
}

