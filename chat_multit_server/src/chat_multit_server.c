/*
 ============================================================================
 Name        : chat_server.c
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
#include <commons/collections/list.h>
#include <commons/string.h>
#include <pthread.h>

//Defines
#define BACKLOG 5
#define MAX_MSG 100	//maximo largo de un mensaje
#define SRC_PORT 3490 //puerto de destino
#define SRC_IP "127.0.0.1" //Ip de destino

int cerrar_servidor=0;

struct t_cliente {
	int* sockfd;
	pthread_t* thr;
};

//Prototypes
//-------------------------------------------------------------------------------------
void leerStdin(char *leido, int maxLargo) {
	fgets(leido, maxLargo, stdin);
	if ((strlen(leido) > 0) && (leido[strlen(leido) - 1] == '\n')) {
		leido[strlen(leido) - 1] = '\0';
	}
}

//-------------------------------------------------------------------------------------
void setSocketAddr(struct sockaddr_in* direccionOrigen) {

	direccionOrigen->sin_family = AF_INET; //familia de direcciones (siempre AF_INET)
	direccionOrigen->sin_port = htons(SRC_PORT);	//setea Puerto a conectarme
	direccionOrigen->sin_addr.s_addr = inet_addr(SRC_IP); //Setea Ip a conectarme
	memset(&(direccionOrigen->sin_zero), '\0', 8); //pone en ceros los bits que sobran de la estructura
}

//-------------------------------------------------------------------------------------
int abrirConexionConClientes(struct sockaddr_in* direccionOrigen) {

	int listener;

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) { //Crea un Socket
		perror("Error while socket()");
		exit(-1);
	}

	int yes=1;
	if (setsockopt(listener,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
		perror("setsockopt");
		exit(1);
	}

	if (bind(listener, (struct sockaddr*) direccionOrigen,sizeof(struct sockaddr)) == -1) { //Asignar puerto de escucha
		perror("Error while bind()");
		exit(-1);
	}

	if (listen(listener, BACKLOG) == -1) { //escuchar por clientes
		perror("Error while listen()");
		exit(-1);
	}

	return listener;
}

//--------------------------------------------------------------------
int aceptar_clientes(int listener,struct sockaddr_in* direccionOrigen)
{
	int sin_size = sizeof(struct sockaddr_in);
	int nuevo_socket;

	if ((nuevo_socket = accept(listener, (struct sockaddr*) direccionOrigen, (socklen_t*) &sin_size)) == -1) { //Crea un Socket
		perror("Error while accept()");
		exit(-1);
	}

	return nuevo_socket;
}



void hilo_conex_client(int* socket_cliente)
{





}

void liberar_cliente(struct t_cliente* cliente)
{
	free(cliente->sockfd);
	free(cliente->thr);
	free(cliente);
}


int main(void) {

	struct sockaddr_in socketaddr_srv; //estructura con puerto y direccion ip de Origen



	int listener;

	t_list* lista_clientes;
	struct t_cliente* cliente;

	lista_clientes = list_create();

	setSocketAddr(&socketaddr_srv);

	listener= abrirConexionConClientes(&socketaddr_srv);

	while(!cerrar_servidor)
	{	cliente = malloc(sizeof(struct t_cliente));
		cliente->sockfd= malloc(sizeof(int));
		cliente->thr = malloc(sizeof(pthread_t));

		list_add(lista_clientes,cliente);

		*(cliente->sockfd) = aceptar_clientes(listener,&socketaddr_srv);

		if (pthread_create(cliente->thr, NULL, (void*) hilo_conex_client, cliente->sockfd) != 0) {
			printf("Error al Intentar crear hilo del cliente!!!\n");
			return -1;
		}
	}

	list_iterate(lista_clientes,(void *)liberar_cliente);

	free(lista_clientes);

	return EXIT_SUCCESS;
}
