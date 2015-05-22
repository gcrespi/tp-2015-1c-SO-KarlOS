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
#include <unistd.h>
#include <ctype.h>

//Defines
#define BACKLOG 5
#define MAX_MSG 100	//maximo largo de un mensaje
#define SRC_PORT 3490 //puerto de destino
#define SRC_IP "127.0.0.1" //Ip de destino

int cerrar_servidor=0;

struct t_cliente {
	int* sockfd;
	pthread_t* thr;
	struct sockaddr_in socketaddr_cli;
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


//--------------------------------------------------------------------
int confirmar_aceptacion(int* socket_cliente)
{
	printf("Tiene una solicitud de conexion. Aceptar? s/n: ");

	char caracter_valido=0, acepta;
	do {
		acepta=getchar();

		if (toupper(acepta)=='N') {
			if (send(*socket_cliente, "El servidor no ha aceptado tu solicitud.", 40, 0) == -1) { //envia mensaje escrito al servidor
				printf("Error while send()\n");
			}
			close(*socket_cliente);
			caracter_valido=1;
		}
		else if (toupper(acepta)=='S')  {
			send(*socket_cliente, "El servidor ha aceptado tu solicitud.", 37, 0);
			caracter_valido=1;
		}
		else {
			printf("Ingrese caracter valido. Acepta? s/n: ");
		}
	} while(!caracter_valido);

	return toupper(acepta)=='S';
}


//-------------------------------------------------------------------
void hilo_conex_client(struct t_cliente *cliente)
{
	char msg_recv[MAX_MSG + 1]; //mensaje que voy a recibir
	int len_msg;				//largo del mensaje
	int cliente_cerrado = 0;


	if(confirmar_aceptacion(cliente->sockfd))
	{
		while((!cerrar_servidor)&&(!cliente_cerrado))
		{

			if ((len_msg = recv(*(cliente->sockfd), msg_recv, MAX_MSG + 1, 0)) == -1) { //recive del servidor
				printf("Error while recv()\n");
			}

			msg_recv[len_msg] = '\0';

			if ((len_msg==0)||(strcmp(msg_recv,":exit")==0)){
				if(len_msg==0) {
					printf("%s cerro conexion\n",inet_ntoa(cliente->socketaddr_cli.sin_addr));
				} else {
					printf("%s solicito salir\n",inet_ntoa(cliente->socketaddr_cli.sin_addr));
				}

				close(*(cliente->sockfd));
				cliente_cerrado = 1;
			}
			else {
				printf("%s dice: %s\n",inet_ntoa(cliente->socketaddr_cli.sin_addr),msg_recv);

				printf("Tu: ");
				leerStdin(msg_recv,MAX_MSG+1);
				send(*(cliente->sockfd),msg_recv,strlen(msg_recv),0);
			}

		}

		if(!cliente_cerrado)
		{
			close(*(cliente->sockfd));
			cliente_cerrado = 1;
		}
	}


}

void liberar_cliente(struct t_cliente* cliente)
{
	free(cliente->sockfd);
	free(cliente->thr);
	free(cliente);
}


void esperar_finalizacion_hilo_conex_client(struct t_cliente* cliente)
{
	void* ret_recep;

	if (pthread_join(*cliente->thr, &ret_recep) != 0) {
		printf("Error al hacer join del hilo\n");
	}
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

		*(cliente->sockfd) = aceptar_clientes(listener,&(cliente->socketaddr_cli));

		if (pthread_create(cliente->thr, NULL, (void*) hilo_conex_client, cliente) != 0) {
			printf("Error al Intentar crear hilo del cliente!!!\n");
			return -1;
		}
	}

	printf("Esperando finalizacion de hilos de conex a cliente\n");
	fflush(stdout);

	list_iterate(lista_clientes,(void *)esperar_finalizacion_hilo_conex_client);

	list_destroy_and_destroy_elements(lista_clientes,(void *)liberar_cliente);

	free(lista_clientes);

	return EXIT_SUCCESS;
}
