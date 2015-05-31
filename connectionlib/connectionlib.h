/*
 * connectionlib.h
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#ifndef CONNECTIONLIB_H_
#define CONNECTIONLIB_H_

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
#include <commons/collections/list.h>

//Enum del protocolo
enum protocolo {DISCONNECTED, INFO_NODO, NUEVO_JOB, ORDER_MAP, ORDER_REDUCE, FINISHED_JOB};

uint32_t recibir_protocolo(int socket);
int enviar_protocolo(int socket, uint32_t protocolo);
int enviar_string(int socket, char *string);
int enviar_int(int socket, uint32_t numero);

int enviar(int , void*, uint32_t);
int recibir(int socket, void *buffer);
char* get_IP();


int solicitarConexionCon(char* server_ip,int server_port);
int solicitarConexionConServer(struct sockaddr_in *direccionDestino);

void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port); // setea el socketaddr para escuchar clientes o conectar con servidor

#endif /* CONNECTIONLIB_H_ */
