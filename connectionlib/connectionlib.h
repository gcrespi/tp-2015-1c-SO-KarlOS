/*
 * connectionlib.h
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#ifndef CONNECTIONLIB_H_
#define CONNECTIONLIB_H_

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h> //Esta la agregeue para poder definir int con tamaño especifico (uint32_t)
#include <ifaddrs.h>

//Enum del protocolo
enum protocolo {INFO_NODO};

int enviar(int , void*, uint32_t);
int recivir(int socket, void *buffer);
char* get_IP();

void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port); // setea el socketaddr para escuchar clientes o conectar con servidor

#endif /* CONNECTIONLIB_H_ */
