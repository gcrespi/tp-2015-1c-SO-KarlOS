/*
 * connectionlib.c
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#include "../connectionlib/connectionlib.h"

//---------------------------------------------------------------------------
int enviar(int socket, void *buffer, uint32_t size_buffer) {
	int result=0;
	if (send(socket, &size_buffer, sizeof(uint32_t), 0) == -1) {
		return -1;
	}
	if ((result += send(socket, buffer, size_buffer, 0)) == -1) {
		return -1;
	}
	return result;
}

//---------------------------------------------------------------------------
int recivir(int socket, void *buffer) {
	int result=0;
	uint32_t size_buffer; //el tamaño del buffer como maximo va a ser de 4 gigas (32bits)
	if (recv(socket, &size_buffer, sizeof(uint32_t), 0) == -1) {
		return -1;
	}
	if ((result += recv(socket, buffer, size_buffer, 0)) == -1) {
		return -1;
	}
	return result;
}

//---------------------------------------------------------------------------
char* get_IP(){ //ojala sirva para algo jaja
	    struct ifaddrs *interface_addr;
	    struct sockaddr_in* sock_addr;
	    char* addr;

	    getifaddrs (&interface_addr);
	    while(interface_addr){
	        if (interface_addr->ifa_addr->sa_family==AF_INET && strcmp(interface_addr->ifa_name,"eth0")==0 ) {
	            sock_addr = (struct sockaddr_in*) interface_addr->ifa_addr;
	        	addr = inet_ntoa(sock_addr->sin_addr);
	        }
	        interface_addr = interface_addr->ifa_next;
	    }
	    freeifaddrs(interface_addr);
	    return addr;
}
