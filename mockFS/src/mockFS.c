/*
 ============================================================================
 Name        : mockFS.c
 Author      : KarlOS
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include <commons/string.h>
#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/log.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include "../../connectionlib/connectionlib.h"

t_log* paranoid_log;

typedef struct {
	int id;
	int cant_blocks;
} t_file;

typedef struct {
	int protocolo;
	uint32_t numero;
	char* saludo;
	char saludo2[30];
} t_paquete;

int main(void) {

	int cant_blocks[9] = { 1, 1, 2, 3, 5, 8, 13, 21, 34 };
	char paths_archivos[6][20] = {"/foo/faa","/foo/fee","/foo/fii","/fuu/faa","/fuu/fee","/fuu/fii"};

	char* path_archivo;

	int i = 0;
	paranoid_log = log_create("./logMaRTA.log", "MaRTA", 1, LOG_LEVEL_TRACE);

	int listener_jobs,marta_sock,prot;
	struct sockaddr_in socketaddr_cli;

	log_debug(paranoid_log, "Obteniendo Puerto para Escuchar Jobs...");
	if ((listener_jobs = escucharConexionesDesde("", 5556)) == -1) {
		log_error(paranoid_log, "No se pudo obtener Puerto para Escuchar Jobs");
		exit(-1);
	} else {
		log_debug(paranoid_log, "Puerto para Escuchar a Martu Obtenido");
	}

	marta_sock = aceptarCliente(listener_jobs,&socketaddr_cli);

	while(1) {
		prot = receive_protocol_in_order(marta_sock);

		if(prot == MARTA_CONNECTION_REQUEST) {
			log_debug(paranoid_log, "Se recibio un prot: MARTA_CONNECTION_REQUEST");
			send_protocol_in_order(marta_sock,MARTA_CONNECTION_ACCEPTED);
		}

		if(prot == INFO_ARCHIVO_REQUEST)
		{
			log_debug(paranoid_log, "Se recibio un prot: INFO_ARCHIVO");
			receive_dinamic_array_in_order(marta_sock,(void**) &path_archivo);

			int j;
			for(j=0;(j<10)&&(strcmp(path_archivo,paths_archivos[j])!=0);j++);

			free(path_archivo);

			t_buffer* info_file_buff = buffer_create_with_protocol(INFO_ARCHIVO);

			buffer_add_int(info_file_buff,j);
			buffer_add_int(info_file_buff,cant_blocks[i]);

			send_buffer_and_destroy(marta_sock,info_file_buff);

		} else {
			if(prot == DISCONNECTED) {
				marta_sock = aceptarCliente(listener_jobs,&socketaddr_cli);
			} else {
				if(prot == BLOCK_LOCATION_REQUEST) {
					uint32_t id_file,block_number;

					receive_int_in_order(marta_sock,&id_file);
					receive_int_in_order(marta_sock,&block_number);

					t_buffer* block_location_buff = buffer_create_with_protocol(BLOCK_LOCATION);

					buffer_add_int(block_location_buff,3);

					for (i = 0; i < 3; i++) {
						buffer_add_int(block_location_buff,i + 1);
						buffer_add_int(block_location_buff,inet_addr("127.0.0.1"));
						buffer_add_int(block_location_buff, 6666 + i);
						buffer_add_int(block_location_buff, 10 - 2 * i);
					}

					send_buffer_and_destroy(marta_sock,block_location_buff);
				}



//				if(ntohl(prot) == FINISHED_JOB)
//				{
//					t_paquete recibido;
//					log_debug(paranoid_log, "Se recibio un prot: FINISHED_JOB");
//
//					recibido.protocolo = ntohl(prot);
//					receive_int_in_order(marta_sock,&(recibido.numero));
//					receive_dinamic_array_in_order(marta_sock,(void **) &(recibido.saludo));
//					receive_static_array_in_order(marta_sock,recibido.saludo2);
//
//					log_debug(paranoid_log,"Paquete recibido: Protocolo: %i, Numero %i Cadenas: %s, %s",recibido.protocolo,recibido.numero,recibido.saludo,recibido.saludo2);
//
//					new_job_buff = buffer_create_with_protocol(recibido.protocolo);
//
//					buffer_add_int(new_job_buff, recibido.numero);
//					buffer_add_string(new_job_buff, recibido.saludo);
//					buffer_add_string(new_job_buff, recibido.saludo2);
//
//					int result = send_buffer_and_destroy(marta_sock,new_job_buff);
//
//					log_debug(paranoid_log,"Paquete enviado: Protocolo: %i, Numero %i Cadenas: %s, %s Result: %i",recibido.protocolo,recibido.numero,recibido.saludo,recibido.saludo2,result);
//				}

			}

		}


		i++;
		if(i/10) {
			i-=10;
		}
	}

	log_destroy(paranoid_log);
	return EXIT_SUCCESS;
}
