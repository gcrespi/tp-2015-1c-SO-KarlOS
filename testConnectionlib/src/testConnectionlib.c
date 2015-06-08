/*
 ============================================================================
 Name        : testConnectionlib.c
 Author      : KarlOS
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <commons/log.h>
#include <string.h>
#include "../../connectionlib/connectionlib.h"

typedef struct {
	int protocolo;
	uint32_t numero;
	char* saludo;
	char saludo2[30];
} t_paquete;


int main(void) {
	t_log* logger = log_create("test.log","testConnectionLib",1,LOG_LEVEL_TRACE);

	int result, socket_fs;

	t_paquete enviado;
	t_paquete recibido;

	enviado.protocolo= FINISHED_JOB;
	enviado.numero= 777;
	enviado.saludo = strdup("Esto es una prueba");
	strcpy(enviado.saludo2, "chau");

	t_buffer* new_job_buff;

	log_info(logger, "Conectando con FS...");
	if ((socket_fs = solicitarConexionCon("127.0.0.1", 5556)) == -1) {
		log_error(logger, "No se pudo Conectar al FS");
		exit(-1);
	}
	log_info(logger, "Conectado a FS");

	new_job_buff = buffer_create_with_protocol(enviado.protocolo);

	buffer_add_int(new_job_buff, enviado.numero);
	buffer_add_string(new_job_buff, enviado.saludo);
	buffer_add_string(new_job_buff, enviado.saludo2);

	result = send_buffer_and_destroy(socket_fs,new_job_buff);

	log_debug(logger,"Paquete enviado: Protocolo: %i, Numero: %i Cadenas: %s, %s Result: %i",enviado.protocolo,enviado.numero,enviado.saludo,enviado.saludo2,result);


	recibido.protocolo = receive_protocol_in_order(socket_fs);

	result = receive_int_in_order(socket_fs,&(recibido.numero));
	result = receive_dinamic_array_in_order(socket_fs,(void **) &(recibido.saludo));
	result = receive_static_array_in_order(socket_fs,recibido.saludo2);

	log_debug(logger,"Paquete recibido: Protocolo: %i, Numero %i Cadenas: %s, %s Result: %i",recibido.protocolo,recibido.numero,recibido.saludo,recibido.saludo2,result);

	free(enviado.saludo);
	free(recibido.saludo);
	log_destroy(logger);
	return 0;
}
