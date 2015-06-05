/*
 * connectionlib.h
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#ifndef CONNECTIONLIB_H_
#define CONNECTIONLIB_H_

#include <commons/bitarray.h>


typedef struct {
	char *buffer;
	size_t size;
} t_buffer;


//Enum del protocolo
enum protocolo {
	DISCONNECTED, INFO_NODO, NUEVO_JOB, ORDER_MAP, ORDER_REDUCE, INFO_ARCHIVO, FINISHED_JOB, ABORTED_JOB
};

t_buffer* buffer_create_with_protocol(uint32_t protocolo);
void buffer_add_string(t_buffer* self, char *string_to_add);
void buffer_add_int(t_buffer* self, uint32_t int_to_add);
int send_buffer_and_destroy(int socket, t_buffer* self);
void buffer_destroy(t_buffer* self);


uint32_t recibir_protocolo(int socket);
int enviar_protocolo(int socket, uint32_t protocolo);
int enviar_string(int socket, char *string);
int enviar_int(int socket, uint32_t numero);

int enviar(int socket, void *buffer, uint32_t size_buffer);
int recibir(int socket, void *buffer);
int recibir_dinamic_buffer(int socket, void** buffer);
char* get_IP();

int solicitarConexionCon(char* server_ip, int server_port);
int escucharConexionesDesde(char* server_ip, int server_port);
int aceptarCliente(int listener, struct sockaddr_in* direccionCliente);

void mostrar_error(int number, char* cause);

void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port); // setea el socketaddr para escuchar clientes o conectar con servidor
void getFromSocketAddrStd(struct sockaddr_in address, char** ip, int* port);

void free_string_splits(char** strings);
int has_all_properties(int cant_properties, char** properties, t_config* conf_arch);
void leerStdin(char *leido, int maxLargo);

#endif /* CONNECTIONLIB_H_ */
