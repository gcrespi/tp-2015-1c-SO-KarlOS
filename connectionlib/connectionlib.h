/*
 * connectionlib.h
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#ifndef CONNECTIONLIB_H_
#define CONNECTIONLIB_H_

#include <commons/config.h>
#include <commons/collections/list.h>
#include <stdint.h>
#include <stddef.h>
#include <arpa/inet.h>

typedef struct {
	char *buffer;
	size_t size;
} t_buffer;


//Enum del protocolo
enum protocolo {
	DISCONNECTED, INFO_NODO, NUEVO_JOB, ORDER_MAP, ORDER_REDUCE, INFO_ARCHIVO,
	FINISHED_JOB, ABORTED_JOB, WRITE_BLOCK, READ_BLOCK, MAP_OK, NODO_NOT_FOUND,
	INFO_ARCHIVO_REQUEST, ARCHIVO_NO_DISPONIBLE, BLOCK_LOCATION_REQUEST, BLOCK_LOCATION,
	LOST_BLOCK, READ_RESULT_JOB, MARTA_CONNECTION_REQUEST, MARTA_CONNECTION_ACCEPTED, MARTA_CONNECTION_REFUSED,
	ARCHIVO_INEXISTENTE, EXECUTE_MAP, EXECUTE_REDUCE, REDUCE_OK, TEMP_NOT_FOUND,
	NODO_LOCATION_REQUEST, NODO_LOCATION, LOST_NODO, SAVE_RESULT_REQUEST,
	SAVE_OK, SAVE_ABORT

};

t_buffer* buffer_create();
t_buffer* buffer_create_with_protocol(uint32_t protocolo);
void buffer_add_string(t_buffer* self, char *string_to_add);
void buffer_add_int(t_buffer* self, uint32_t int_to_add);
void buffer_add_buffer_and_destroy_added(t_buffer* self, t_buffer* added);

int send_protocol_in_order(int socket, uint32_t protocol);
int send_int_in_order(int socket, uint32_t entero);
int send_buffer_and_destroy(int socket, t_buffer* self);
void buffer_destroy(t_buffer* self);


int receive_dinamic_array_in_order(int socket, void** buffer);
int receive_static_array_in_order(int socket, void *buffer);
int receive_int_in_order(int socket, uint32_t *number);
uint32_t receive_protocol_in_order(int socket);


int send_stream_without_size(int socket, void *buffer, uint32_t size_buffer);
int send_stream_with_size_in_order(int socket, void *buffer, uint32_t size_buffer);


int send_entire_file_by_parts(int socket, char* src_path, size_t max_part_length);
int receive_entire_file_by_parts(int socket, char* dest_path, size_t max_part_length);



char* get_IP();

int solicitarConexionCon(char* server_ip, int server_port);
int escucharConexionesDesde(char* server_ip, int server_port);
int aceptarCliente(int listener, struct sockaddr_in* direccionCliente);

void mostrar_error(int number, char* cause);

void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port); // setea el socketaddr para escuchar clientes o conectar con servidor
void getFromSocketAddrStd(struct sockaddr_in address, char** ip, int* port);
char* from_int_to_inet_addr(uint32_t ip_int);

void free_string_splits(char** strings);
int has_all_properties(int cant_properties, char** properties, t_config* conf_arch);
void leerStdin(char *leido, int maxLargo);

void string_static_trim_left(char* string);
void string_static_trim_right(char* string);
void string_static_trim(char* string);

int contains(void* elem, t_list* list);
int string_split_size(char** matriz);
void* mayorSegun(void* elemento1, void* elemento2, int(*criterio)(void*));
void* menorSegun(void* elemento1, void* elemento2, int(*criterio)(void*));
void* foldl(void*(*function)(void*, void*), void* seed, t_list* list);
void* foldl1(void*(*function)(void*, void*), t_list* list);


#endif /* CONNECTIONLIB_H_ */
