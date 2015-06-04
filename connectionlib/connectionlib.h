/*
 * connectionlib.h
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#ifndef CONNECTIONLIB_H_
#define CONNECTIONLIB_H_

#include <commons/bitarray.h>

//Enum del protocolo
enum protocolo {DISCONNECTED, INFO_NODO, NUEVO_JOB, ORDER_MAP, ORDER_REDUCE, INFO_ARCHIVO, FINISHED_JOB, ABORTED_JOB};


//************************ Nuevo Bitarray ***********************************
typedef struct {
	t_bitarray* bitarray_commons;
	size_t size_in_bits;
} t_kbitarray;


t_kbitarray* kbitarray_create(size_t cant_bits);
t_kbitarray* kbitarray_create_and_clean_all(size_t cant_bits);
void kbitarray_clean_all(t_kbitarray* self);
void kbitarray_set_all(t_kbitarray* self);
size_t kbitarray_amount_bits_set(t_kbitarray* self);
size_t kbitarray_amount_bits_clear(t_kbitarray* self);
bool kbitarray_test_bit(t_kbitarray* self, off_t bit_index);
void kbitarray_set_bit(t_kbitarray* self, off_t bit_index);
void kbitarray_clean_bit(t_kbitarray* self, off_t bit_index);
size_t kbitarray_get_size_in_bits(t_kbitarray* self);
size_t kbitarray_get_size_in_bytes(t_kbitarray* self);
void kbitarray_destroy(t_kbitarray* self);



uint32_t recibir_protocolo(int socket);
int enviar_protocolo(int socket, uint32_t protocolo);
int enviar_string(int socket, char *string);
int enviar_int(int socket, uint32_t numero);

int enviar(int socket, void *buffer, uint32_t size_buffer);
int recibir(int socket, void *buffer);
int recibir_dinamic_buffer(int socket, void** buffer);
char* get_IP();


int solicitarConexionCon(char* server_ip,int server_port);
int escucharConexionesDesde(char* server_ip,int server_port);
int aceptarCliente(int listener,struct sockaddr_in* direccionCliente);

void mostrar_error(int number, char* cause);

void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port); // setea el socketaddr para escuchar clientes o conectar con servidor
void getFromSocketAddrStd(struct sockaddr_in address, char** ip, int* port);

void free_string_splits(char** strings);
int has_all_properties(int cant_properties, char** properties, t_config* conf_arch);
void leerStdin(char *leido, int maxLargo);




#endif /* CONNECTIONLIB_H_ */
