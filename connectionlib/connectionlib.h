/*
 * connectionlib.h
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#ifndef CONNECTIONLIB_H_
#define CONNECTIONLIB_H_



//Enum del protocolo
enum protocolo {DISCONNECTED, INFO_NODO, NUEVO_JOB, ORDER_MAP, ORDER_REDUCE, FINISHED_JOB};

uint32_t recibir_protocolo(int socket);
int enviar_protocolo(int socket, uint32_t protocolo);
int enviar_string(int socket, char *string);
int enviar_int(int socket, uint32_t numero);

int enviar(int socket, void *buffer, uint32_t size_buffer);
int recibir(int socket, void *buffer);
char* get_IP();


int solicitarConexionCon(char* server_ip,int server_port);
int escucharConexionesDesde(char* server_ip,int server_port);
int aceptarCliente(int listener,struct sockaddr_in* direccionCliente);

void mostrar_error(int number, char* cause);

void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port); // setea el socketaddr para escuchar clientes o conectar con servidor

void free_string_splits(char** strings);
int has_all_properties(int cant_properties, char** properties, t_config* conf_arch);
void leerStdin(char *leido, int maxLargo);


#endif /* CONNECTIONLIB_H_ */
