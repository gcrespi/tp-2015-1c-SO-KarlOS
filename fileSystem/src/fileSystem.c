/*
 ============================================================================
 Name        : fileSystem.c
 Author      : karlOS
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
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include "../../connectionlib/connectionlib.h"
#include "../../kbitarray/kbitarray.h"
#include "../../mongobiblioteca/mongobiblioteca.h"

//Constantes de la consola
#define MAX_COMMANOS_VALIDOS 30
#define MAX_COMMAND_LENGTH 100
#define RED  "\033[1m\033[31m"
#define BOLD "\033[1m\033[37m"
#define BLUE "\033[1m\033[36m"
#define NORMAL  "\033[0m"
#define CLEAR "\033[H\033[J"
#define OFFSET 0

//  Estados del nodo
enum t_client_type {
	MARTA,NODO, INVALID
};

/* YA ESTA EN LA MONGOLIB

//  Estados del nodo
enum t_estado_nodo {
	DESCONECTADO,CONECTADO
};

//no se persisten: estado, aceptado
//ya que cuando se cae el FS deben volver a conectar
struct t_nodo {
	//numero unico de identificacion de cada nodo
	uint32_t id_nodo;

	//file descriptor del socket del nodo
	uint32_t socket_FS_nodo;

	char usando_socket;

	//IP del socket
	uint32_t ip_listen;

	//Puerto del socket
	uint32_t port_listen;

	//DESCONECTADO, CONECTADO, PENDIENTE (de aceptacion)
	int estado;

	//cantidad de bloques que se pueden almacenar en el nodo
	int cantidad_bloques;

	//array de bits donde cada bit simboliza un bloque del nodo
	//bloqueOcupado = 1, bloqueVacio = 0
	t_kbitarray* bloquesLlenos;

};

//Estructura de carpetas del FS (Se persiste)
struct t_dir {
	struct t_dir* parent_dir;
	char* nombre;
	t_list* list_dirs;
	t_list* list_archs;
};

struct t_copia_bloq {
	//numero de nodo en el que esta la copia
	int id_nodo;

	//bloque del nodo en donde esta la copia
	int bloq_nodo;
};

struct t_bloque {
	//numero de bloque del archivo fragmentado
	int nro_bloq;

	//cada una de las copias que tiene el bloque
	t_list* list_copias;
};

//Se persiste, sino perdería toda la info sobre los archivos guardados y sus bloques
struct t_arch {
	//codigo unico del archivo fragmentado
	int id_archivo;

	//nombre del archivo dentro del MDFS
	char *nombre;

	//directorio padre del archivo dentro del MDFS
	struct t_dir *parent_dir;

	//cantidad de bloques en las cuales se fragmento el archivo
	int cant_bloq;

	//(lista de bloq_archivo) Lista de los bloques que componen al archivo
	t_list* bloques;
};
*/


// La estructura que contiene todos los datos del arch de conf
struct conf_fs {
	int fs_vacio;
	int puerto_listen;
	int min_cant_nodos;
};

// La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	uint32_t id;
	uint32_t nodo_nuevo;
	uint32_t cant_bloques;
	uint32_t socket_FS_nodo;
	uint32_t ip_listen;
	uint32_t port_listen;

};

//Prototipos de la consola
void receive_command(char*, int);
char execute_command(char*);
void help();
void format();
void pwd();
void ls();
void cd(char*);
void rm(char*);
void mv(char*,char*);
void makedir(char*);
void remdir(char*);
void mvdir(char*, char*);
int upload(char*, char*, int);
int download(char*, char*, int);
void md5(char*);
void blocks();
void rmblock(char*,char*,char*);
void cpblock(char*,char*);
void lsrequest();
void lsnode();
void addnode(char*);
void rmnode(char*);

//Prototipos
void levantar_arch_conf();
int recivir_info_nodo (int);
int recibir_instrucciones_nodo(int socket, fd_set* master);
void hilo_listener();
void preparar_fs (); //Configura lo inicial del fs o levanta la informacion persistida.
void set_root();
void info_nodo_destroy(struct info_nodo*);
void dir_destroy(struct t_dir*);
void arch_destroy(struct t_arch*);
void bloque_destroy(struct t_bloque*);
void nodo_destroy(struct t_nodo*);
void nodo_remove(struct t_nodo*);
void print_path_actual();
int warning(char*);
struct t_dir* dir_create(char*, struct t_dir*);
struct t_arch* arch_create(char*, struct t_dir*, int, t_list*);
void get_info_from_path(char*, char**, struct t_dir**);
struct t_dir* get_dir_from_path(char*);
struct info_nodo* find_infonodo_with_sockfd(int);
struct t_nodo* find_nodo_with_sockfd(int);
struct t_nodo* find_nodo_with_ID(int);
void* list_remove_elem(t_list*, void*);
void list_destroy_elem(t_list*, void*, void*);
int estaDisponibleElArchivo(struct t_arch* archivo);
int estaActivoNodo(int);
struct t_bloque* find_block_with_num(int,t_list*);
int any_block_with_num(int,t_list*);
struct t_arch* get_arch_from_path(char* path);
void set_kbitarrays_de_nodos();
void foreach_dir_do(void(*closure)(struct t_dir*));
void foreach_dir_do_starts_from(void(*closure)(struct t_dir*), struct t_dir*);
void clean_copies_from_nodo(int);
int save_result(char*, char*, int);
void copia_bloque_destroy(struct t_copia_bloq*);

//Variables Globales
struct conf_fs conf; //Configuracion del fs
char end; //Indicador de que deben terminar todos los hilos
t_list* list_info_nodo; //Lista de nodos que solicitan conectarse al FS
t_list* listaNodos; //Lista de nodos activos o desconectados
pthread_mutex_t mutex_listaNodos;

int arch_id_counter; //Se incrementa cada vez que s hace un upload
int dir_id_counter;
struct t_dir* root;
struct t_dir* dir_actual;

t_log* logger;

int main(void) {
	listaNodos = list_create();
	pthread_mutex_init(&mutex_listaNodos,NULL);

	logger = log_create("fs.log","FS",0,LOG_LEVEL_TRACE);
	log_info(logger,"Se inicia el MDFS");
	list_info_nodo = list_create();

	levantar_arch_conf();   //Levanta el archivo de configuracion "fs.cfg"
	preparar_fs ();
	pthread_t t_listener;
	pthread_create(&t_listener, NULL, (void*) hilo_listener, NULL);

	char command[MAX_COMMAND_LENGTH + 1];
	puts(CLEAR RED"KarlOS MDFS\n"NORMAL"Type 'help' to show the commands");
	do {
		printf("MDFS:~"); print_path_actual(); printf("$ ");
		receive_command(command,MAX_COMMAND_LENGTH+1);
		end = execute_command(command);
	} while(!end);
	puts("Bye :)");

	pthread_cancel(t_listener);
	dir_destroy(root);

	pthread_join(t_listener, NULL);
	list_destroy_and_destroy_elements(list_info_nodo, (void*) info_nodo_destroy);
	list_destroy_and_destroy_elements(listaNodos, (void*) nodo_destroy);
	log_destroy(logger);
	pthread_mutex_destroy(&mutex_listaNodos);
	cerrarMongo();
	log_info(logger,"Se cierra todo correctamente");
	return EXIT_SUCCESS;
}


//---------------------------------------------------------------------------
char connectionIsBeingUsed(int socket) {
	struct t_nodo* nodo;



	if((nodo = find_nodo_with_sockfd(socket)) != NULL) {
		int self;
		pthread_mutex_lock(&(nodo->mutex_socket));
		if((self = nodo->usando_socket)) {
			nodo->usando_socket = 0;
		}
		pthread_mutex_unlock(&(nodo->mutex_socket));
		return self;
	}

	return 0;
}

//---------------------------------------------------------------------------
int is_protocol_from(int prot) {
	switch(prot) {
		case INFO_NODO:
			return NODO;
			break;

		case MARTA_CONNECTION_REQUEST:
			return MARTA;
			break;
	}

	return prot;
}

//---------------------------------------------------------------------------
int isNodeActive(struct t_nodo* nodo) {
	return nodo->estado == CONECTADO;
}


//---------------------------------------------------------------------------
int tieneSuficientesNodos() {

	int self;

	pthread_mutex_lock(&mutex_listaNodos);
	self =conf.min_cant_nodos <= list_count_satisfying(listaNodos,(void*) isNodeActive);
	pthread_mutex_unlock(&mutex_listaNodos);

	return self;
}

//---------------------------------------------------------------------------
int receive_marta(int martaSock, uint32_t prot) {

	switch(prot) {

		case MARTA_CONNECTION_REQUEST:
			if(tieneSuficientesNodos()) {
				log_info(logger,"Se acepta conexion con marta");
;				return send_protocol_in_order(martaSock,MARTA_CONNECTION_ACCEPTED);
			} else {
				log_info(logger,"Se rechaza conexion con marta por falta de nodos");
				return send_protocol_in_order(martaSock,MARTA_CONNECTION_REFUSED);
			}
			break;

		default:
			return -1;
	}
}

//---------------------------------------------------------------------------
int receive_new_client(int listener, int* martaSock) {

	struct sockaddr_in sockaddr_cli;
	int clientSock, result;

	if((clientSock = aceptarCliente(listener,&sockaddr_cli)) <= 0) {
		log_error(logger,"No se pudo aceptar un cliente");
		return -1;
	}

	int prot_new_client = receive_protocol_in_order(clientSock);

	switch(is_protocol_from(prot_new_client)) {

		case NODO:
			result = recivir_info_nodo(clientSock);
			break;


		case MARTA:
			*martaSock = clientSock;
			result = receive_marta(clientSock,prot_new_client);
			break;

		default:
			result = -1;
			break;
	}

	if(result > 0) {
		return clientSock;
	}

	return result;
}

//---------------------------------------------------------------------------
int esSocketDeNodo(int self) {
	if(find_infonodo_with_sockfd(self) != NULL) return 1;
	else if(find_nodo_with_sockfd(self) != NULL) return 1;
	else return 0;
}

//---------------------------------------------------------------------------
int recibir_instrucciones_nodo(int socket, fd_set* master)
{
	struct info_nodo* infnodo;
	struct t_nodo* nodo;

	int prot = receive_protocol_in_order(socket);


	switch (prot) {
		case DISCONNECTED:
			if( (infnodo = find_infonodo_with_sockfd(socket)) != NULL ){
				log_info(logger,"Se ha Desconectado el nodo en espera ID: %i",infnodo->id);
				list_destroy_elem(list_info_nodo,infnodo, (void*) info_nodo_destroy);
			} else if (( nodo = find_nodo_with_sockfd(socket)) != NULL ) {
				nodo->estado = DESCONECTADO;
				FD_CLR(socket, master);
				log_info(logger,"Se ha Desconectado el nodo aceptado ID: %i",nodo->id_nodo);
			}
			return 1;
			break;

		default:
			log_error(logger,"Protocolo del NODO no esperado: %i",prot);
			return -1;
			break;
	}

}

//---------------------------------------------------------------------------
void buffer_add_block_location(t_buffer *buffer, struct t_arch *file, int block_number){
	struct t_bloque* block = find_block_with_num(block_number,file->bloques);

	int is_available(struct t_copia_bloq* copy){
		return estaActivoNodo(copy->id_nodo);
	}
	t_list *list_available_copies = list_filter(block->list_copias, (void*) is_available);

	int ammount_copies = list_size(list_available_copies);
	buffer_add_int(buffer,ammount_copies);

	void charge_buffer(struct t_copia_bloq* copy){

		buffer_add_int(buffer, copy->id_nodo);
		buffer_add_int(buffer, copy->bloq_nodo);
	}
	list_iterate(list_available_copies, (void*) charge_buffer);
	list_destroy(list_available_copies);

}

//---------------------------------------------------------------------------
int receive_marta_instructions(int *martaSock, fd_set *master) {

	int prot = receive_protocol_in_order(*martaSock);
	int result = 0;
	char *path_file;
	uint32_t id_nodo;

	uint32_t block_number;

	switch (prot) {
	case INFO_ARCHIVO_REQUEST:
		result = receive_dinamic_array_in_order(*martaSock,(void**) &path_file);
		if(result > 0) {
			log_info(logger, "Solicitud de MaRTA de Información de Archivo %s",path_file);
			struct t_arch* file = get_arch_from_path(path_file);
			free(path_file);

			if(file !=NULL) {
				if(estaDisponibleElArchivo(file)) {
					t_buffer* info_file_buff = buffer_create_with_protocol(INFO_ARCHIVO);
					buffer_add_int(info_file_buff,file->cant_bloq);
					result = send_buffer_and_destroy(*martaSock,info_file_buff);
				} else {
					result = send_protocol_in_order(*martaSock,ARCHIVO_NO_DISPONIBLE);
				}
			} else {
				result = send_protocol_in_order(*martaSock,ARCHIVO_INEXISTENTE);
			}
		}

		if(result < 0) {
			log_error(logger, "No se pudo enviar a MaRTA Info de Archivo");
			return -1;
		}
		if(result == 0) {
			log_warning(logger, "MaRTA se Desconectó");
			FD_CLR(*martaSock, master);
			*martaSock = -1;
			return 1;
		}
		break;

	case BLOCK_LOCATION_REQUEST:

		result = receive_dinamic_array_in_order(*martaSock,(void**) &path_file);
		if(result > 0) {
			result = receive_int_in_order(*martaSock,&block_number);
			if(result > 0) {
				log_info(logger, "Solicitud de MaRTA de Localización de Archivo: %s Bloque: %i",path_file, block_number);
				struct t_arch* file = get_arch_from_path(path_file);

				if(file !=NULL) {
					if(estaDisponibleElArchivo(file)&&any_block_with_num(block_number,file->bloques)) {
						t_buffer* block_location_buff = buffer_create_with_protocol(BLOCK_LOCATION);

						buffer_add_block_location(block_location_buff,file,block_number);

						result = send_buffer_and_destroy(*martaSock,block_location_buff);
					} else {
						result = send_protocol_in_order(*martaSock,ARCHIVO_NO_DISPONIBLE);
					}
				} else {
					result = send_protocol_in_order(*martaSock,ARCHIVO_INEXISTENTE);
				}
			}
		}
		free(path_file);

		if(result < 0) {
			log_error(logger, "No se pudo enviar a MaRTA Info de Bloque");
			return -1;
		}

		if(result == 0) {
			log_warning(logger, "MaRTA se Desconectó");
			FD_CLR(*martaSock, master);
			*martaSock = -1;
			return 1;
		}
		break;

	case NODO_LOCATION_REQUEST:
		result = receive_int_in_order(*martaSock, &id_nodo);

		if(result > 0) {
			log_info(logger, "Solicitud de MaRTA de Localización de Nodo: %i",id_nodo);
			struct t_nodo* nodo = find_nodo_with_ID(id_nodo);
			t_buffer* nodo_location_buff;
			if(nodo != NULL) {
				nodo_location_buff = buffer_create_with_protocol(NODO_LOCATION);
				buffer_add_int(nodo_location_buff,nodo->ip_listen);
				buffer_add_int(nodo_location_buff,nodo->port_listen);
			} else {
				nodo_location_buff = buffer_create_with_protocol(LOST_NODO);
			}
			result = send_buffer_and_destroy(*martaSock, nodo_location_buff);
		}
		if(result < 0) {
			log_error(logger, "No se pudo enviar a MaRTA Localización de Nodo");
			return -1;
		}

		if(result == 0) {
			log_warning(logger, "MaRTA se Desconectó");
			FD_CLR(*martaSock, master);
			*martaSock = -1;
			return 1;
		}
		break;

	case SAVE_RESULT_REQUEST:
		result = receive_int_in_order(*martaSock, &id_nodo);
		char* src_name;
		char* path_result;
		result = (result > 0) ? receive_dinamic_array_in_order(*martaSock,(void **) &src_name) : result;
		result = (result > 0) ? receive_dinamic_array_in_order(*martaSock,(void **) &path_result) : result;

		if(result > 0) {
			result = save_result(path_result, src_name, id_nodo);

			t_buffer* save_result_buff;
			if(result > 0) {
				save_result_buff = buffer_create_with_protocol(SAVE_OK);
			} else {
				save_result_buff = buffer_create_with_protocol(SAVE_ABORT);
			}
			result = send_buffer_and_destroy(*martaSock, save_result_buff);
		}

		free(src_name);
		free(path_result);

		if(result < 0) {
			log_error(logger, "No se pudo guardar resultado de Job");
			return -1;
		}

		if(result == 0) {
			log_warning(logger, "MaRTA se Desconectó");
			FD_CLR(*martaSock, master);
			*martaSock = -1;
			return 1;
		}
		break;

	case DISCONNECTED:
		log_warning(logger, "MaRTA se Desconectó");
		FD_CLR(*martaSock, master);
		*martaSock = -1;
		return 1;
		break;

	case -1:
		log_error(logger, "No se pudo recibir instrucciones de MaRTA");
		return -1;
		break;

	default:
		log_error(logger, "Protocolo Inesperado %i", prot);
		return -1;
		break;
	}

	return result;
}


//---------------------------------------------------------------------------
void hilo_listener() {

	fd_set master; // Nuevo set principal
	fd_set read_fds; // Set temporal para lectura
	FD_ZERO(&master); // Vacio los sets
	FD_ZERO(&read_fds);
	int fd_max; // Va a ser el maximo de todos los descriptores de archivo del select
	int martaSock = -1;
	int listener;

	void cleanup() {
		close(listener);
	}
	pthread_cleanup_push(cleanup,NULL); // Handler de la cancelacion, funciona similar a Try-Catch

	if((listener = escucharConexionesDesde("", conf.puerto_listen)) == -1) {
		log_error(logger, "No se pudo obtener Puerto para Escuchar Clientes");
		exit(-1);
	} else {
	}

	FD_SET(listener, &master);
	fd_max = listener;

	int i;
	while(1){
		read_fds = master; // Cada iteracion vuelvo a copiar del principal al temporal
		select(fd_max + 1, &read_fds, NULL, NULL, NULL); // El select se encarga de poner en los temp los fds que recivieron algo
		for (i = 0; i <= fd_max; i++) {
			if (FD_ISSET(i, &read_fds)) {

				if(i==listener) {
					int clientSock;

					if((clientSock = receive_new_client(listener,&martaSock))<=0) {
						log_error(logger,"No se pudo aceptar un cliente");
					}

					FD_SET(clientSock, &master);
					if (clientSock > fd_max)
						fd_max = clientSock;

				} else if(martaSock == i) {
					int result;
					if((result = receive_marta_instructions(&martaSock, &master))<=0) {
						log_error(logger,"Error al recibir instrucciones de MaRTA");
					}

				} else if(esSocketDeNodo(i)) {
					if(!connectionIsBeingUsed(i)) {
						if(recibir_instrucciones_nodo(i, &master) <= 0) {
							log_error(logger,"Error al recibir instrucciones de nodo");
						}
					}
				}
			}
		}
	}

	pthread_cleanup_pop(0); //Fin del Handler (Try-Catch)

}

//---------------------------------------------------------------------------
int recivir_info_nodo (int socket){
	struct info_nodo* info_nodo;
	info_nodo = malloc(sizeof(struct info_nodo));

	int result = receive_int_in_order(socket,&(info_nodo->id));

	result = (result > 0) ? receive_int_in_order(socket,&(info_nodo->cant_bloques)) : result;
	result = (result > 0) ? receive_int_in_order(socket,&(info_nodo->nodo_nuevo)) : result;
	result = (result > 0) ? receive_int_in_order(socket,&(info_nodo->ip_listen)) : result;
	result = (result > 0) ? receive_int_in_order(socket,&(info_nodo->port_listen)) : result;

	if(result) log_info(logger,"Se recibio solicitud del nodo con exito");
	else log_info(logger,"Error al recibir solicitud del nodo");
	info_nodo->socket_FS_nodo = socket;
	list_add(list_info_nodo, info_nodo);

	return result;
}

//---------------------------------------------------------------------------
int estaActivoNodo(int ID_nodo) {
	int self;

	int _esta_activo(struct t_nodo* nodo) {
		return (ID_nodo == nodo->id_nodo) && (nodo->estado);
	}
	pthread_mutex_lock(&mutex_listaNodos);
	self = list_any_satisfy(listaNodos, (void*) _esta_activo);
	pthread_mutex_unlock(&mutex_listaNodos);

	return self;
}

//---------------------------------------------------------------------------
int bloqueActivo(struct t_bloque* block) {
	int _esta_nodo_activo(struct t_copia_bloq* copy){
		return estaActivoNodo(copy->id_nodo);
	}
	return list_any_satisfy(block->list_copias, (void*) _esta_nodo_activo);
}

//---------------------------------------------------------------------------
int todosLosBloquesDelArchivoDisponibles(struct t_arch* archivo) {
	return list_all_satisfy(archivo->bloques, (void*) bloqueActivo);
}

//---------------------------------------------------------------------------
int ningunBloqueBorrado(struct t_arch* archivo) {
	return archivo->cant_bloq == list_size(archivo->bloques);
}

//---------------------------------------------------------------------------
int estaDisponibleElArchivo(struct t_arch* archivo) {
	return todosLosBloquesDelArchivoDisponibles(archivo);
//			&& ningunBloqueBorrado(archivo);
}

//---------------------------------------------------------------------------
void levantar_arch_conf() {

	char** properties = string_split("PUERTO_LISTEN,MIN_CANT_NODOS", ",");
	t_config* conf_arch = config_create("fs.cfg");

	if (has_all_properties(2, properties, conf_arch)) {
		conf.puerto_listen = config_get_int_value(conf_arch,properties[0]);
		conf.min_cant_nodos = config_get_int_value(conf_arch,properties[1]);
	} else {
		log_error(logger, "Faltan propiedades en archivo de Configuración");
		exit(-1);
	}
	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------
void preparar_fs () {
	iniciarMongo();
	set_root();
	levantarNodos(listaNodos);
	set_kbitarrays_de_nodos();
}

//---------------------------------------------------------------------------
void set_root(){
	root = levantarRaizDeMongo();
	dir_actual = root;
	dir_id_counter = ultimoIdDirectorio()+1;
	arch_id_counter = ultimoIdArchivo()+1;
 }

//---------------------------------------------------------------------------
void levantar_nodo(struct t_copia_bloq* copy){
	struct t_nodo* nodo;
	nodo = find_nodo_with_ID(copy->id_nodo);
	kbitarray_set_bit(nodo->bloquesLlenos, copy->bloq_nodo);
}

//---------------------------------------------------------------------------
void set_kbitarrays_de_nodos(){
	void _levantar_nodos_del_dir(struct t_dir* dir){
		void _levantar_nodos_de_arch(struct t_arch* arch){
			void _levantar_nodos_de_bloq(struct t_bloque* block){
				void _levantar_nodo_de_copia(struct t_copia_bloq* copy){
					levantar_nodo(copy);
				}
				list_iterate(block->list_copias, (void*) _levantar_nodo_de_copia);
			}
			list_iterate(arch->bloques, (void*) _levantar_nodos_de_bloq);
		}
		list_iterate(dir->list_archs, (void*) _levantar_nodos_de_arch);
	}
	foreach_dir_do((void*) _levantar_nodos_del_dir);
}


//---------------------------------------------------------------------------
void foreach_dir_do(void(*closure)(struct t_dir*)){
	foreach_dir_do_starts_from(closure, root);
}

//---------------------------------------------------------------------------
void foreach_dir_do_starts_from(void(*closure)(struct t_dir*), struct t_dir* dir){
	void _apply_closure(struct t_dir* subdir){
		foreach_dir_do_starts_from(closure, subdir);
	}
	list_iterate(dir->list_dirs, (void*) _apply_closure);
	closure(dir);
}

//---------------------------------------------------------------------------
void list_destroy_all_that_satisfy(t_list* list,int(*condition)(void*),void(*destroyer)(void*)){ //XXX Probar si anda bien
	int i = 0;
	int length = list_size(list);
	void* elem;
	while(i<length){
		elem = list_get(list,i);
		if(condition(elem)){
			list_remove(list,i);
			destroyer(elem);
			length--;
		} else {
			i++;
		}
	}

}

//---------------------------------------------------------------------------
void clean_copies_from_nodo(int ID_nodo){
	void _delete_copies_in_dir(struct t_dir* dir){
		void _delete_copies_in_arch(struct t_arch* arch){
			void _delete_copies_in_bloq(struct t_bloque* block){
				int _eq_id(struct t_copia_bloq* copia){
					return copia->id_nodo == ID_nodo;
				}
				void _remove_copy(struct t_copia_bloq* copy){
					eliminarCopiaBloque(arch,block->nro_bloq,copy);
					copia_bloque_destroy(copy);
				}
				list_destroy_all_that_satisfy(block->list_copias,(void*) _eq_id,(void*) _remove_copy);
			}
			list_iterate(arch->bloques, (void*) _delete_copies_in_bloq);
		}
		list_iterate(dir->list_archs, (void*) _delete_copies_in_arch);
	}
	foreach_dir_do((void*) _delete_copies_in_dir);
}

//---------------------------------------------------------------------------
int save_result(char* path_result, char* src_name, int ID_nodo){
	int result = 1;
	t_buffer* read_result_buff;

	struct t_nodo* nodo = find_nodo_with_ID(ID_nodo);
	if (nodo == NULL) {
		log_error(logger, "Nodo no existe");
		return -1;
	}
	read_result_buff = buffer_create_with_protocol(READ_RESULT_JOB);
	buffer_add_string(read_result_buff,src_name);
	result = (result > 0) ? send_buffer_and_destroy(nodo->socket_FS_nodo, read_result_buff) : result;
	result = (result > 0) ? receive_entire_file_by_parts(nodo->socket_FS_nodo, "result.tmp", MAX_PART_SIZE) : result;
	if (result < 0){
		log_error(logger, "Error al recibir archivo result.tmp");
		return -1;
	}

	result = upload("result.tmp",path_result,0);
	if (result < 0) {
		log_error(logger, "Error al hacer el upload del result.tmp");
		return -1;
	}

	result = remove("result.tmp");
	if (result < 0) {
		log_error(logger, "Error al remover archivo result.tmp");
		return -1;
	}

	return 1;
}

							  //FINDERS
//---------------------------------------------------------------------------
struct t_dir* find_dir_with_name(char* name, t_list* list){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,name);
	}
	return list_find(list, (void*) _eq_name);
}

//---------------------------------------------------------------------------
struct t_arch* find_arch_with_name(char* name, t_list* list){
	int _eq_name(struct t_arch* arch){
		return string_equals_ignore_case(arch->nombre,name);
	}
	return list_find(list, (void*) _eq_name);
}

//---------------------------------------------------------------------------
struct t_bloque* find_block_with_num(int num, t_list* list){
	int _eq_num(struct t_bloque* block){
		return block->nro_bloq==num;
	}
	return list_find(list, (void*) _eq_num);
}

//---------------------------------------------------------------------------
struct t_copia_bloq* find_copia_activa(t_list* lista_de_copias){
	int _esta_activa(struct t_copia_bloq* copy){
		return estaActivoNodo(copy->id_nodo);
	}
	return list_find(lista_de_copias, (void*) _esta_activa);
}

//---------------------------------------------------------------------------
struct t_nodo* find_nodo_with_ID(int id){
	int _eq_ID(struct t_nodo* nodo){
		return nodo->id_nodo==id;
	}
	pthread_mutex_lock(&mutex_listaNodos);
	struct t_nodo* self = list_find(listaNodos, (void*) _eq_ID);
	pthread_mutex_unlock(&mutex_listaNodos);
	return self;
}

//---------------------------------------------------------------------------
struct info_nodo* find_infonodo_with_sockfd(int sockfd) {
	int _eq_sock(struct info_nodo* inodo){
		return inodo->socket_FS_nodo == sockfd;
	}
	return list_find(list_info_nodo, (void*) _eq_sock);
}

//---------------------------------------------------------------------------
struct t_nodo* find_nodo_with_sockfd(int sockfd) {
	int _eq_sock(struct t_nodo* nodo){
		return nodo->socket_FS_nodo == sockfd;
	}

	pthread_mutex_lock(&mutex_listaNodos);
	struct t_nodo* self = list_find(listaNodos, (void*) _eq_sock);
	pthread_mutex_unlock(&mutex_listaNodos);
	return self;
}

//---------------------------------------------------------------------------
int any_dir_with_name(char* name, t_list* list){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,name);
	}
	return list_any_satisfy(list, (void*) _eq_name);
}

//---------------------------------------------------------------------------
int any_arch_with_name(char* name, t_list* list){
	int _eq_name(struct t_arch* arch){
		return string_equals_ignore_case(arch->nombre,name);
	}
	return list_any_satisfy(list, (void*) _eq_name);
}

//---------------------------------------------------------------------------
int any_nodo_with_ID(int id){
	int _eq_ID(struct t_nodo* nodo){
		return nodo->id_nodo==id;
	}
	pthread_mutex_lock(&mutex_listaNodos);
	int result = list_any_satisfy(listaNodos, (void*) _eq_ID);
	pthread_mutex_unlock(&mutex_listaNodos);
	return result;
}

//---------------------------------------------------------------------------
int any_info_nodo_with_ID(int id){
	int _eq_ID(struct info_nodo* infonodo){
		return infonodo->id==id;
	}
	return list_any_satisfy(list_info_nodo, (void*) _eq_ID);
}

//---------------------------------------------------------------------------
int any_block_with_num(int num, t_list* list){
	int _eq_num(struct t_bloque* block){
		return block->nro_bloq==num;
	}
	return list_any_satisfy(list, (void*) _eq_num);
}

//---------------------------------------------------------------------------
int any_infonodo_with_sockfd(int sockfd) {
	int _eq_sock(struct info_nodo* inodo){
		return inodo->socket_FS_nodo == sockfd;
	}
	return list_any_satisfy(list_info_nodo, (void*) _eq_sock);
}

							 //DESTROYERS
//---------------------------------------------------------------------------
void info_nodo_destroy(struct info_nodo* self){
	free(self);
}

//---------------------------------------------------------------------------
void nodo_destroy(struct t_nodo* self){
	pthread_mutex_destroy(&self->mutex_socket);
	kbitarray_destroy(self->bloquesLlenos);
	free(self);
}

//---------------------------------------------------------------------------
void nodo_remove(struct t_nodo* self){
	clean_copies_from_nodo(self->id_nodo);
	eliminarNodo(self);
	pthread_mutex_lock(&mutex_listaNodos);
	nodo_destroy(self);
	pthread_mutex_unlock(&mutex_listaNodos);
}

//---------------------------------------------------------------------------
void copia_bloque_destroy(struct t_copia_bloq* self){
	struct t_nodo* nodo;
	nodo = find_nodo_with_ID(self->id_nodo);
	if(nodo!=NULL){
		kbitarray_clean_bit(nodo->bloquesLlenos,self->bloq_nodo);
	}
	free(self);
}

//---------------------------------------------------------------------------
void bloque_destroy(struct t_bloque* self){
	list_destroy_and_destroy_elements(self->list_copias, (void*) copia_bloque_destroy);
	free(self);
}

//---------------------------------------------------------------------------
void arch_destroy(struct t_arch* self){
	list_destroy_and_destroy_elements(self->bloques, (void*) bloque_destroy);
	free(self->nombre);
	free(self);
}

//---------------------------------------------------------------------------
void dir_destroy(struct t_dir* self){
	list_destroy_and_destroy_elements(self->list_dirs, (void*) dir_destroy);
	list_destroy_and_destroy_elements(self->list_archs, (void*) arch_destroy);
	free(self->nombre);
	free(self);
}

//---------------------------------------------------------------------------
void* list_remove_elem(t_list* list, void* elem){
	int _eq_ptr(void* something){
		return something==elem;
	}
	return list_remove_by_condition(list, (void*) _eq_ptr);
}

//---------------------------------------------------------------------------
void list_destroy_elem(t_list* list, void* elem, void* elem_destroyer){
	int _eq_ptr(void* something){
		return something==elem;
	}
	list_remove_and_destroy_by_condition(list, (void*) _eq_ptr, elem_destroyer);
}

						//MANEJO DE ARCHIVOS
//---------------------------------------------------------------------------
int has_disp_block(struct t_nodo* nodo){
	int blocks_disp;
	blocks_disp = kbitarray_amount_bits_clean(nodo->bloquesLlenos);
	return blocks_disp > 0;
}

//---------------------------------------------------------------------------
int get_nodo_disp(t_list* list_used, struct t_nodo** the_choosen_one, int* index_set){ //Devuelve el nodo que tenga espacio disponible y no este en la lista de usados
	t_list* filtered_list;
	int _disp_and_not_used(struct t_nodo* nodo){
		return (nodo->estado==CONECTADO) && has_disp_block(nodo) && !contains(nodo, list_used);
	}
	pthread_mutex_lock(&mutex_listaNodos);
	filtered_list = list_filter(listaNodos, (void*) _disp_and_not_used);
	pthread_mutex_unlock(&mutex_listaNodos);

	int _by_more_free_space(struct t_nodo* n1, struct t_nodo* n2){
		int amount_set_n1 = kbitarray_amount_bits_set(n1->bloquesLlenos);
		int amount_set_n2 = kbitarray_amount_bits_set(n2->bloquesLlenos);
		return amount_set_n1 < amount_set_n2;
	}
	list_sort(filtered_list, (void*) _by_more_free_space);
	*the_choosen_one = list_get(filtered_list,0);
	list_destroy(filtered_list);
	if(*the_choosen_one == NULL) return -1;
	*index_set = kbitarray_find_first_clean((*the_choosen_one)->bloquesLlenos);
	kbitarray_set_bit((*the_choosen_one)->bloquesLlenos,*index_set);
	return 0;
}



//---------------------------------------------------------------------------
int dividir_int(int numerador, int denominador){
	if (numerador % denominador){
		return div(numerador,denominador).quot + 1;
	} else {
		return div(numerador,denominador).quot;
	}
}

//---------------------------------------------------------------------------
int first_free_block(struct t_nodo* nodo){
	return kbitarray_find_first_clean(nodo->bloquesLlenos);
}

//---------------------------------------------------------------------------
int send_block(char* data, struct t_nodo* nodo, int index_set, int block_start, int block_end) {
	int result;
	int socket_nodo = nodo->socket_FS_nodo;

	t_buffer* write_block_buff = buffer_create_with_protocol(WRITE_BLOCK);
	buffer_add_int(write_block_buff, index_set);
	result = send_buffer_and_destroy(socket_nodo,write_block_buff);
	result = (result > 0) ? send_stream_with_size_in_order(socket_nodo, &data[block_start], block_end-block_start+1) : result;
	return result;
}

//---------------------------------------------------------------------------
int recv_block(char** data, struct t_nodo* nodo, int index_set) {
	int result;
	int socket_nodo = nodo->socket_FS_nodo;

	t_buffer* read_block_buff = buffer_create_with_protocol(READ_BLOCK);
	buffer_add_int(read_block_buff, index_set);
	pthread_mutex_lock(&nodo->mutex_socket);
	nodo->usando_socket = 1;
	result = send_buffer_and_destroy(socket_nodo,read_block_buff);
	result = (result > 0) ? receive_dinamic_array_in_order(socket_nodo, (void**) data) : result;
	pthread_mutex_unlock(&nodo->mutex_socket);
	return result;
}

//---------------------------------------------------------------------------
int send_all_blocks(char* data, int* blocks_sent, t_list** list_blocks){
	struct t_nodo* nodo_disp;
	int i, fin = 0, index_set;
	int data_last_index = string_length(data)-1,
		block_start = 0,
		block_end;
	t_list* list_used = list_create();
	struct t_bloque *block;
	struct t_copia_bloq* copy_block;

	*blocks_sent = 0;
	while(!fin){
		block = malloc(sizeof(struct t_bloque));
		block->list_copias = list_create();
		block_end = block_start + BLOCK_SIZE -2;
		if(block_end > data_last_index){
			block_end = data_last_index;
			fin = 1;
		}
		while(data[block_end]!='\n') block_end--;
		for(i=0;i<CANT_COPIAS;i++){
			if(get_nodo_disp(list_used, &nodo_disp, &index_set)==-1){
				puts("\nno hay suficientes nodos disponibles para mandar el archivo");
				log_error(logger,"No hay suficientes nodos disponibles para mandar el archivo");
				list_destroy(list_used);
				list_add(*list_blocks,block);
				return -1;
			}
			if (send_block(data,nodo_disp,index_set,block_start,block_end)<=0) {
				log_error(logger,"Error al enviar bloque");
				list_destroy(list_used);
				list_add(*list_blocks,block);
				return -1;
			}
			list_add(list_used, nodo_disp);
			copy_block = malloc(sizeof(struct t_copia_bloq));
				copy_block->id_nodo = nodo_disp->id_nodo;
				copy_block->bloq_nodo = index_set;
			list_add(block->list_copias,copy_block);
		}
		list_clean(list_used);
		block->nro_bloq = *blocks_sent;
		list_add(*list_blocks,block);
		(*blocks_sent)++;
		block_start = block_end + 1;
	}
	list_destroy(list_used);
	return 0;
}

//---------------------------------------------------------------------------
int rebuild_arch(struct t_arch* arch, int local_fd){
	struct t_copia_bloq* copy;
	struct t_bloque* block;
	struct t_nodo* nodo;
	char* data;
	int i, data_size;
	for(i=0;i<arch->cant_bloq;i++) {
			block = list_get(arch->bloques,i);
			copy = find_copia_activa(block->list_copias);
			nodo = find_nodo_with_ID(copy->id_nodo);
			data_size = recv_block(&data,nodo,copy->bloq_nodo);
			if(data_size !=-1 ){
				if ((write(local_fd, data, data_size-1)) == -1){
					log_error(logger,"Error al escribir archivo");
					perror("error al escribir archivo");
					free(data);
					return -1;
				}
				free(data);
			} else {
				log_error(logger,"Error al recibir bloque");
				free(data);
				return -1;
			}
	}
	return 0;
}

//---------------------------------------------------------------------------
int copy_block(struct t_bloque* block, struct t_arch* arch){
	t_list* list_used;
	struct t_nodo* recv_nodo,
				 * send_nodo;
	struct t_copia_bloq* copy_to_copy,
					   * copied_copy;
	int index_set;
	char* data;

	copy_to_copy = find_copia_activa(block->list_copias);
	if(copy_to_copy==NULL){
		log_error(logger,"Error: no hay ningun nodo disponible para pedir el bloque a copiar");
		puts("\nerror: no hay ningun nodo disponible para pedir el bloque a copiar");
		return -1;
	}
	recv_nodo = find_nodo_with_ID(copy_to_copy->id_nodo);
	if(recv_block(&data,recv_nodo,copy_to_copy->bloq_nodo)==-1) {
		log_error(logger,"Error al recibir bloque");
		return -1;
	}
	int _nodo_used(struct t_nodo* nodo){
		int i, success=0;
		struct t_copia_bloq* copy;
		for(i=0;i<list_size(block->list_copias);i++){
			copy = list_get(block->list_copias,i);
			if(copy->id_nodo==nodo->id_nodo) success = 1;
		}
		if(success) { return 1; }
		else { return 0; }
	}
	pthread_mutex_lock(&mutex_listaNodos);
	list_used = list_filter(listaNodos, (void*) _nodo_used);
	pthread_mutex_unlock(&mutex_listaNodos);
	if(get_nodo_disp(list_used,&send_nodo,&index_set)==-1) {
		list_destroy(list_used);
		free(data);
		log_error(logger,"Error: no hay nigun nodo disponible donde no este la copia actualmente");
		puts("\nerror: no hay nigun nodo disponible donde no este la copia actualmente");
		return -1;
	}
	if(send_block(data,send_nodo,index_set,0,string_length(data)-1)<=0){
		log_error(logger,"Error al enviar el bloque");
		list_destroy(list_used);
		free(data);
		return -1;
	}
	copied_copy = malloc(sizeof(struct t_copia_bloq));
		copied_copy->id_nodo = send_nodo->id_nodo;
		copied_copy->bloq_nodo = index_set;
	list_add(block->list_copias,copied_copy);
	copiarBloque(arch,block->nro_bloq,copied_copy);
	list_destroy(list_used);
	free(data);
	return 0;
}

						//CONSOLA Y COMANDOS
//---------------------------------------------------------------------------
void print_path_actual(){
	char* path = string_new();
	char* aux;
	struct t_dir* dir = dir_actual;
	do {
		aux = string_new();
		string_append(&aux,"/");
		string_append(&aux,dir->nombre);
		string_append(&aux,path);
		free(path);
		path = string_duplicate(aux);
		free(aux);
		dir = dir->parent_dir;
	} while(dir!=NULL);
	printf("%s",path);
	free(path);
}

//---------------------------------------------------------------------------
int warning(char* message){
	printf("%s (S/n): ",message);
	char readed[MAX_COMMAND_LENGTH+1];
	fgets(readed,MAX_COMMAND_LENGTH+1,stdin);
	if(readed[1]=='\n') readed[1]='\0';
	if(string_equals_ignore_case(readed,"s")){
		return 1;
	} else {
		return 0;
	}

}

//---------------------------------------------------------------------------
struct t_arch* arch_create(char* arch_name, struct t_dir* parent_dir, int blocks_sent, t_list* list_blocks){;
	struct t_arch* new_arch;
	new_arch = malloc(sizeof(struct t_arch));
		new_arch->nombre = arch_name;
		new_arch->parent_dir = parent_dir;
		new_arch->id_archivo = arch_id_counter;
		new_arch->cant_bloq = blocks_sent;
		new_arch->bloques = list_blocks;
		crearArchivoEn(new_arch,new_arch->parent_dir);
	arch_id_counter++;
	log_info(logger,"Se crea el archivo %s",arch_name);
	return new_arch;
}

//---------------------------------------------------------------------------
struct t_dir* dir_create(char* dir_name, struct t_dir* parent_dir){;
	struct t_dir* new_dir;
	new_dir = malloc(sizeof(struct t_dir));
		new_dir->id_directorio = dir_id_counter;
		new_dir->nombre = dir_name;
		new_dir->parent_dir = parent_dir;
		new_dir->list_dirs = list_create();
		new_dir->list_archs = list_create();
	crearDirectorioEn(new_dir,new_dir->parent_dir);
	dir_id_counter++;
	log_info(logger,"Se crea el directorio %s",dir_name);
	return new_dir;
}

//---------------------------------------------------------------------------
int dir_is_empty(struct t_dir* dir){
	if( list_size(dir->list_dirs) || list_size(dir->list_archs) ){
		return 0;
	} else {
		return 1;
	}
}

//---------------------------------------------------------------------------
void validate_arch_name(char** name, struct t_dir* parent_dir){
	int i;
	int OK = 0;
	char* aux_name;
	if(!any_arch_with_name(*name,parent_dir->list_archs)) OK=1;
	for (i=1;!OK;i++){
		aux_name = strdup(*name);
		string_append_with_format(&aux_name,"-%d",i);
		if(!any_arch_with_name(aux_name,parent_dir->list_archs)) {
			string_append_with_format(name,"-%d",i);
			OK=1;
		}
		free(aux_name);
	}
}

//---------------------------------------------------------------------------
int is_valid_dir_name(char* name, struct t_dir* parent_dir){
	if(string_equals_ignore_case(name,"..") ||
	   any_dir_with_name(name,parent_dir->list_dirs)) {
		return 0;
	}
	return 1;
}

//---------------------------------------------------------------------------
void arch_move(struct t_arch** arch, struct t_dir* parent_dir){
	int _eq_name(struct t_arch* archs){
		return string_equals_ignore_case(archs->nombre,(*arch)->nombre);
	}
	list_remove_by_condition((*arch)->parent_dir->list_archs, (void*) _eq_name);
	(*arch)->parent_dir = parent_dir;
	list_add(parent_dir->list_archs, *arch);
}

//---------------------------------------------------------------------------
void dir_move(struct t_dir** dir, struct t_dir* parent_dir){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,(*dir)->nombre);
	}
	list_remove_by_condition((*dir)->parent_dir->list_dirs, (void*) _eq_name);
	(*dir)->parent_dir = parent_dir;
	list_add(parent_dir->list_dirs, *dir);
}

//---------------------------------------------------------------------------
struct t_arch* get_arch_from_path(char* path){ //Si hay error devuelve NULL
	char* arch_name;
	struct t_dir* dir;
	struct t_arch* arch;
	get_info_from_path(path, &arch_name, &dir);
	if(dir!=NULL) {
		if (any_arch_with_name(arch_name, dir->list_archs)) {
			arch = find_arch_with_name(arch_name, dir->list_archs);
		} else {
			free(arch_name);
			return NULL;
		}
	} else {
		free(arch_name);
		return NULL;
	}
	free(arch_name);
	return arch;
}

//---------------------------------------------------------------------------
struct t_dir* get_dir_from_path(char* path){ //Si hay error devuelve NULL
	char* dir_name;
	struct t_dir *dir;
	get_info_from_path(path, &dir_name, &dir);
	if(dir!=NULL) {
		if(string_equals_ignore_case(dir_name,"..")) {
			if(dir!=root) dir = dir->parent_dir;
		} else if (string_equals_ignore_case(dir_name,"root")) {
			dir = root;
		} else if (any_dir_with_name(dir_name, dir->list_dirs)) {
			dir = find_dir_with_name(dir_name, dir->list_dirs);
		}else {
			free(dir_name);
			return NULL;
		}
	} else {
		free(dir_name);
		return NULL;
	}
	free(dir_name);
	return dir;
}

//---------------------------------------------------------------------------
void get_info_from_path(char* path, char** name, struct t_dir** parent_dir){ //Si har error: parent_dir = NULL

	char** sub_dirs;
	if(path[0]=='/') {
		*parent_dir = root;
		if(strlen(path+1) == 0) {
			*name = strdup("root");
			return;
		}
		sub_dirs = string_split(path+1,"/"); //XXX Luego hacer mejor solucion... se se se... deja de flashar
	} else {
		*parent_dir = dir_actual;
		sub_dirs = string_split(path,"/");
	}

	int last_index = string_split_size(sub_dirs)-1;
	*name = string_duplicate(sub_dirs[last_index]);


	t_list* list_dirs;
	int i, error = 0;
	for(i=0;i<last_index && !error;i++){
		list_dirs = (*parent_dir)->list_dirs;
		if(string_equals_ignore_case(sub_dirs[i],"..") ) {
			if(*parent_dir!=root) *parent_dir = (*parent_dir)->parent_dir;
		}  else if (any_dir_with_name(sub_dirs[i], list_dirs)) {
			*parent_dir = find_dir_with_name(sub_dirs[i], list_dirs);
		} else {
			*parent_dir = NULL;
			error = 1;
		}
	}

	free_string_splits(sub_dirs);
}

//---------------------------------------------------------------------------
void receive_command(char* readed, int max_command_length){
	fgets(readed,max_command_length,stdin);
	readed[strlen(readed)-1] = '\0';
}

//---------------------------------------------------------------------------

char execute_command(char* command){

	char comandos_validos[MAX_COMMANOS_VALIDOS][16]={"help","format","pwd","ls","cd","rm","mv","mkdir","rmdir","mvdir",
													 "upload","download","md5","blocks","rmblock","cpblock","lsrequest",
													 "lsnode","addnode","rmnode", "clear","exit","","","","","",""};
	int i,salir=0;
	string_static_trim(command);
	if(command[0]=='\0')
	{	return 0;}
	char** subcommands = string_split(command, " ");
	for (i = 0; (i < MAX_COMMANOS_VALIDOS) && (strcmp(subcommands[0], comandos_validos[i]) != 0); i++);

	switch (i) {
		case  0: help(); break;
		case  1: format(); break;
		case  2: pwd(); break;
		case  3: ls(); break;
		case  4: cd(subcommands[1]); break;
		case  5: rm(subcommands[1]); break;
		case  6: mv(subcommands[1],subcommands[2]); break;

		case  7: makedir(subcommands[1]); break;
		case  8: remdir(subcommands[1]); break;
		case  9: mvdir(subcommands[1],subcommands[2]); break;

		case 10: upload(subcommands[1],subcommands[2],1); break;
		case 11: download(subcommands[1],subcommands[2],1); break;
		case 12: md5(subcommands[1]); break;

		case 13: blocks(subcommands[1]); break;
		case 14: if(subcommands[2]==NULL) rmblock(subcommands[1],NULL,NULL);
				 else rmblock(subcommands[1],subcommands[2],subcommands[3]);
				 break;
		case 15: cpblock(subcommands[1],subcommands[2]); break;

		case 16: lsrequest(); break;
		case 17: lsnode(); break;
		case 18: addnode(subcommands[1]); break;
		case 19: rmnode(subcommands[1]); break;

		case 20: printf(CLEAR); break;
		case 21: salir=1; break;

	default:
		printf("%s: no es un comando valido\n", subcommands[0]);
		break;
	}

	free_string_splits(subcommands);
	return salir;
}

//---------------------------------------------------------------------------
void help(){
	puts(BOLD" help"NORMAL" -> Muestra los comandos validos.");
	puts(BOLD" format"NORMAL" -> Formatea el MDFS.");
	puts(BOLD" pwd"NORMAL" -> Indica la ubicacion actual.");
	puts(BOLD" ls"NORMAL" -> Lista los directorios y archivos dentro del directorio actual.");
	puts(BOLD" cd (path dir)"NORMAL" -> Mueve la ubicacion actual al directorio (path dir).");
	puts(BOLD" rm (path file)"NORMAL" -> Elimina el archivo (path file).");
	puts(BOLD" mv (old path file) (new path file)"NORMAL" -> Mueve el archivo de (old path file) a (new path file).");
	puts(BOLD" mkdir (dir name)"NORMAL" -> Crea el directorio (dir name).");
	puts(BOLD" rmdir (path dir)"NORMAL" -> Elimina el directorio (path dir).");
	puts(BOLD" mvdir (old path dir) (new path dir)"NORMAL" -> Mueve el directorio de (old path dir) a (new path dir).");
	puts(BOLD" upload (local path file) (MDFS path file)"NORMAL" -> Copia el archivo del filesystem local (local path file) al MDFS (MDFS path file).");
	puts(BOLD" download (MDFS path file) (local path file)"NORMAL" -> Copia el archivo del MDFS (MDFS path file) al filesystem local (local path file).");
	puts(BOLD" md5 (path file)"NORMAL" -> Solicita el MD5 del archivo path (file).");
	puts(BOLD" blocks (path file)"NORMAL" -> Muestra los bloques que componen el archivo (path file).");
	puts(BOLD" rmblock (path file) (num block)"NORMAL" -> Elimina el bloque nro (num block) del archivo (path file).");
	puts(BOLD" cpblock (path file) (num block)"NORMAL" -> Copia el bloque nro (num block) del archivo (path file) en un nodo disponoble.");
	puts(BOLD" lsrequest"NORMAL" -> Lista los nodos que solicitan agregarse.");
	puts(BOLD" lsnode"NORMAL" -> Lista los nodos actualmente disponible.");
	puts(BOLD" addnode (node)"NORMAL" -> Agrega el nodo de datos (node).");
	puts(BOLD" rmnode (node)"NORMAL" -> Elimina el nodo de datos (node).");
	puts(BOLD" clear"NORMAL" -> Limpiar la pantalla");
	puts(BOLD" exit"NORMAL" -> Salir del MDSF");
}

//---------------------------------------------------------------------------
void format(){
	if (warning("Esta seguro que desea formatear el MDFS?")) {
		eliminarDirectorio(root);
		dir_destroy(root);
		set_root();
		int _is_disconnected(struct t_nodo* nodo){
			return !nodo->estado;
		}
		list_destroy_all_that_satisfy(listaNodos, (void*) _is_disconnected, (void*) nodo_remove);
		log_info(logger,"Se FORMATEA el MDFS");
	}
}

//---------------------------------------------------------------------------
void pwd(){
	print_path_actual();
	printf("\n");
}

//---------------------------------------------------------------------------
void ls(){
	int empty = 1;
	//Print Dirs
	int _dir_orden_alfabetico(struct t_dir* menor, struct t_dir* mayor){
		return strcmp(menor->nombre,mayor->nombre)==-1;
	}
	list_sort(dir_actual->list_dirs, (void*) _dir_orden_alfabetico);
	void _print_dir(struct t_dir* dir){
		printf(BLUE"%s  "NORMAL,dir->nombre);
		empty = 0;
	}
	list_iterate(dir_actual->list_dirs, (void*) _print_dir);
	//Print Archs
	int _arch_orden_alfabetico(struct t_dir* menor, struct t_dir* mayor){
		return strcmp(menor->nombre,mayor->nombre)==-1;
	}
	list_sort(dir_actual->list_dirs, (void*) _arch_orden_alfabetico);
	void _print_arch(struct t_arch* arch){
		printf("%s  ",arch->nombre);
		empty = 0;
	}
	list_iterate(dir_actual->list_archs, (void*) _print_arch);
	if (!empty) puts("");
}

//---------------------------------------------------------------------------
void cd(char* dir_path){
	struct t_dir* dir_aux;
	if(dir_path==NULL){
		dir_actual = root;
	} else if((dir_aux = get_dir_from_path(dir_path))!=NULL) {
		dir_actual = dir_aux;
	} else {
		printf("%s: el directorio no existe\n",dir_path);
	}
}

//---------------------------------------------------------------------------
void rm(char* arch_path){
	struct t_arch* arch_aux;
	if(arch_path==NULL){
		puts("rm: falta un operando");
	} else if((arch_aux = get_arch_from_path(arch_path))!=NULL) {
		int _eq_name(struct t_arch* arch){
			return string_equals_ignore_case(arch->nombre,arch_aux->nombre);
		}
		list_remove_by_condition(arch_aux->parent_dir->list_archs, (void*) _eq_name);
		log_info(logger,"Se ELIMINA el archivo %s",arch_path);
		eliminarArchivo(arch_aux);
		arch_destroy(arch_aux);
	} else {
		printf("%s: el archivo no existe\n", arch_path);
	}
}

//---------------------------------------------------------------------------
void mv(char* old_path, char* new_path) {
	struct t_dir *parent_dir_aux;
	struct t_arch *arch_aux;
	char *arch_name,
		 *aux_name;
	if(old_path==NULL || new_path==NULL){
		puts("mv: falta un archivo como operando");
	} else if((arch_aux = get_arch_from_path(old_path))!=NULL) {
		get_info_from_path(new_path, &arch_name, &parent_dir_aux);
		if(parent_dir_aux!=NULL) {
			validate_arch_name(&arch_name, parent_dir_aux);
			log_info(logger,"Se MUEVE el archivo %s a %s",old_path,new_path);
			moverArchivo(arch_aux,parent_dir_aux,arch_name);
			arch_move(&arch_aux, parent_dir_aux);
			aux_name = arch_aux->nombre;
			arch_aux->nombre = arch_name;
			free(aux_name);
		} else {
			printf("%s: el archivo no existe\n",new_path);
		}
	} else {
		printf("%s: el archivo no existe\n",old_path);
	}
}

//---------------------------------------------------------------------------
void makedir(char* dir_path){
	if (dir_path==NULL) {
		puts("mkdir: falta un operando");
	} else {
		char* dir_name;
		struct t_dir* parent_dir;
		get_info_from_path(dir_path, &dir_name, &parent_dir);
		if(parent_dir!=NULL) {
			if(is_valid_dir_name(dir_name, parent_dir)) {
				log_info(logger,"Se CREA el directorio %s",dir_path);
				list_add(parent_dir->list_dirs, dir_create(dir_name,parent_dir));
			} else {
				printf("%s: no es un nombre valido\n",dir_name);
				free(dir_name);
			}
		} else {
			printf("%s: el directorio no existe\n",dir_path);
			free(dir_name);
		}
	}
}

//---------------------------------------------------------------------------
void remdir(char* dir_path){
	struct t_dir* dir_aux;
	if(dir_path==NULL){
		puts("rmdir: falta un operando");
	} else if((dir_aux = get_dir_from_path(dir_path))!=NULL) {
		int _eq_name(struct t_dir* direc){
			return string_equals_ignore_case(direc->nombre,dir_aux->nombre);
		}
		if(dir_is_empty(dir_aux)) {
			log_info(logger,"Se ELIMINA el directorio %s", dir_path);
			list_remove_by_condition(dir_aux->parent_dir->list_dirs, (void*) _eq_name);
			eliminarDirectorio(dir_aux);
			dir_destroy(dir_aux);
		} else if(warning("El directorio no esta vacio, desea eliminarlo de todas formas?")) {
			log_info(logger,"Se ELIMINA el directorio %s", dir_path);
			list_remove_by_condition(dir_aux->parent_dir->list_dirs, (void*) _eq_name);
			eliminarDirectorio(dir_aux);
			dir_destroy(dir_aux);
		}
	} else {
		printf("%s: el directorio no existe\n", dir_path);
	}
}

//---------------------------------------------------------------------------
void mvdir(char* old_path, char* new_path){
	struct t_dir *dir_aux,
				 *parent_dir_aux;
	char *dir_name,
		 *aux_name;
	if(old_path==NULL || new_path==NULL){
		puts("mvdir: falta un directorio como operando");
	} else if((dir_aux = get_dir_from_path(old_path))!=NULL) {
		get_info_from_path(new_path, &dir_name, &parent_dir_aux);
		if(parent_dir_aux!=NULL) {
			if(is_valid_dir_name(dir_name, parent_dir_aux)) {
				log_info(logger,"Se MUEVE el directorio de %s a %s", old_path, new_path);
				moverDirectorio(dir_aux,parent_dir_aux,dir_name);
				dir_move(&dir_aux, parent_dir_aux);
				aux_name = dir_aux->nombre;
				dir_aux->nombre = dir_name;
				free(aux_name);
			} else {
				printf("%s: no es un nombre valido\n",dir_name);
				free(dir_name);
			}
		} else {
			printf("%s: el directorio no existe\n",new_path);
		}
	} else {
		printf("%s: el directorio no existe\n",old_path);
	}
}

//---------------------------------------------------------------------------
int upload(char* local_path, char* mdfs_path, int is_console){
	int local_fd, blocks_sent =0, send_ok;
	struct stat file_stat;
	char *data, *arch_name;
	struct t_dir *parent_dir;
	t_list* list_blocks;

	if(local_path!=NULL && mdfs_path!=NULL) {
		if ((local_fd = open(local_path, O_RDONLY)) != -1) {
			if(is_console) printf("Procesando... ");
			fflush(stdout);
			fstat(local_fd, &file_stat);
			data = mmap((caddr_t) 0, file_stat.st_size, PROT_READ, MAP_FILE|MAP_PRIVATE|MAP_NORESERVE, local_fd, OFFSET);
			if (data == (caddr_t)(-1)) {
				log_error(logger,"Error al mappear el archivo %s",local_path);
				perror("mmap");
				return -1;
			}
			get_info_from_path(mdfs_path, &arch_name, &parent_dir);
			if(parent_dir!=NULL) {
				list_blocks = list_create();
				send_ok = send_all_blocks(data,&blocks_sent, &list_blocks);
				if(munmap(data,file_stat.st_size)==-1){
					log_error(logger,"Error al munmappear el archivo %s",local_path);
					perror("munmap");
					return -1;
				}
				if (close(local_fd) == -1){
					log_error(logger,"Error al cerrar el archivo %s",local_path);
					perror("close");
					return -1;
				}
				if(send_ok!=-1){
					validate_arch_name(&arch_name, parent_dir);
					list_add(parent_dir->list_archs, arch_create(arch_name,parent_dir,blocks_sent,list_blocks));
					if(is_console) printf("OK\n");
					log_info(logger,"El archivo %s se subio correctamente", arch_name);
				} else {
					log_error(logger,"Error al enviar archivo %s",local_path);
					list_destroy_and_destroy_elements(list_blocks, (void*) bloque_destroy);
					return -1;
				}
			} else {
				if(is_console) printf("%s: el directorio no existe\n",mdfs_path);
				free(arch_name);
				list_destroy_and_destroy_elements(list_blocks, (void*) bloque_destroy);
				return -1;
			}
		} else {
			if(is_console) perror("error: no se pudo abrir el archivo");
			log_error(logger,"Error al abrir el archivo %s",local_path);
			return -1;
		}
	} else {
		if(is_console) puts("upload: falta un operando");
		return -1;
	}
	return 1;
}

//---------------------------------------------------------------------------
int download(char* mdfs_path, char* local_path, int is_console){
	struct t_arch* arch_aux;
	int local_fd;
	if(mdfs_path==NULL || local_path==NULL){
		puts("download: falta un operando");
		return -1;
	} else if((arch_aux = get_arch_from_path(mdfs_path))!=NULL) {
		if(estaDisponibleElArchivo(arch_aux)){
			if ((local_fd = open(local_path, O_RDWR |  O_CREAT, S_IRWXU | S_IRWXO )) != -1) {
				if(is_console) printf("Procesando... ");

				if (rebuild_arch(arch_aux,local_fd)!=-1) {
					if(is_console) printf("OK\n");
					log_info(logger,"El archivo %s se descargo correctamente",mdfs_path);
				} else {
					if(is_console) printf("ERROR\n");
					log_error(logger,"Error al reconstruir el archivo %s",mdfs_path);
				}

				if (close(local_fd) == -1){
					if(is_console)	perror("close");
					log_error(logger,"Error al cerrar el archivo %s", local_path);
					return -1;
				}
			} else {
				perror("error al crear el archivo");
				log_error(logger,"Error al crear el archivo %s", local_path);
				return -1;
			}
		} else {
			printf("%s: el archivo no se encuentra disponible, verifique que los nodos necesarios esten conectados\n", arch_aux->nombre);
			log_info(logger,"El archivo %s no se encuentra disponible",mdfs_path);
			return -1;
		}
	} else {
		printf("%s: el archivo no existe\n", mdfs_path);
		return -1;
	}
	return 1;
}

//---------------------------------------------------------------------------
void md5(char* arch_path){
	int result;
	result = download(arch_path,"MD5",0);
	if (result > 0){
		system("md5sum MD5");
		if (remove("MD5") < 0) {
			log_error(logger,"Error al remover MD5");
			perror("remove MD5");
		}
	}
}

//---------------------------------------------------------------------------
void blocks(char* arch_path){
	struct t_arch* arch_aux;
	int i,j;
	if(arch_path==NULL){
		puts("blocks: falta un archivo como operando");
	} else if((arch_aux = get_arch_from_path(arch_path))!=NULL) {
		i=0;
		void _print_block(struct t_bloque* block){
			block = list_get(arch_aux->bloques,i);
			printf(" # Block: %d\n",block->nro_bloq);
			j=0;
			void _print_copies(struct t_copia_bloq* copy){
				printf("  *Copia %d: Nodo %d, Bloque %d\n", j,copy->id_nodo, copy->bloq_nodo);
				j++;
			}
			list_iterate(block->list_copias, (void*) _print_copies);
			i++;
		}
		list_iterate(arch_aux->bloques, (void*) _print_block);
	} else {
		printf("%s: el archivo no existe\n", arch_path);
	}
}

//---------------------------------------------------------------------------
void rmblock(char* arch_path, char* num_block_str, char* num_copy_str){
	int num_block, num_copy;
	struct t_arch* arch_aux;
	struct t_bloque* block;
	if(num_block_str==NULL){
		puts("rmblock: falta un numero de bloque como operando");
	} else if(arch_path==NULL){
		puts("rmblock: falta un archivo como operando");
	} else {
		num_block = strtol(num_block_str, NULL, 10);
		if((arch_aux=get_arch_from_path(arch_path))!=NULL){
			if(any_block_with_num(num_block,arch_aux->bloques)){
				block = find_block_with_num(num_block,arch_aux->bloques);
				void _remove_copy(struct t_copia_bloq* copy){
					eliminarCopiaBloque(arch_aux,block->nro_bloq,copy);
					copia_bloque_destroy(copy);
				}
				if(num_copy_str==NULL){
					log_info(logger,"Se elimina el bloque %s del archivo %s",num_block_str, arch_path);
					list_clean_and_destroy_elements(block->list_copias, (void*) _remove_copy);
				} else {
					num_copy = strtol(num_copy_str, NULL, 10);
					if(num_copy>=0 || num_copy<list_size(block->list_copias)){
						log_info(logger,"Se elimina la copia %s del bloque %s del archivo %s",num_copy_str,num_block_str, arch_path);
						list_remove_and_destroy_element(block->list_copias, num_copy, (void*) _remove_copy);
					} else {
						puts("rmblock: no existe numero de copia dentro del bloque");
					}
				}
			} else {
				puts("rmblock: no hay ningun bloque con ese numero");
			}
		} else {
			printf("%s: el archivo no existe\n",arch_path);
		}
	}
}

//---------------------------------------------------------------------------
void cpblock(char* arch_path, char* num_block_str){
	int num_block;
	struct t_arch* arch_aux;
	struct t_bloque* bloque;
	if(num_block_str==NULL){
		puts("cpblock: falta un numero de bloque como operando");
	} else if(arch_path==NULL){
		puts("cpblock: falta un archivo como operando");
	} else {
		num_block = strtol(num_block_str, NULL, 10);
		if((arch_aux=get_arch_from_path(arch_path))!=NULL){
			int _eq_num_block(struct t_bloque* block){
				return block->nro_bloq==num_block;
			}
			bloque = list_find(arch_aux->bloques, (void*) _eq_num_block);
			if(bloque!=NULL){
				printf("Procesando... ");
				if(copy_block(bloque,arch_aux)!=-1){
					log_info(logger,"Se copia el bloque %s del archivo %s",num_block_str, arch_path);
					printf("OK\n");
				}
			} else {
				printf("%d: no existe en el archivo un bloque con ese numero",num_block);
			}
		} else {
			if(arch_aux)
			printf("%s: el archivo no existe\n",arch_path);
		}
	}
}

//---------------------------------------------------------------------------
void lsrequest() {
	int lsize = list_size(list_info_nodo);
	if (lsize == 0) {
		puts("No hay nodos solicitando conexion :(");
	} else {
		puts("Nodos solicitando conexion:");
	}
	struct info_nodo* ptr_inodo;
	struct sockaddr_in peer;
	socklen_t peer_size = sizeof(struct sockaddr_in);
	int i;
	for (i = 0; i < lsize; i++) {
		ptr_inodo = list_get(list_info_nodo, i);
		getpeername(ptr_inodo->socket_FS_nodo, (struct sockaddr*) &peer, &peer_size);
		printf(" ID: %d\n", ptr_inodo->id);
		printf("  *IP: %s\n", inet_ntoa(peer.sin_addr));
		printf("  *Cantidad de bloques: %d\n", ptr_inodo->cant_bloques);
		if(ptr_inodo->nodo_nuevo)
			printf("  *Nodo nuevo: SI\n");
		else
			printf("  *Nodo nuevo: NO\n");
	}
}

//---------------------------------------------------------------------------
void lsnode() {
	pthread_mutex_lock(&mutex_listaNodos);
	int lsize = list_size(listaNodos);
	pthread_mutex_unlock(&mutex_listaNodos);
	if (lsize == 0) {
		puts("No hay nodos disponibles :/");
	} else {
		puts("Nodos disponibles:");
	}
	struct t_nodo* ptr_nodo;
	struct sockaddr_in peer;
	socklen_t peer_size = sizeof(struct sockaddr_in);
	int i;
	for (i = 0; i < lsize; i++) {
		pthread_mutex_lock(&mutex_listaNodos);
		ptr_nodo = list_get(listaNodos, i);
		pthread_mutex_unlock(&mutex_listaNodos);
		printf(" ID: %d\n", ptr_nodo->id_nodo);
		if (ptr_nodo->estado==CONECTADO){
			getpeername(ptr_nodo->socket_FS_nodo, (struct sockaddr*)&peer, &peer_size);
			printf("  *Estado: CONECTADO\n");
			printf("  *IP: %s\n", inet_ntoa(peer.sin_addr));
		} else {
			printf("  *Estado: DESCONECTADO\n");
		}
		printf("  *Cantidad de bloques: %d\n", ptr_nodo->cantidad_bloques);
		printf("  *Bloques ocupados: %i\n",(int) kbitarray_amount_bits_set(ptr_nodo->bloquesLlenos));
	}
}

//---------------------------------------------------------------------------
void addnode(char* IDstr){
	int node_id;
	struct t_nodo* viejo_nodo;

	if(IDstr!=NULL){
		node_id = strtol(IDstr, NULL, 10);
		int _eq_ID(struct info_nodo* ninf){
			return ninf->id == node_id;
		}
		if(any_info_nodo_with_ID(node_id)){
			struct info_nodo* infnod = list_find(list_info_nodo, (void*) _eq_ID);
			viejo_nodo = find_nodo_with_ID(node_id);
			if(viejo_nodo!=NULL) {
				list_remove_by_condition(list_info_nodo, (void*) _eq_ID);
				if(infnod->nodo_nuevo) {
					puts("ID en uso, intente conectar el nodo con un ID distinto");
				} else {
					pthread_mutex_lock(&mutex_listaNodos);
					viejo_nodo->estado = CONECTADO;
					viejo_nodo->socket_FS_nodo = infnod->socket_FS_nodo;
					viejo_nodo->ip_listen = infnod->ip_listen;
					viejo_nodo->port_listen = infnod->port_listen;
					viejo_nodo->usando_socket = 0;
//					if(infnod->nodo_nuevo) {
//						eliminarNodo(viejo_nodo);
//						clean_copies_from_nodo(node_id);
//						kbitarray_destroy(viejo_nodo->bloquesLlenos);
//						viejo_nodo->cantidad_bloques = infnod->cant_bloques;
//						viejo_nodo->bloquesLlenos = kbitarray_create_and_clean_all(infnod->cant_bloques);
//						crearNodo(viejo_nodo);
//					}
					pthread_mutex_unlock(&mutex_listaNodos);
					log_info(logger,"Se agrega el nodo %s",IDstr);
				}
				info_nodo_destroy(infnod);
			} else {
				list_remove_by_condition(list_info_nodo, (void*) _eq_ID);
				if(infnod->nodo_nuevo){
					struct t_nodo* nuevo_nodo;
					nuevo_nodo = malloc(sizeof(struct t_nodo));
						nuevo_nodo->id_nodo = infnod->id;
						nuevo_nodo->estado = CONECTADO;
						nuevo_nodo->cantidad_bloques = infnod->cant_bloques;
						nuevo_nodo->socket_FS_nodo = infnod->socket_FS_nodo;
						pthread_mutex_init(&(nuevo_nodo->mutex_socket), NULL);
						nuevo_nodo->usando_socket = 0;
						nuevo_nodo->ip_listen = infnod->ip_listen;
						nuevo_nodo->port_listen = infnod->port_listen;
						nuevo_nodo->bloquesLlenos = kbitarray_create_and_clean_all(infnod->cant_bloques);
					pthread_mutex_lock(&mutex_listaNodos);
					list_add(listaNodos, nuevo_nodo);
					pthread_mutex_unlock(&mutex_listaNodos);
					crearNodo(nuevo_nodo);
					log_info(logger,"Se agrega el nodo %s",IDstr);
				} else {
					puts("No existe ningun nodo desconectado con ese ID, conecte el nodo como nuevo");
				}
				info_nodo_destroy(infnod);
			}

		} else {
			printf("%d: no hay ningun nodo con ese ID\n",node_id);
		}
	} else {
		puts("addnode: falta un ID como operando");
	}
}

//---------------------------------------------------------------------------
void rmnode(char* IDstr){
	int ID;
	if(IDstr!=NULL){
		ID = strtol(IDstr, NULL, 10);
		int result = any_nodo_with_ID(ID);
		if(result){
			int _eq_ID(struct t_nodo* nodo){
				return nodo->id_nodo == ID;
			}
			list_remove_and_destroy_by_condition(listaNodos, (void*) _eq_ID, (void*) nodo_remove);
			log_info(logger,"Se ELIMINA el nodo %s",IDstr);
		} else {
			printf("%d: no hay ningun nodo con ese ID\n",ID);
		}
	} else {
		puts("addnode: falta un ID como operando");
	}
}
