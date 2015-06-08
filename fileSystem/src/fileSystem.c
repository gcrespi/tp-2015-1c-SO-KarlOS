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

//Constantes de la consola
#define MAX_COMMANOS_VALIDOS 30
#define MAX_COMMAND_LENGTH 100
#define RED "\e[1;91m"
#define BOLD "\e[0;1m"
#define BLUE "\e[1;94m"
#define NORMAL "\e[0;0m"
#define CLEAR "\033[H\033[J"
#define OFFSET 0
#define BLOCK_SIZE 4*1024 //20*1024*1024
#define CANT_COPIAS 1 // cantidad de copias a enviar a los nodos

//  Estados del nodo
enum t_estado_nodo {
	DESCONECTADO,CONECTADO
};

//Estructura de carpetas del FS (Se persiste)
struct t_dir {
	struct t_dir* parent_dir;
	char* nombre;
	t_list* list_dirs;
	t_list* list_archs;
};

//no se persisten: estado, aceptado
//ya que cuando se cae el FS deben volver a conectar
struct t_nodo {
	//numero unico de identificacion de cada nodo
	uint32_t id_nodo;

	//file descriptor del socket del nodo
	uint32_t socket_FS_nodo;

	//IP del socket
	uint32_t ip_listen;

	//Puerto del socket
	uint32_t port_listen;

	//DESCONECTADO, CONECTADO, PENDIENTE (de aceptacion)
	enum t_estado_nodo estado;

	//cantidad de bloques que se pueden almacenar en el nodo
	int cantidad_bloques;

	//array de bits donde cada bit simboliza un bloque del nodo
	//bloqueOcupado = 1, bloqueVacio = 0
	t_kbitarray* bloquesLlenos;

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
void rname(char*,char*);
void makedir(char*);
void remdir(char*);
void mvdir(char*, char*);
void renamedir(char*,char*);
void upload(char*, char*);
void download(char*, char*);
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
int recivir_instrucciones(int);
void setSocketAddr(struct sockaddr_in*);
void hilo_listener();
void preparar_fs (); //Configura lo inicial del fs o levanta la informacion persistida.
void set_root();
void info_nodo_destroy(struct info_nodo*);
void dir_destroy(struct t_dir*);
void arch_destroy(struct t_arch*);
void bloque_destroy(struct t_bloque*);
void nodo_destroy(struct t_nodo*);
void print_path_actual();
int warning(char*);
struct t_dir* dir_create(char*, struct t_dir*);
struct t_arch* arch_create(char*, struct t_dir*, int, t_list*);
void get_info_from_path(char*, char**, struct t_dir**);
struct t_dir* get_dir_from_path(char*);

//Variables Globales
struct conf_fs conf; //Configuracion del fs
char end; //Indicador de que deben terminar todos los hilos
t_list* list_info_nodo; //Lista de nodos que solicitan conectarse al FS
t_list* listaNodos; //Lista de nodos activos o desconectados
int arch_id_counter; //Se incrementa cada vez que s hace un upload
struct t_dir* root;
struct t_dir* dir_actual;

int main(void) {                                         //TODO aca esta el main

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

	dir_destroy(root);
	list_destroy_and_destroy_elements(listaNodos, (void*) nodo_destroy);
	pthread_join(t_listener, NULL);

	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
void hilo_listener() {
	list_info_nodo = list_create();

	fd_set master; // Nuevo set principal
	fd_set read_fds; // Set temporal para lectura
	FD_ZERO(&master); // Vacio los sets
	FD_ZERO(&read_fds);
	int fd_max; // Va a ser el maximo de todos los descriptores de archivo del select
	struct sockaddr_in sockaddr_listener, sockaddr_cli;
	setSocketAddr(&sockaddr_listener);

	int listener = socket(AF_INET, SOCK_STREAM, 0);
	int socketfd_cli;
	int yes = 1;
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
			== -1) {
		perror("setsockopt");
		exit(1);
	}
	if (bind(listener, (struct sockaddr*) &sockaddr_listener,
			sizeof(sockaddr_listener)) == -1) {
		perror("Error binding");
		exit(-1);
	}
	if (listen(listener, 100) == -1) {
		perror("Error listening");
		exit(-1);
	}

	FD_SET(listener, &master);
	FD_SET(STDIN_FILENO, &master);
	fd_max = listener;
	int sin_size = sizeof(struct sockaddr_in);

	int i;
	while(!end){
		read_fds = master; // Cada iteracion vuelvo a copiar del principal al temporal
		select(fd_max + 1, &read_fds, NULL, NULL, NULL); // El select se encarga de poner en los temp los fds que recivieron algo
		for (i = 0; i <= fd_max; i++) {
			if (FD_ISSET(i, &read_fds)) {
				if (i == listener) {
					socketfd_cli = accept(listener, (struct sockaddr*) &sockaddr_cli, (socklen_t*) &sin_size);
					if(recivir_instrucciones(socketfd_cli) <= 0) {
						puts("Error al recibir el info Nodo");//FIXME
						list_destroy_and_destroy_elements(list_info_nodo, (void*) info_nodo_destroy);
						close(listener);
						exit(-1);
					}
					FD_SET(socketfd_cli, &master);
					if (socketfd_cli > fd_max)
						fd_max = socketfd_cli;
				}
			}
		}
	}

	list_destroy_and_destroy_elements(list_info_nodo, (void*) info_nodo_destroy);
	close(listener);
}

//---------------------------------------------------------------------------
int recivir_instrucciones(int socket){
	uint32_t prot;
//	if (recv(socket, &prot, sizeof(uint32_t), 0) == -1) {
//		return -1;
//	}
	int result;

	prot = receive_protocol_in_order(socket);

	switch (prot) {
	case INFO_NODO:
		result = recivir_info_nodo(socket);
		break;
	default:
		return -1;
	}
	return result;
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

	info_nodo->socket_FS_nodo = socket;
	printf("agregando nodo: %i\n",info_nodo->id);
	fflush(stdout);
	list_add(list_info_nodo, info_nodo);

	return result;
}

//---------------------------------------------------------------------------
int estaActivoNodo(int nro_nodo) {
	int _esta_activo(struct t_nodo* nodo) {
		return (nro_nodo == nodo->id_nodo) && (nodo->estado);
	}
	return list_any_satisfy(listaNodos, (void*) _esta_activo);
}

//---------------------------------------------------------------------------
struct t_copia_bloq* find_copia_activa(t_list* lista_de_copias){
	int _esta_activa(struct t_copia_bloq* copy){
		return estaActivoNodo(copy->id_nodo);
	}
	return list_find(lista_de_copias, (void*) _esta_activa);
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
	return todosLosBloquesDelArchivoDisponibles(archivo)
			&& ningunBloqueBorrado(archivo);
}

//---------------------------------------------------------------------------
//int esperarConexionesDeNodos(int cant_minima_nodos);

//---------------------------------------------------------------------------
//int atenderSolicitudesDeMarta();

//---------------------------------------------------------------------------
void levantar_arch_conf() {

	char** properties = string_split("FS_VACIO,PUERTO_LISTEN,MIN_CANT_NODOS", ",");
	t_config* conf_arch = config_create("fs.cfg");

	if (has_all_properties(3, properties, conf_arch)) {
		conf.fs_vacio = config_get_int_value(conf_arch,properties[0]);
		conf.puerto_listen = config_get_int_value(conf_arch,properties[1]);
		conf.min_cant_nodos = config_get_int_value(conf_arch,properties[2]);
	} else {
//		log_error(paranoid_log, "Faltan propiedades en archivo de Configuración");//FIXME tirar cartel de error
		exit(-1);
	}
	config_destroy(conf_arch);
	free_string_splits(properties);
}

//---------------------------------------------------------------------------
void setSocketAddr(struct sockaddr_in* direccionDestino) {
	direccionDestino->sin_family = AF_INET; // familia de direcciones (siempre AF_INET)
	direccionDestino->sin_port = htons(conf.puerto_listen); // setea Puerto a conectarme
	direccionDestino->sin_addr.s_addr = htonl(INADDR_ANY); // escucha todas las conexiones
	memset(&(direccionDestino->sin_zero), '\0', 8); // pone en ceros los bits que sobran de la estructura
}


//---------------------------------------------------------------------------
void preparar_fs () {
	listaNodos = list_create();
	if(conf.fs_vacio){//FIXME sacar fs_vacio
		set_root();
		arch_id_counter = 0;
	} else {
		// ToDo levantar toda la informacion persistida
	}
}

//---------------------------------------------------------------------------
void set_root(){
	struct t_dir* dir_root;
	dir_root = malloc(sizeof(struct t_dir));
		dir_root->nombre = string_duplicate("root");
		dir_root->parent_dir = NULL;
		dir_root->list_dirs = list_create();
		dir_root->list_archs = list_create();
	root = dir_root;
	dir_actual = dir_root;
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
struct t_nodo* find_nodo_with_ID(int id){
	int _eq_ID(struct t_nodo* nodo){
		return nodo->id_nodo==id;
	}
	return list_find(listaNodos, (void*) _eq_ID);
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
int any_nodo_with_ID(int id, t_list* list){
	int _eq_ID(struct t_nodo* nodo){
		return nodo->id_nodo==id;
	}
	return list_any_satisfy(list, (void*) _eq_ID);
}

//---------------------------------------------------------------------------
int contains(void* elem, t_list* list){
	int _eq(void* any_elem){
		return elem == any_elem;
	}
	return list_any_satisfy(list, (void*) _eq);
}

							 //DESTROYERS
//---------------------------------------------------------------------------
void info_nodo_destroy(struct info_nodo* self){
	free(self);
}

//---------------------------------------------------------------------------
void nodo_destroy(struct t_nodo* self){
	kbitarray_destroy(self->bloquesLlenos);
	free(self);
}

//---------------------------------------------------------------------------
void copia_bloque_destroy(struct t_copia_bloq* self){
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
		return has_disp_block(nodo) && !contains(nodo, list_used);
	}
	filtered_list = list_filter(listaNodos, (void*) _disp_and_not_used);
	int _by_more_free_space(struct t_nodo* n1, struct t_nodo* n2){
		int amount_clean_n1 = kbitarray_amount_bits_clean(n1->bloquesLlenos);
		int amount_clean_n2 = kbitarray_amount_bits_clean(n1->bloquesLlenos);
		return amount_clean_n1 > amount_clean_n2;
	}
	list_sort(filtered_list, (void*) _by_more_free_space);
	*the_choosen_one = list_get(filtered_list,0);
	list_destroy(filtered_list);
	if(*the_choosen_one == NULL) return -1;
	*index_set = kbitarray_find_first_clean((*the_choosen_one)->bloquesLlenos);
	kbitarray_set_bit((*the_choosen_one)->bloquesLlenos,*index_set);
	return 0;
} //FIXME elegir el que tenga más bloques disponibles



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
	int e1, e2, e3;
	int socket_nodo = nodo->socket_FS_nodo;
	e1 = enviar_protocolo(socket_nodo, WRITE_BLOCK);
		if(e1==-1)return -1;
	e2 = enviar_int(socket_nodo, index_set);
		if(e2==-1)return -1;
	e3 = enviar(socket_nodo, &data[block_start], (block_end-block_start)+1);
		if(e3==-1)return -1;
	return 0;
}

//---------------------------------------------------------------------------
int recv_block(char** data, struct t_nodo* nodo, int index_set) {
	int e1, e2, e3;
	int socket_nodo = nodo->socket_FS_nodo;
	e1 = enviar_protocolo(socket_nodo, READ_BLOCK);
		if(e1==-1)return -1;
	e2 = enviar_int(socket_nodo, index_set);
		if(e2==-1)return -1;
	e3 = recibir_dinamic_buffer(socket_nodo, (void**) data);
		if(e3==-1)return -1;
	return 0;
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
		block_end = block_start + BLOCK_SIZE;
		if(block_end > data_last_index){
			block_end = data_last_index;
			fin = 1;
		}
		while(data[block_end]!='\n') block_end--;
		for(i=0;i<CANT_COPIAS;i++){
			if(get_nodo_disp(list_used, &nodo_disp, &index_set)==-1){
				puts("no hay nodos disponibles");
				list_destroy(list_used);
				return -1;
			}
			if (send_block(data,nodo_disp,index_set,block_start,block_end)==-1) {
				list_destroy(list_used);
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
	int i;
	for(i=0;i<arch->cant_bloq;i++) {
			block = list_get(arch->bloques,i);
			copy = find_copia_activa(block->list_copias);
			nodo = find_nodo_with_ID(copy->id_nodo);
			if(recv_block(&data,nodo,copy->bloq_nodo)!=-1){
				if ((write(local_fd,data,string_length(data))) == -1){
					perror("error al escribir archivo");
					return -1;
				}
			} else {
				return -1;
			}
	}
	return 0;
}

//---------------------------------------------------------------------------
int copy_block(struct t_bloque* block){
	t_list* list_used;
	struct t_nodo* recv_nodo,
				 * send_nodo;
	struct t_copia_bloq* copy_to_copy;
	int index_set;
	char* data;

	copy_to_copy = find_copia_activa(block->list_copias);
	recv_nodo = find_nodo_with_ID(copy_to_copy->id_nodo);
	if(recv_block(&data,recv_nodo,copy_to_copy->bloq_nodo)==-1) {
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
	list_used = list_filter(listaNodos, (void*) _nodo_used);
	if(get_nodo_disp(list_used,&send_nodo,&index_set)==-1) {
		list_destroy(list_used);
		free(data);
		puts("error: no hay ningun nodo disponible");
		return -1;
	}
	if(send_block(data,send_nodo,index_set,0,string_length(data)-1)==-1){
		list_destroy(list_used);
		free(data);
		return -1;
	}
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
	arch_id_counter++;
	return new_arch;
}

//---------------------------------------------------------------------------
struct t_dir* dir_create(char* dir_name, struct t_dir* parent_dir){;
	struct t_dir* new_dir;
	new_dir = malloc(sizeof(struct t_dir));
		new_dir->nombre = dir_name;
		new_dir->parent_dir = parent_dir;
		new_dir->list_dirs = list_create();
		new_dir->list_archs = list_create();
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
int is_valid_arch_name(char* name, struct t_dir* parent_dir){
	if(any_arch_with_name(name,parent_dir->list_archs)) {
		return 0;
	}
	return 1;
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
		}  else if (any_dir_with_name(dir_name, dir->list_dirs)) {
			dir = find_dir_with_name(dir_name, dir->list_dirs);
		} else {
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
int string_split_size(char** matriz){ //Todo <- esto esta bueno para agregarlo a la lib..
	int i;
	for (i=0;matriz[i]!=NULL;i++);
	return i;
}

//---------------------------------------------------------------------------
void get_info_from_path(char* path, char** name, struct t_dir** parent_dir){ //Si har error: parent_dir = NULL
	char** sub_dirs = string_split(path,"/");
	int last_index = string_split_size(sub_dirs)-1;
	*name = string_duplicate(sub_dirs[last_index]);
	*parent_dir = dir_actual;

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

	char comandos_validos[MAX_COMMANOS_VALIDOS][16]={"help","format","pwd","ls","cd","rm","mv","rename","mkdir","rmdir","mvdir",
													"renamedir","upload","download","md5","blocks","rmblock","cpblock","lsrequest",
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
		case  7: rname(subcommands[1],subcommands[2]); break;

		case  8: makedir(subcommands[1]); break;
		case  9: remdir(subcommands[1]); break;
		case 10: mvdir(subcommands[1],subcommands[2]); break;
		case 11: renamedir(subcommands[1],subcommands[2]); break;

		case 12: upload(subcommands[1],subcommands[2]); break;
		case 13: download(subcommands[1],subcommands[2]); break;
		case 14: md5(subcommands[1]); break;

		case 15: blocks(subcommands[1]); break;
		case 16: rmblock(subcommands[1],subcommands[2],subcommands[3]); break;
		case 17: cpblock(subcommands[1],subcommands[2]); break;

		case 18: lsrequest(); break;
		case 19: lsnode(); break;
		case 20: addnode(subcommands[1]); break;
		case 21: rmnode(subcommands[1]); break;

		case 22: printf(CLEAR); break;
		case 23: salir=1; break;

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
	puts(BOLD" rename (old path file) (nuevo path file)"NORMAL" -> Renombra el archivo (old name file) con (new name file)."); //?????
	puts(BOLD" mkdir (dir name)"NORMAL" -> Crea el directorio (dir name).");
	puts(BOLD" rmdir (path dir)"NORMAL" -> Elimina el directorio (path dir).");
	puts(BOLD" mvdir (old path dir) (new path dir)"NORMAL" -> Mueve el directorio de (old path dir) a (new path dir).");
	puts(BOLD" renamedir (old path dir) (nuevo path dir)"NORMAL" -> Renombra el directorio (old name dir) con (new name dir)."); //?????
	puts(BOLD" upload (local path file) (MDFS path file)"NORMAL" -> Copia el archivo del filesystem local (local path file) al MDFS (MDFS path file).");
	puts(BOLD" download (MDFS path file) (local path file)"NORMAL" -> Copia el archivo del MDFS (MDFS path file) al filesystem local (local path file).");
	puts(BOLD" md5 (path file)"NORMAL" -> Solicita el MD5 del archivo path (file).");
	puts(BOLD" blocks (path file)"NORMAL" -> Muestra los bloques que componen el archivo (path file).");
	puts(BOLD" rmblock (num block) (num copy) (path file)"NORMAL" -> Elimina el bloque nro (num block) copia nro (num copy) del archivo (path file).");
	puts(BOLD" cpblock (num block) (path file)"NORMAL" -> Copia el bloque nro (num block) del archivo (path file) en un nodo disponoble.");
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
		dir_destroy(root);
		set_root();
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
			if(is_valid_arch_name(arch_name, parent_dir_aux)) {
				arch_move(&arch_aux, parent_dir_aux);
				aux_name = arch_aux->nombre;
				arch_aux->nombre = arch_name;
				free(aux_name);
			} else {
				printf("%s: no es un nombre valido\n",arch_name);
				free(arch_name);
			}
		} else {
			printf("%s: el archivo no existe\n",new_path);
		}
	} else {
		printf("%s: el archivo no existe\n",old_path);
	}
}

//---------------------------------------------------------------------------
void rname(char* arch_path, char* new_name) {
	struct t_arch *arch_aux;
	char* aux_name;
	if(arch_path==NULL){
		puts("mv: falta un archivo como operando");
	} else if((arch_aux = get_arch_from_path(arch_path))!=NULL) {
		if(new_name!=NULL) {
			if(is_valid_arch_name(new_name, arch_aux->parent_dir)) {
				aux_name = arch_aux->nombre;
				arch_aux->nombre = string_duplicate(new_name);
				free(aux_name);
			} else {
				printf("%s: no es un nombre valido\n",new_name);
			}
		} else {
			puts("renamedir: falta un nombre como operando");
		}
	} else {
		printf("%s: el archivo no existe\n",arch_path);
	}
}

//---------------------------------------------------------------------------
void makedir(char* dir_path){  //TODO Hay que persistir algunas cosas aca
	if (dir_path==NULL) {
		puts("mkdir: falta un operando");
	} else {
		char* dir_name;
		struct t_dir* parent_dir;
		get_info_from_path(dir_path, &dir_name, &parent_dir);
		if(parent_dir!=NULL) {
			if(is_valid_dir_name(dir_name, parent_dir)) {
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
void remdir(char* dir_path){  //TODO Hay que persistir algunas cosas aca
	struct t_dir* dir_aux;
	if(dir_path==NULL){
		puts("rmdir: falta un operando");
	} else if((dir_aux = get_dir_from_path(dir_path))!=NULL) {
		int _eq_name(struct t_dir* direc){
			return string_equals_ignore_case(direc->nombre,dir_aux->nombre);
		}
		if(dir_is_empty(dir_aux)) {
			list_remove_by_condition(dir_aux->parent_dir->list_dirs, (void*) _eq_name);
			dir_destroy(dir_aux);
		} else if(warning("El directorio no esta vacio, desea eliminarlo de todas formas?")) {
			list_remove_by_condition(dir_aux->parent_dir->list_dirs, (void*) _eq_name);
			dir_destroy(dir_aux);
		}
	} else {
		printf("%s: el directorio no existe\n", dir_path);
	}
}

//---------------------------------------------------------------------------
void mvdir(char* old_path, char* new_path){  //TODO Hay que persistir algunas cosas aca
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
void renamedir(char* dir_path, char* new_name){ //todo esto no estaria siendo muy inutil
	struct t_dir* dir_aux;
	char* old_name;
	if(dir_path==NULL){
		puts("renamedir: falta un directorio como operando");
	} else if((dir_aux = get_dir_from_path(dir_path))!=NULL) {
		if(new_name!=NULL) {
			if(is_valid_dir_name(new_name, dir_aux->parent_dir)) {
				old_name = dir_aux->nombre;
				dir_aux->nombre = string_duplicate(new_name);
				free(old_name);
			} else {
				printf("%s: no es un nombre valido\n",new_name);
			}
		} else {
			puts("renamedir: falta un nombre como operando");
		}
	} else {
		puts("error: el directorio no existe");
	}
}

//---------------------------------------------------------------------------
void upload(char* local_path, char* mdfs_path){
	puts("Procesando...");
	int local_fd, blocks_sent =0, send_ok=1; //desinicializar todo
	struct stat file_stat;
	char *data, *arch_name;
	struct t_dir *parent_dir;
	t_list* list_blocks = list_create();

	if(local_path!=NULL && mdfs_path!=NULL) {
		if ((local_fd = open(local_path, O_RDONLY)) != -1) {

			fstat(local_fd, &file_stat);
			data = mmap((caddr_t)0, file_stat.st_size, PROT_READ, MAP_SHARED, local_fd, OFFSET);
			if (data == (caddr_t)(-1)) {
				perror("mmap");
				exit(1);
			}
			send_ok = send_all_blocks(data,&blocks_sent, &list_blocks);

			if (close(local_fd) == -1) perror("close");
			if(send_ok!=-1){
				get_info_from_path(mdfs_path, &arch_name, &parent_dir);
				if(parent_dir!=NULL) {
					if(is_valid_arch_name(arch_name, parent_dir)) {
						list_add(parent_dir->list_archs, arch_create(arch_name,parent_dir,blocks_sent,list_blocks));
					} else {
						printf("%s: no es un nombre valido\n",arch_name);
						free(arch_name);
					}
				} else {
					printf("%s: el directorio no existe\n",mdfs_path);
					free(arch_name);
				}
			}
		} else {
			perror("Error abriendo el archivo");
		}
	} else {
		puts("upload: falta un operando");
	}
}

//---------------------------------------------------------------------------
void download(char* arch_path, char* local_path){
	puts("Procesando...");
	struct t_arch* arch_aux;
	int local_fd;
	if(arch_path==NULL){
		puts("download: falta un operando");
	} else if((arch_aux = get_arch_from_path(arch_path))!=NULL) {
		if(todosLosBloquesDelArchivoDisponibles(arch_aux)){
			if ((local_fd = open(local_path, O_CREAT | O_RDWR)) != -1) {

				rebuild_arch(arch_aux,local_fd);

				if (close(local_fd) == -1) perror("close");
			} else {
				perror("error al crear el archivo");
			}
		} else {
			printf("%s: el archivo no se encuentra disponible", arch_aux->nombre);
		}
	} else {
		printf("%s: el archivo no existe\n", arch_path);
	}
}

//---------------------------------------------------------------------------
void md5(char* arch_path){

}

//---------------------------------------------------------------------------
void blocks(char* arch_path){
	struct t_arch* arch_aux;
	struct t_bloque* block;
	int i,j;
	if(arch_path==NULL){
		puts("blocks: falta un archivo como operando");
	} else if((arch_aux = get_arch_from_path(arch_path))!=NULL) {
		for(i=0;i<arch_aux->cant_bloq;i++){
			block = list_get(arch_aux->bloques,i); //FIXME al borrar uno no baja la cantidad de blockes  (usar list_iterate)
			printf(" # Block: %d\n",block->nro_bloq);
			j=0;
			void _print_copies(struct t_copia_bloq* copy){
				printf("  *Copia %d: Nodo %d, Bloque %d\n", j,copy->id_nodo, copy->bloq_nodo);
				j++;
			}
			list_iterate(block->list_copias, (void*) _print_copies);
		}
	} else {
		printf("%s: el archivo no existe\n", arch_path);
	}
}

//---------------------------------------------------------------------------
void rmblock(char* num_block_str, char* num_copy_str, char* arch_path){//XXX preguntar por si borra solo una copia o todas
	int num_block, num_copy;
	struct t_arch* arch_aux;
	struct t_bloque* bloque;
	if(num_block_str==NULL){
		puts("rmblock: falta un numero de bloque como operando");
	} else if(num_copy_str==NULL){
		puts("rmblock: falta un numero de copia como operando");
	} else if(arch_path==NULL){
		puts("rmblock: falta un archivo como operando");
	} else {
		num_block = strtol(num_block_str, NULL, 10);
		num_copy = strtol(num_copy_str, NULL, 10);
		if((arch_aux=get_arch_from_path(arch_path))!=NULL){
			int _eq_num_block(struct t_bloque* block){
				return block->nro_bloq==num_block;
			}
			bloque = list_remove_by_condition(arch_aux->bloques, (void*) _eq_num_block);
			copia_bloque_destroy(list_get(bloque->list_copias, num_copy));
		} else {
			printf("%s: el archivo no existe\n",arch_aux->nombre);
		}
	}
}

//---------------------------------------------------------------------------
void cpblock(char* num_block_str, char* arch_path){
	puts("Procesando...");
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
				if(copy_block(bloque)!=-1){
					puts("el bloque fue copiado con exito\n");
				}
			} else {
				printf("%d: no existe en el archivo un bloque con ese numero",num_block);
			}
		} else {
			printf("%s: el archivo no existe\n",arch_aux->nombre);
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
		printf("  *Nodo nuevo: %d\n", ptr_inodo->nodo_nuevo);
	}
}

//---------------------------------------------------------------------------
void lsnode() {
	int lsize = list_size(listaNodos);
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
		ptr_nodo = list_get(listaNodos, i);
		getpeername(ptr_nodo->socket_FS_nodo, (struct sockaddr*)&peer, &peer_size);
		printf(" ID: %d\n", ptr_nodo->id_nodo);
		printf("  *IP: %s\n", inet_ntoa(peer.sin_addr));
		printf("  *Cantidad de bloques: %d\n", ptr_nodo->cantidad_bloques);
		printf("  *Bloques ocupados: %i\n",(int) kbitarray_amount_bits_set(ptr_nodo->bloquesLlenos));
	}
}

//---------------------------------------------------------------------------
void addnode(char* IDstr){  //TODO Hay que persistir algunas cosas aca
	int ID;
	int _eq_ID(struct info_nodo* ninf){
		return ninf->id == ID;
	}
	if(IDstr!=NULL){
		ID = strtol(IDstr, NULL, 10);
		if(any_nodo_with_ID(ID, list_info_nodo)){
			struct info_nodo* infnod = list_remove_by_condition(list_info_nodo, (void*) _eq_ID);
			struct t_nodo* nuevo_nodo;
			nuevo_nodo = malloc(sizeof(struct t_nodo));
				nuevo_nodo->id_nodo = infnod->id;
				nuevo_nodo->estado = infnod->nodo_nuevo;
				nuevo_nodo->cantidad_bloques = infnod->cant_bloques;//FIXME es innecesario
				nuevo_nodo->socket_FS_nodo = infnod->socket_FS_nodo;
				nuevo_nodo->ip_listen = infnod->ip_listen;
				nuevo_nodo->port_listen = infnod->port_listen;
				nuevo_nodo->bloquesLlenos = kbitarray_create_and_clean_all(infnod->cant_bloques);
			info_nodo_destroy(infnod);
			list_add(listaNodos, nuevo_nodo);
		} else {
			printf("%d: no hay ningun nodo con ese ID\n",ID);
		}
	} else {
		puts("addnode: falta un ID como operando");
	}
}

//---------------------------------------------------------------------------
void rmnode(char* IDstr){
	int ID;
	int _eq_ID(struct info_nodo* ninf){
		return ninf->id == ID;
	}
	if(IDstr!=NULL){
		ID = strtol(IDstr, NULL, 10);
		if(any_nodo_with_ID(ID, listaNodos)){
			list_remove_and_destroy_by_condition(listaNodos, (void*) _eq_ID, (void*) nodo_destroy);
		} else {
			printf("%d: no hay ningun nodo con ese ID\n",ID);
		}
	} else {
		puts("addnode: falta un ID como operando");
	}
}
