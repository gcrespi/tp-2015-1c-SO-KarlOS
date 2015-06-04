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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include "../../connectionlib/connectionlib.h"

//Constantes de la consola
#define MAX_COMMANOS_VALIDOS 30
#define MAX_COMMAND_LENGTH 100
#define BOLD "\033[1m"
#define NORMAL "\033[0m"
#define CLEAR "\033[H\033[J"

//  Estados del nodo
enum t_estado_nodo {
	DESCONECTADO,CONECTADO,PENDIENTE  // todo Pendiente que seria?
};

//Estructura de carpetas del FS (Se persiste)
struct t_dir {
	struct t_dir* parent_dir;
	int id_dir;
	char* nombre;
	t_list* list_dirs;
	t_list* list_archs;
};

//no se persisten: estado, aceptado
//ya que cuando se cae el FS deben volver a conectar
struct t_nodo {
	//numero unico de identificacion de cada nodo
	int id_nodo;

	//file descriptor del socket del nodo
	int socketfd_nodo;

	//DESCONECTADO, CONECTADO, PENDIENTE (de aceptacion)
	enum t_estado_nodo estado;

	//cantidad de bloques que se pueden almacenar en el nodo
	int cantidad_bloques;

	//array de bits donde cada bit simboliza un bloque del nodo
	//bloqueOcupado = 1, bloqueVacio = 0
	t_bitarray bloquesLlenos;

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
	struct t_copia_bloq copia[3];
};

//Se persiste, sino perdería toda la info sobre los archivos guardados y sus bloques
struct t_archivo {
	//codigo unico del archivo fragmentado
	int id_archivo;

	//nombre del archivo dentro del MDFS
	char *nombre;

	//directorio padre del archivo dentro del MDFS
	int id_dir_padre;

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
	int id;
	int nodo_nuevo;
	int cant_bloques;
	int socketfd_nodo;
};

//Prototipos de la consola
void receive_command(char*, int);
char execute_command(char*);
void help();
void format();
void pwd();
void ls();
void cd(char*);
void mkdir(char*);
void remdir(char*);
void mvdir(char*, char*);
void renamedir(char*,char*);
void lsnode();
void addnode(char*);

//Prototipos
void levantar_arch_conf();
void recivir_info_nodo (int);
int recivir_instrucciones(int);
void setSocketAddr(struct sockaddr_in*);
void hilo_listener();
void preparar_fs (); //Configura lo inicial del fs o levanta la informacion persistida.
void set_root();
void info_nodo_destroy(struct info_nodo*);
void dir_destroy(struct t_dir*);
void arch_destroy(struct t_archivo*);
void bloque_destroy(struct t_bloque*);
void nodo_destroy(struct t_nodo*);
void print_path_actual();
int warning(char*);
struct t_dir* dir_create(char*, struct t_dir*);
void get_info_from_path(char*, char**, struct t_dir**);
struct t_dir* get_dir_from_path(char*);

//Variables Globales
struct conf_fs conf; //Configuracion del fs
char end; //Indicador de que deben terminar todos los hilos
t_list* list_info_nodo; //Lista de nodos que solicitan conectarse al FS
t_list* listaNodos; //Lista de nodos
int dir_id_counter;
struct t_dir* root;
struct t_dir* dir_actual;

int main(void) {                                         //TODO aca esta el main

	levantar_arch_conf();   //Levanta el archivo de configuracion "fs.cfg"
	preparar_fs ();
	pthread_t t_listener;
	pthread_create(&t_listener, NULL, (void*) hilo_listener, NULL);

	char command[MAX_COMMAND_LENGTH + 1];
	puts(CLEAR NORMAL"Console KarlOS\nType 'help' to show the commands");
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
					recivir_instrucciones(socketfd_cli);
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
	if (recv(socket, &prot, sizeof(uint32_t), 0) == -1) {
		return -1;
	}
	switch (prot) {
	case INFO_NODO:
		recivir_info_nodo(socket);
		break;
	default:
		return -1;
	}
	return prot;
}
//---------------------------------------------------------------------------
void recivir_info_nodo (int socket){
	struct info_nodo* info_nodo;
	info_nodo = malloc(sizeof(struct info_nodo));
	if (recibir(socket, &(info_nodo->id)) == -1) { //recibe id del nodo
		perror("Error reciving id");
		exit(-1);
	}
	if (recibir(socket, &(info_nodo->cant_bloques)) == -1) { //recive cantidad de bloques del nodo
		perror("Error reciving cant_bloques");
		exit(-1);
	}
	if (recibir(socket, &(info_nodo->nodo_nuevo)) == -1) { //recive si el nodo es nuevo o no
		perror("Error reciving nodo_nuevo");
		exit(-1);
	}
	info_nodo->socketfd_nodo = socket;
	list_add(list_info_nodo, info_nodo);
}

//---------------------------------------------------------------------------
int estaActivoNodo(int nro_nodo) {
	int _nodoConNumeroBuscadoActivo(struct t_nodo nodo) {
		return (nro_nodo == nodo.id_nodo) && (nodo.estado);
	}
	return list_any_satisfy(listaNodos, (void*) _nodoConNumeroBuscadoActivo);
}

//---------------------------------------------------------------------------
int bloqueActivo(struct t_bloque bloque) {
	int i;

	for (i = 0; (i < 3) && (!estaActivoNodo(bloque.copia[i].id_nodo)); i++)
		;

	return i < 3;
}

//---------------------------------------------------------------------------
int todosLosBloquesDelArchivoDisponibles(struct t_archivo archivo) {
	return list_all_satisfy(archivo.bloques, (void*) bloqueActivo);
}

//---------------------------------------------------------------------------
int ningunBloqueBorrado(struct t_archivo archivo) {
	return archivo.cant_bloq == list_size(archivo.bloques);
}

//---------------------------------------------------------------------------
int estaDisponibleElArchivo(struct t_archivo archivo) {
	return todosLosBloquesDelArchivoDisponibles(archivo)
			&& ningunBloqueBorrado(archivo);
}

//---------------------------------------------------------------------------
//int esperarConexionesDeNodos(int cant_minima_nodos);

//---------------------------------------------------------------------------
//int atenderSolicitudesDeMarta();

//---------------------------------------------------------------------------
void levantar_arch_conf() {
	t_config* conf_arch;
	conf_arch = config_create("fs.cfg");
	if (config_has_property(conf_arch,"FS_VACIO")){
			conf.fs_vacio = config_get_int_value(conf_arch,"FS_VACIO");
		} else printf("Error: el archivo de conf no tiene FS_VACIO\n");
	if (config_has_property(conf_arch,"PUERTO_LISTEN")){
		conf.puerto_listen = config_get_int_value(conf_arch,"PUERTO_LISTEN");
	} else printf("Error: el archivo de conf no tiene PUERTO_LISTEN\n");
	if (config_has_property(conf_arch,"MIN_CANT_NODOS")){
		conf.min_cant_nodos = config_get_int_value(conf_arch,"MIN_CANT_NODOS");
	} else printf("Error: el archivo de conf no tiene MIN_CANT_NODOS\n");
	config_destroy(conf_arch);
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
	if(conf.fs_vacio){
		set_root();
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
struct t_dir* find_dir_with_id(int id, t_list* list){
	int _eq_id(struct t_dir* direc){
		return id == direc->id_dir;
	}
	return list_find(list, (void*) _eq_id);
}

//---------------------------------------------------------------------------
struct t_dir* find_dir_with_name(char* name, t_list* list){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,name);
	}
	return list_find(list, (void*) _eq_name);
}

//---------------------------------------------------------------------------
int any_dir_with_name(char* name, t_list* list){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,name);
	}
	return list_any_satisfy(list, (void*) _eq_name);
}

							 //DESTROYERS
//---------------------------------------------------------------------------
void info_nodo_destroy(struct info_nodo* self){
	free(self);
}

//---------------------------------------------------------------------------
void nodo_destroy(struct t_nodo* self){
	bitarray_destroy(&self->bloquesLlenos);
	free(self);
}

//---------------------------------------------------------------------------
void bloque_destroy(struct t_bloque* self){
	free(self);
}

//---------------------------------------------------------------------------
void arch_destroy(struct t_archivo* self){
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
int is_valid_name(char* name, struct t_dir* parent_dir){
	if(string_equals_ignore_case(name,"..") ||
	   any_dir_with_name(name,parent_dir->list_dirs)) {
		return 0;
	}
	return 1;
}

//---------------------------------------------------------------------------
void dir_move(struct t_dir** dir, struct t_dir** parent_dir){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,(*dir)->nombre);
	}
	list_remove_by_condition((*dir)->parent_dir->list_dirs, (void*) _eq_name);
	(*dir)->parent_dir = *parent_dir;
	list_add((*parent_dir)->list_dirs, *dir);
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
													"renamedir","upload","download","md5","blocks","rmblock","cpblock","lsnode","addnode","rmnode",
													"clear","exit","","","","","","",""};
	int i,salir=0;
	for(i=0;command[i]==' ';i++);
	if(command[i]=='\0')
	{	return 0;}
	char** subcommands = string_split(command, " ");
	for (i = 0; (i < MAX_COMMANOS_VALIDOS) && (strcmp(subcommands[0], comandos_validos[i]) != 0); i++);

	switch (i) {
	case 0:
		help();
		break;
		case  1: format(); break;
		case  2: pwd(); break;
		case  3: ls(); break;
		case  4: cd(subcommands[1]); break;
//		case  5: rm(subcommands[1]); break
//		case  6: mv(subcommands[1],subcommands[2]); break;
//		case  7: rename(subcommands[1],subcommands[2]); break;

		case  8: mkdir(subcommands[1]); break;
		case  9: remdir(subcommands[1]); break;
		case 10: mvdir(subcommands[1],subcommands[2]); break;
		case 11: renamedir(subcommands[1],subcommands[2]); break;

//		case 12: upload(subcommands[1],subcommands[2]); break;
//		case 13: download(subcommands[1],subcommands[2]); break;
//		case 14: md5(subcommands[1]); break;

//		case 15: blocks(subcommands[1]); break;
//		case 16: rmblock(subcommands[1],subcommands[2]); break;
//		case 17: cpblock(subcommands[1],subcommands[2],subcommands[3]); break;

		case 18: lsnode(); break;
		case 19: addnode(subcommands[1]); break;
//		case 20: rmnode(subcommands[1]); break;

		case 21: printf(CLEAR); break;
		case 22: salir=1; break;

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
	puts(BOLD" rmblock (num block) (path file)"NORMAL" -> Elimina el bloque nro (num block) del archivo (path file).");
	puts(BOLD" cpblock (num block) (old path file) (new path file)"NORMAL" -> Copia el bloque nro (num block) del archivo (old path file) en el archivo (new path file).");
	puts(BOLD" lsnode"NORMAL" -> Lista los nodos disponibles para agregar.");
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
		printf(BOLD"%s  "NORMAL,dir->nombre);
		empty = 0;
	}
	list_iterate(dir_actual->list_dirs, (void*) _print_dir);
	//Print Archs
	int _arch_orden_alfabetico(struct t_dir* menor, struct t_dir* mayor){
		return strcmp(menor->nombre,mayor->nombre)==-1;
	}
	list_sort(dir_actual->list_dirs, (void*) _arch_orden_alfabetico);
	void _print_arch(struct t_archivo* arch){
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
void mkdir(char* dir_path){
	if (dir_path==NULL) {
		puts("mkdir: falta un operando");
	} else {
		char* dir_name;
		struct t_dir* parent_dir;
		get_info_from_path(dir_path, &dir_name, &parent_dir);
		if(parent_dir!=NULL) {
			if(is_valid_name(dir_name, parent_dir)) {
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
		if(dir_is_empty(dir_aux)) {
			dir_destroy(dir_aux);
		} else if(warning("El directorio no esta vacio, desea eliminarlo de todas formas?")) {
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
			if(is_valid_name(dir_name, parent_dir_aux)) {
				dir_move(&dir_aux, &parent_dir_aux);
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
void renamedir(char* dir_path, char* new_name){ //todo esto estaria siendo muy inutil
	struct t_dir* dir_aux;
	char* old_name;
	if(dir_path==NULL){
		puts("renamedir: falta un directorio como operando");
	} else if((dir_aux = get_dir_from_path(dir_path))!=NULL) {
		if(new_name!=NULL) {
			if(is_valid_name(new_name, dir_aux->parent_dir)) {
				old_name = dir_aux->nombre;
				dir_aux->nombre = string_duplicate(new_name);
				free(old_name);
			} else {
				printf("%s: no es un nombre valido\n",new_name);
			}
		}
	} else {
		puts("error: el directorio no existe");
	}
}

//---------------------------------------------------------------------------
void lsnode() {
	int lsize = list_size(list_info_nodo);
	if (lsize == 0) {
		puts("No hay nodos disponibles para agregar :(");
	} else {
		puts("Nodos disponibles para agregar:");
	}
	struct info_nodo* ptr_inodo;
	int i;
	for (i = 0; i < lsize; i++) {
		ptr_inodo = list_get(list_info_nodo, i);
		printf(" ID: %d\n",ptr_inodo->id);
		printf("  *Cantidad de bloques: %d\n",ptr_inodo->cant_bloques);
		printf("  *Nodo nuevo: %d\n",ptr_inodo->nodo_nuevo);
	}
}

//---------------------------------------------------------------------------
void addnode(char* IDstr){
	int ID = strtol(IDstr, NULL, 10);
	int _hasTheSameID(struct info_nodo* ninf){
		return ninf->id == ID;
	}
	struct info_nodo* infnod = list_remove_by_condition(list_info_nodo, (void*) _hasTheSameID);
	struct t_nodo nuevo_nodo;
		nuevo_nodo.id_nodo = infnod->id;
		nuevo_nodo.estado = infnod->nodo_nuevo;
		nuevo_nodo.cantidad_bloques = infnod->cant_bloques;
		nuevo_nodo.socketfd_nodo = infnod ->socketfd_nodo;
	list_add(listaNodos, &nuevo_nodo); //TODO Hay que persistir algunas cosas aca
}


