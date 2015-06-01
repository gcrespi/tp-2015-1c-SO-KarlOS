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
	int id_dir;
	char *nombre;
	int id_padre;
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
void free_string_splits(char**);
void receive_command(char*, int);
char execute_command(char*);
void help();
void pwd();
void ls();
void cd(char*);
void mkdir(char*);
void lsnode();
void addnode(char*);

//Prototipos
void levantar_arch_conf();
void recivir_info_nodo (int);
int recivir_instrucciones(int);
void setSocketAddr(struct sockaddr_in*);
void hilo_listener();
static void info_nodo_destroy(struct info_nodo*);
void preparar_fs (); //Configura lo inicial del fs o levanta la informacion persistida.
void set_root();
void print_path_actual();

//Variables Globales
struct conf_fs conf; //Configuracion del fs
char end; //Indicador de que deben terminar todos los hilos
t_list* list_info_nodo; //Lista de nodos que solicitan conectarse al FS
t_list* listaNodos; //(lista de t_nodo)
t_list* listaDir; //(lista de t_dir)
int dir_id_counter;
t_list* listaArchivos; //(lista de t_archivo)
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
static void info_nodo_destroy(struct info_nodo* self){
	free(self);
}

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
	listaArchivos = list_create();
	listaDir = list_create();
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
		dir_root->id_dir = 0;
		dir_root->nombre = string_duplicate("root");
		dir_root->id_padre = -1;
	list_add(listaDir, dir_root);
	dir_id_counter = 0;
	dir_id_counter++;
	dir_actual = dir_root;
 }

//---------------------------------------------------------------------------
struct t_dir* find_dir_with_id(int id){
	int _eq_id(struct t_dir* direc){
		return id == direc->id_dir;
	}
	return list_find(listaDir, (void*) _eq_id);
}

//---------------------------------------------------------------------------
struct t_dir* find_dir_with_name(char* name){
	int _eq_name(struct t_dir* direc){
		return string_equals_ignore_case(direc->nombre,name);
	}
	return list_find(listaDir, (void*) _eq_name);
}

//---------------------------------------------------------------------------
void free_dir_nombres(){
	//ToDo liberar los nombres de todos los archivos;
}

//---------------------------------------------------------------------------
void print_path_actual(){
	char* path = string_new();
		char* aux;
		int pid;
		struct t_dir* direc = dir_actual;
		do {
			aux = string_new();
			string_append(&aux,"/");
			string_append(&aux,direc->nombre);
			string_append(&aux,path);
			//free(path);
			path = string_duplicate(aux);
			free(aux);
			pid = direc->id_padre;
			if(pid!=-1) direc = find_dir_with_id(pid);
		} while(pid!=-1);
		printf("%s",path);
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
//		case  1: format(); break;
		case  2: pwd(); break;
		case  3: ls(); break;
		case  4: cd(subcommands[1]); break;
//		case  5: rm(subcommands[1]); break;
//
//		case  6: mv(subcommands[1],subcommands[2]); break;
//		case  7: renamedir(subcommands[1],subcommands[2]); break;
		case  8: mkdir(subcommands[1]); break;
//		case  9: rmdir(subcommands[1]); break;
//		case 10: mvdir(subcommands[1],subcommands[2]); break;
//
//		case 11: renamedir(subcommands[1],subcommands[2]); break;
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
void pwd(){
	print_path_actual();
	printf("\n");
}

//---------------------------------------------------------------------------
void ls(){
	/*int pid = dir_actual->id_dir;
	void _print_if_pid(struct t_)
	list_iterate()*/
}

//---------------------------------------------------------------------------
void cd(char* dir_name){
	if(string_equals_ignore_case(dir_name,"..")){
		dir_actual = find_dir_with_id(dir_actual->id_padre);
	} else { //ToDo contemplar errores, poner condicion en el caso de nombre invalido
		dir_actual = find_dir_with_name(dir_name);
	}
}

//---------------------------------------------------------------------------
void mkdir(char* dir_name){
	struct t_dir* nuevo_dir;
	nuevo_dir = malloc(sizeof(struct t_dir));
		nuevo_dir->id_dir = dir_id_counter;
		nuevo_dir->nombre = string_duplicate(dir_name);
		nuevo_dir->id_padre = dir_actual->id_dir;
	dir_id_counter++;
	list_add(listaDir, nuevo_dir);
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


