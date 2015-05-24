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
#include <ifaddrs.h>




enum t_estado_nodo {
	DESCONECTADO,CONECTADO,PENDIENTE
};

//Estructura de carpetas del FS (Se persiste)
struct t_dir
{
	int id_dir;
	char *nombre;
	int id_padre;
};

//no se persisten: estado, aceptado
//ya que cuando se cae el FS deben volver a conectar
struct t_nodo {
	//numero unico de identificacion de cada nodo
	int id_nodo;

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
struct t_archivo
{
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
	int puerto_listen;
	int min_cant_nodos;
};

// La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	int nodo_nuevo;
	int cant_bloques;
};

//Enum del protocolo
enum protocolo {INFO_NODO};

//(lista de t_nodo)
t_list* listaNodos;

//(lista de t_dir)
t_list* listaDir;

//(lista de t_archivo)
t_list* listaArchivos;


int estaActivoNodo(int nro_nodo) {
	int _nodoConNumeroBuscadoActivo(struct t_nodo nodo) {
		return (nro_nodo == nodo.id_nodo) && (nodo.estado);
	}

	return list_any_satisfy(listaNodos, (void*) _nodoConNumeroBuscadoActivo);

}

int bloqueActivo(struct t_bloque bloque) {
	int i;

	for (i = 0; (i < 3) && (!estaActivoNodo(bloque.copia[i].id_nodo)); i++)
		;

	return i < 3;
}

int todosLosBloquesDelArchivoDisponibles(struct t_archivo archivo) {
	return list_all_satisfy(archivo.bloques, (void*) bloqueActivo);
}

int ningunBloqueBorrado(struct t_archivo archivo) {
	return archivo.cant_bloq == list_size(archivo.bloques);
}

int estaDisponibleElArchivo(struct t_archivo archivo) {
	return todosLosBloquesDelArchivoDisponibles(archivo)
			&& ningunBloqueBorrado(archivo);
}

//int esperarConexionesDeNodos(int cant_minima_nodos); //
//int conectarNuevoNodo(); //proceso multihilo
//
//int atenderSolicitudesDeMarta();

//Prototipos
void levantar_arch_conf();
int recivir_info_nodo (int, struct info_nodo*);
int recivir_bajo_protocolo(int);
int recivir(int socket, void *buffer);
void setSocketAddr(struct sockaddr_in*);
char* get_IP();

//Variables Globales
struct conf_fs conf;

int main(void) {

	levantar_arch_conf();

	struct sockaddr_in sockaddr_listener, sockaddr_cli;
	setSocketAddr(&sockaddr_listener);
	int listener = socket(AF_INET, SOCK_STREAM, 0), socketfd_cli;
	int yes = 1;
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
			== -1) {
		perror("setsockopt");
		exit(1);
	}
	if (bind(listener,(struct sockaddr*) &sockaddr_listener,sizeof(sockaddr_listener))==-1){
		perror("Error binding");
		exit(-1);
	}
	if (listen(listener, 100) == -1){
		perror("Error listening");
		exit(-1);
	}
	int sin_size = sizeof(struct sockaddr_in);
	if ((socketfd_cli = accept(listener, (struct sockaddr*) &sockaddr_cli, (socklen_t*) &sin_size))==-1){
		perror("Error accepting");
		exit(-1);
	}
	recivir_bajo_protocolo(socketfd_cli);

	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
void levantar_arch_conf(){
	t_config* conf_arch;
	conf_arch = config_create("fs.cfg");
	if (config_has_property(conf_arch,"PUERTO_LISTEN")){
		conf.puerto_listen = config_get_int_value(conf_arch,"PUERTO_LISTEN");
	} else printf("Error: el archivo de conf no tiene PUERTO_LISTEN\n");
	if (config_has_property(conf_arch,"MIN_CANT_NODOS")){
		conf.min_cant_nodos = config_get_int_value(conf_arch,"MIN_CANT_NODOS");
	} else printf("Error: el archivo de conf no tiene MIN_CANT_NODOS\n");
	config_destroy(conf_arch);
}

//---------------------------------------------------------------------------
char* get_IP(){ //ojala sirva para algo jaja
	    struct ifaddrs *interface_addr;
	    struct sockaddr_in* sock_addr;
	    char* addr;

	    getifaddrs (&interface_addr);
	    while(interface_addr){
	        if (interface_addr->ifa_addr->sa_family==AF_INET && strcmp(interface_addr->ifa_name,"eth0")==0 ) {
	            sock_addr = (struct sockaddr_in*) interface_addr->ifa_addr;
	        	addr = inet_ntoa(sock_addr->sin_addr);
	        }
	        interface_addr = interface_addr->ifa_next;
	    }
	    freeifaddrs(interface_addr);
	    return addr;
}
//---------------------------------------------------------------------------
void setSocketAddr(struct sockaddr_in* direccionDestino) {
	direccionDestino->sin_family = AF_INET; // familia de direcciones (siempre AF_INET)
	direccionDestino->sin_port = htons(conf.puerto_listen); // setea Puerto a conectarme
	direccionDestino->sin_addr.s_addr = htonl(INADDR_ANY); // Setea a la Ip local
	memset(&(direccionDestino->sin_zero), '\0', 8); // pone en ceros los bits que sobran de la estructura
}

//---------------------------------------------------------------------------
int recivir_bajo_protocolo(int socket){ //no estoy seguro si esto sirve
  	int result=0;
	uint32_t prot;
	if ((result += recv(socket, &prot, sizeof(uint32_t), 0)) == -1) {
		return -1;
	}
	struct info_nodo nodo_env;
	switch(prot){
		case INFO_NODO:
			recivir_info_nodo(socket, &nodo_env);
			printf("Cantidad de bloques: %d\n",nodo_env.cant_bloques);
			printf("Nodo nuevo: %d\n",nodo_env.nodo_nuevo);
			break;
		default: return -1;
	}
	return result;
}
//---------------------------------------------------------------------------
int recivir_info_nodo (int socket, struct info_nodo *info_nodo){

	int result=0;

	if ((result += recivir(socket, &(info_nodo->cant_bloques))) == -1) { //envia el primer campo
		return -1;
	}
	if ((result += recivir(socket, &(info_nodo->nodo_nuevo))) == -1) { //envia el segundo campo
		return -1;
	}
	return result;
}

//---------------------------------------------------------------------------
int recivir(int socket, void *buffer) {
	int result=0;
	uint32_t size_buffer; //el tamaño del buffer como maximo va a ser de 4 gigas (32bits)
	if (recv(socket, &size_buffer, sizeof(uint32_t), 0) == -1) {
		return -1;
	}
	if ((result += recv(socket, buffer, size_buffer, 0)) == -1) {
		return -1;
	}
	return result;
}


