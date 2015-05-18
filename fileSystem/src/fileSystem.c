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
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

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

// La estructura que envia el nodo al FS al iniciarse
struct info_nodo {
	int nodo_nuevo;
	int cant_bloques;
	char* saludo; // TODO eliminar esta linea
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
int recivir_info_nodo (int, struct info_nodo*);
int recivir(int socket, void *buffer);


int main(void) {
	return EXIT_SUCCESS;
}

//---------------------------------------------------------------------------
int recvir_bajo_protocolo(int socket, void* something){ //no estoy seguro si esto sirve
	int result=0;
	uint32_t prot;
	if ((result += recv(socket, &prot, sizeof(uint32_t), 0)) == -1) {
		return -1;
	}
	switch(prot){
		case INFO_NODO: recivir_info_nodo(socket, something); break;
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
	if ((result += recv(socket, &size_buffer, sizeof(uint32_t), 0)) == -1) {
		return -1;
	}
	if ((result += recv(socket, buffer, size_buffer, 0)) == -1) {
		return -1;
	}
	return result;
}


