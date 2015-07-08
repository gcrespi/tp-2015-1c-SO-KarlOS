/*
 * mongobiblioteca.h
 *
 *  Created on: 26/6/2015
 *      Author: utnso
 */

#ifndef MONGOBIBLIOTECA_H_
#define MONGOBIBLIOTECA_H_

#include <libbson-1.0/bson.h>
#include <libmongoc-1.0/mongoc.h>
#include <stdio.h>
#include <commons/collections/list.h>
#include "../kbitarray/kbitarray.h"

mongoc_client_t *client;
mongoc_collection_t *directorioCollection;
mongoc_collection_t *archivoCollection;
mongoc_collection_t *bloqueCollection;
mongoc_collection_t *nodoCollection;

#define CANT_COPIAS 3 // cantidad de copias a enviar a los nodos

enum t_estado_nodo {
	DESCONECTADO,CONECTADO
};

struct t_dir {
    int id_directorio;
	struct t_dir* parent_dir;
    char* nombre;
    t_list* list_dirs;
    t_list* list_archs;
};
struct t_arch {
    int id_archivo;
    char *nombre;
    struct t_dir *parent_dir;
    int cant_bloq;
    t_list* bloques;
};
struct t_copia_bloq {
 int id_nodo;
 int bloq_nodo;
};
struct t_bloque {
 int nro_bloq;
 t_list* list_copias;
};

struct t_nodo {
	uint32_t id_nodo;
	uint32_t socket_FS_nodo;
	char usando_socket;
	uint32_t ip_listen;
	uint32_t port_listen;
	int estado;
	int cantidad_bloques;
	t_kbitarray* bloquesLlenos;
};

void crearBloque(struct t_bloque * bloqueNuevo, int idArchivo);
void eliminarBloque(int idArchivo, int nroBloque);
void crearDirectorioEn(struct t_dir* directorioNuevo, struct t_dir* directorioPadre);
void eliminarDirectorio(struct t_dir* dir);
void crearArchivoEn(struct t_arch* archivoNuevo, struct t_dir* directorioPadre);
void eliminarArchivo(struct t_arch * archivo);
struct t_bloque * recibirBloqueDeMongo (int idArchivo, int nro_bloq);
struct t_arch * recibirArchivoDeMongo (int idArchivo, struct t_dir * directorioPadre);
struct t_dir * recibirDirectorioDeMongo (int idDirectorio, struct t_dir * directorioPadre);
void iniciarMongo();
void cerrarMongo();
struct t_dir * levantarRaizDeMongo();
int ultimoIdDirectorio();
int ultimoIdArchivo();
void moverDirectorio(struct t_dir* dir, struct t_dir* parent_dir, char* new_name);
void eliminarNodo(struct t_nodo* nodo);
void crearNodo(struct t_nodo * nodo);
void levantarNodos(t_list* lista_nodos);
void moverArchivo(struct t_arch* arch, struct t_dir* parent_dir, char* new_name);

#endif /* MONGOBIBLIOTECA_H_ */
