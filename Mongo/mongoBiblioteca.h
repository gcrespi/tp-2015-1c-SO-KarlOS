#ifndef MONGOLIB_H_
#define MONGOLIB_H_

#include <libbson-1.0/bson.h>
#include <libmongoc-1.0/mongoc.h>
#include <stdio.h>
#include <commons/collections/list.h>

mongoc_client_t *client;
mongoc_collection_t *directorioCollection;
mongoc_collection_t *archivoCollection;
mongoc_collection_t *bloqueCollection;

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

void crearBloque(struct t_bloque * bloqueNuevo, int idArchivo);
void eliminarBloque(int idArchivo, int nroBloque);
void crearDirectorio(struct t_dir* directorioNuevo);
void eliminarDirectorio(int idDirectorio);
void crearArchivo(struct t_arch* archivoNuevo);
void eliminarArchivo(int idArchivo, int idDirectorioPadre);
struct t_bloque * recibirBloqueDeMongo (int idArchivo, int nro_bloq);
struct t_arch * recibirArchivoDeMongo (int idArchivo, struct t_dir * directorioPadre);
struct t_dir * recibirDirectorioDeMongo (int idDirectorio, struct t_dir * directorioPadre);
void iniciarMongo();
void cerrarMongo();
struct t_dir * levantarRaizDeMongo();
int ultimoIdDirectorio();
int ultimoIdArchivo();
void formatMongo();

#endif /* MONGOLIB_H_ */
