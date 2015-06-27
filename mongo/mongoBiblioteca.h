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
