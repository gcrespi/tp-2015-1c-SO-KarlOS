#include <libbson-1.0/bson.h>
#include <libmongoc-1.0/mongoc.h>
#include <stdio.h>
#include <string.h>
#include <commons/collections/list.h>
#include <commons/string.h>
#include "mongobiblioteca.h"


/*
 *
 * Directorio = {
 *     _id: idDirectorio,
 *     nombre: nombre,
 *     directorios: [idDirectoriosHijos]
 *     archivos: [idArchivos]
 * }
 *
 * Archivo = {
 *     _id: idArchivo,
 *     nombre: nombre,
 *     idDirectorio: idDirectorioPadre,
 *     cant_bloques: cantidadDeBloques,
 * }
 *
 * Bloque = {
 *     idArchivo: idArchivo,
 *     numero: numeroBloque,
 *     copias: [{numero: numeroCopia, nodo: numeroNodo, bloque: bloqueNodo}]
 * }
 *
 */

void crearBloque(struct t_bloque * bloqueNuevo, int idArchivo){
    /*Recibo un bloque y el id del archivo, y lo escribo en mongo*/
    bson_t *array, *bloque, *query;
    bson_t *doc;
    int i;
    struct t_copia_bloq * copia;

    /*Busco si existe el archivo*/
    query = BCON_NEW ("_id", BCON_INT32 (idArchivo));
    if(!mongoc_collection_count (archivoCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
        printf("Error: no existe el archivo\n");
        bson_destroy (query);
        return;
    }

    /*Creo el documento*/
	bloque = bson_new();
    doc = bson_new();
    array = bson_new();

    BSON_APPEND_INT32 (doc, "idArchivo", idArchivo);
    BSON_APPEND_INT32 (doc, "numero", (bloqueNuevo->nro_bloq));

    bson_append_array_begin (doc, "copias", -1, array);
    for(i=0; i<CANT_COPIAS ; i++){
    	bson_append_document_begin (array, "copias", -1, bloque);
		copia = list_get((bloqueNuevo->list_copias), i);
		BSON_APPEND_INT32 (bloque, "numeroCopia", i);
		BSON_APPEND_INT32 (bloque, "nodo", (copia->id_nodo));
		BSON_APPEND_INT32 (bloque, "bloque", (copia->bloq_nodo));
		bson_append_document_end (array,bloque);
    }
    bson_append_array_end (doc, array);


    /*Hago el insert en Bloques*/
    if (!mongoc_collection_insert (bloqueCollection, MONGOC_INSERT_NONE, doc, NULL, NULL)) {
        printf ("Error insertando nuevo bloque\n");
        bson_destroy (query);
    	bson_destroy (bloque);
        bson_destroy (array);
        bson_destroy (doc);
        return;
    }

	bson_destroy (bloque);
    bson_destroy (query);
    bson_destroy (array);
    bson_destroy (doc);
}

void eliminarBloque(int idArchivo, int nroBloque){
  bson_error_t error;
  bson_t *query;

/*Busco el bloque*/
  query = bson_new ();
  BSON_APPEND_INT32 (query, "idArchivo", idArchivo);
  BSON_APPEND_INT32 (query, "numero", nroBloque);
  if(!mongoc_collection_count (bloqueCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
    printf("Error: No existe el bloque\n");
    bson_destroy (query);
    return;
  }

  /*Borro el bloque*/
  if (!mongoc_collection_remove (bloqueCollection, MONGOC_DELETE_SINGLE_REMOVE, query, NULL, &error)) {
    printf ("Error: %s\n", error.message);
  }

  bson_destroy (query);
}

void crearDirectorioEn(struct t_dir* directorioNuevo,struct t_dir* directorioPadre){
  bson_t *doc;
  bson_t *query, *update;
  bson_error_t error;

  if(directorioPadre!=NULL){
	  /*Actualizo "hijos" en padre nuevo*/
	  query = BCON_NEW ("_id", BCON_INT32(directorioPadre->id_directorio));
  	  update = BCON_NEW ("$push", "{",
		  	  	  	  	  "directorios", BCON_INT32 (directorioNuevo->id_directorio),
					  	  "}");
  	  if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  NULL)) {
	  	  printf ("Error update directorio padre nuevo\n");
	  	  bson_destroy (query);
      	  bson_destroy (update);
      	  return;
  	  }
  }

  doc = BCON_NEW("_id", BCON_INT32(directorioNuevo->id_directorio),
		  	  	 "nombre", BCON_UTF8(directorioNuevo->nombre),
				 "directorios", "[", "]",
				 "archivos", "[", "]");

   //Inserto en la coleccion
  if (!mongoc_collection_insert (directorioCollection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
    printf ("%s\n", error.message);
    if(directorioPadre){
    	bson_destroy (update);
    	bson_destroy (query);
    }
    bson_destroy(doc);
    return;
  }

  if(directorioPadre){
      	bson_destroy (update);
      	bson_destroy (query);
      }
  bson_destroy(doc);
}

void eliminarDirectorio(struct t_dir* dir){
  bson_error_t error;
  bson_t *query, *update;
  const bson_t *doc;
  bson_iter_t iter;
  bson_iter_t sub_iter;
  mongoc_cursor_t *cursor;
  struct t_arch archivo;
  struct t_dir directorio;

  if(dir->id_directorio!=0) {
	  /*Actualizo "hijos" en padre viejo*/
	  query = BCON_NEW ("_id", BCON_INT32(dir->parent_dir->id_directorio));
	  update = BCON_NEW ("$pull", "{",
								  "directorios", BCON_INT32 (dir->id_directorio),
								  "}");
	  if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
		  bson_destroy (query);
		  bson_destroy (update);
		  return;
	  }
	  bson_destroy (update);
	  bson_destroy (query);
  }

  /*Busco el directorio*/
  query = bson_new ();
  BSON_APPEND_INT32 (query, "_id", dir->id_directorio);
  if(!mongoc_collection_count (directorioCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
    printf("Error: No existe el directorio\n");
    bson_destroy (query);
    return;
  }

  /*Borro los archivos y subdirectiorios*/
  /*Archivos: */
  cursor = mongoc_collection_find(directorioCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

  while (mongoc_cursor_next (cursor, &doc)) {
    if (bson_iter_init_find (&iter, doc, "archivos") &&
        BSON_ITER_HOLDS_ARRAY (&iter) &&
        bson_iter_recurse (&iter, &sub_iter)) {
            while (bson_iter_next (&sub_iter)) {
                archivo.id_archivo = (int) bson_iter_int32(&sub_iter);
            	archivo.parent_dir = dir;
            	eliminarArchivo(&archivo);
            }
    }
  }
  mongoc_cursor_destroy (cursor);

  /*Subdirectorios: */
  cursor = mongoc_collection_find(directorioCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

  while (mongoc_cursor_next (cursor, &doc)) {
    if (bson_iter_init_find (&iter, doc, "directorios") &&
        BSON_ITER_HOLDS_ARRAY (&iter) &&
        bson_iter_recurse (&iter, &sub_iter)) {
            while (bson_iter_next (&sub_iter)) {
                directorio.id_directorio = (int) bson_iter_int32(&sub_iter);
                directorio.parent_dir = dir;
            	eliminarDirectorio(&directorio);
            }
    }
  }
  mongoc_cursor_destroy (cursor);

  /*Borro el directorio*/
  if (!mongoc_collection_remove (directorioCollection, MONGOC_DELETE_SINGLE_REMOVE, query, NULL, &error)) {
    printf ("Error: %s\n", error.message);
  }

  bson_destroy (query);

}

void crearArchivoEn(struct t_arch* archivoNuevo, struct t_dir * directorio){
    /*Recibo el archivo y lo escribo en mongo*/
    bson_t *doc;
    bson_t *query, *update;

    /*Creo el documento*/
    doc = bson_new();

    BSON_APPEND_INT32 (doc, "_id", (archivoNuevo -> id_archivo));
    BSON_APPEND_UTF8 (doc, "nombre", (archivoNuevo->nombre));
    BSON_APPEND_INT32 (doc, "idDirectorio", (directorio->id_directorio));
    BSON_APPEND_INT32 (doc, "cant_bloq", (archivoNuevo -> cant_bloq));

    /*Hago el insert en Archivos*/
    if (!mongoc_collection_insert (archivoCollection, MONGOC_INSERT_NONE, doc, NULL, NULL)) {
        printf ("Error insertando nuevo archivo\n");
        bson_destroy (doc);
        return;
    }

    void _crearbloques(struct t_bloque* block){
    	crearBloque(block, archivoNuevo->id_archivo);
    }

    list_iterate(archivoNuevo->bloques, (void*) _crearbloques);

    /*Actualizo "hijos" en padre nuevo*/
  	query = BCON_NEW ("_id", BCON_INT32(directorio->id_directorio));
    update = BCON_NEW ("$push", "{",
  	  	  	  	  	  "archivos", BCON_INT32 (archivoNuevo->id_archivo),
  				  	  "}");
    if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  NULL)) {
    	printf ("Error update directorio padre nuevo\n");
  	  	bson_destroy (query);
        bson_destroy (update);
        bson_destroy (doc);
        return;
    }

    bson_destroy (query);
    bson_destroy (update);
    bson_destroy (doc);
}

void eliminarArchivo(struct t_arch * archivo){
  bson_error_t error;
  bson_t *query;
  const bson_t *doc;
  mongoc_cursor_t *cursor;
  int i;
  bson_iter_t iter;

  /*Busco el archivo*/
  query = bson_new ();
  BSON_APPEND_INT32 (query, "_id", archivo->id_archivo);
  BSON_APPEND_INT32 (query, "idDirectorio", archivo->parent_dir->id_directorio);
  if(!mongoc_collection_count (archivoCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
    printf("Error: No existe el archivo\n");
    bson_destroy (query);
    return;
  }

  /*Borro los bloques*/
  cursor = mongoc_collection_find(archivoCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

  while (mongoc_cursor_next (cursor, &doc)) {
    if(bson_iter_init_find (&iter, doc, "cant_bloq")){
            for(i=0; i<(bson_iter_int32(&iter)); i++){
                    eliminarBloque(archivo->id_archivo, i);
            }
    }
  }

  /*Borro el archivo*/
  if (!mongoc_collection_remove (archivoCollection, MONGOC_DELETE_SINGLE_REMOVE, query, NULL, &error)) {
    printf ("Error: %s\n", error.message);
  }

  mongoc_cursor_destroy (cursor);
  bson_destroy (query);
}

struct t_bloque * recibirBloqueDeMongo (int idArchivo, int nro_bloq){
	struct t_bloque * bloque;
	struct t_copia_bloq * copia;
	bson_t * query;
	const bson_t *doc;
	mongoc_cursor_t *cursor;
	bson_iter_t iter, array_iter, document_iter, nroCopia_iter, nodo_iter, bloque_iter;

	//Busco el bloque
	query = bson_new ();
	BSON_APPEND_INT32 (query, "idArchivo", idArchivo);
	BSON_APPEND_INT32 (query, "numero", nro_bloq);
	if(!mongoc_collection_count (bloqueCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
		printf("Error: No existe el bloque\n");
		bson_destroy (query);
		return NULL;
	}

	//Armo el bloque
	bloque = malloc(sizeof(struct t_bloque));
	bloque -> nro_bloq = nro_bloq;
	bloque -> list_copias = list_create();

	copia = malloc(sizeof(struct t_copia_bloq));

	//Armo las copias
	cursor = mongoc_collection_find(bloqueCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
	mongoc_cursor_next (cursor, &doc);
    if (bson_iter_init_find (&iter, doc, "copias") &&
        BSON_ITER_HOLDS_ARRAY (&iter) &&
        bson_iter_recurse (&iter, &array_iter)){
			while (bson_iter_next (&array_iter)) {
				if(BSON_ITER_HOLDS_DOCUMENT (&array_iter)&&
				   bson_iter_recurse (&array_iter, &document_iter)&&
				   bson_iter_find_descendant (&document_iter, "numeroCopia", &nroCopia_iter)&&
				   bson_iter_find_descendant (&document_iter, "nodo", &nodo_iter)&&
				   bson_iter_find_descendant (&document_iter, "bloque", &bloque_iter)){
					copia -> id_nodo = (int) bson_iter_int32(&nodo_iter);
					copia -> bloq_nodo = (int) bson_iter_int32(&bloque_iter);
					list_add_in_index((bloque->list_copias), (int) bson_iter_int32(&nroCopia_iter), copia);
				}
			}
    }

	mongoc_cursor_destroy (cursor);
	bson_destroy (query);
	return bloque;
}

struct t_arch * recibirArchivoDeMongo (int idArchivo, struct t_dir * directorioPadre){
	struct t_arch * archivo;
	bson_t * query;
	const bson_t *doc;
	mongoc_cursor_t *cursor;
	bson_iter_t iter, nombre_iter, cantBloques_iter;
	int i;

	//Busco el archivo
	query = bson_new ();
	BSON_APPEND_INT32 (query, "_id", idArchivo);
	if(!mongoc_collection_count (archivoCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
		printf("Error: No existe el archivo\n");
		bson_destroy (query);
		return NULL;
	}

	//Armo el archivo
	archivo = malloc(sizeof(struct t_arch));
	archivo -> id_archivo = idArchivo;
	archivo -> parent_dir = directorioPadre;
	archivo -> bloques = list_create();

	cursor = mongoc_collection_find(archivoCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
	mongoc_cursor_next (cursor, &doc);
    if (bson_iter_init (&iter, doc) &&
        bson_iter_find_descendant (&iter, "nombre", &nombre_iter)&&
		bson_iter_find_descendant (&iter, "cant_bloq", &cantBloques_iter)){
			archivo->nombre = strdup((char*) bson_iter_utf8(&nombre_iter, NULL));
			archivo->cant_bloq = bson_iter_int32(&cantBloques_iter);
	}

	//Armo los bloques del archivo
	for(i=0; i<(archivo->cant_bloq); i++){
		list_add_in_index((archivo->bloques), i, recibirBloqueDeMongo(idArchivo, i));
	}

	mongoc_cursor_destroy (cursor);
	bson_destroy (query);
	return archivo;
}

struct t_dir * recibirDirectorioDeMongo (int idDirectorio, struct t_dir * directorioPadre){
	struct t_dir * directorio;
	bson_t * query;
	const bson_t *doc;
	mongoc_cursor_t *cursor;
	bson_iter_t iter, nombre_iter, sub_iter;

	//Busco el directorio
	query = bson_new ();
	BSON_APPEND_INT32 (query, "_id", idDirectorio);
	if(!mongoc_collection_count (directorioCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
		printf("Error: No existe el directorio\n");
		bson_destroy (query);
		return NULL;
	}

	//Armo el directorio
	directorio = malloc(sizeof(struct t_dir));
	directorio -> id_directorio = idDirectorio;
	directorio -> parent_dir = directorioPadre;
	directorio -> list_archs = list_create();
	directorio -> list_dirs = list_create();

	cursor = mongoc_collection_find(directorioCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
	mongoc_cursor_next (cursor, &doc);
    if (bson_iter_init (&iter, doc) &&
        bson_iter_find_descendant (&iter, "nombre", &nombre_iter)){
			directorio->nombre = strdup((char*) bson_iter_utf8(&nombre_iter, NULL));
	}

	//Armo la lista de archivos
    if (bson_iter_init_find (&iter, doc, "archivos") &&
        BSON_ITER_HOLDS_ARRAY (&iter) &&
        bson_iter_recurse (&iter, &sub_iter)) {
            while (bson_iter_next (&sub_iter)) {
				list_add((directorio->list_archs), recibirArchivoDeMongo((int) bson_iter_int32(&sub_iter), directorio));
			}
	}

	//Armo la lista de directorios
    if (bson_iter_init_find (&iter, doc, "directorios") &&
        BSON_ITER_HOLDS_ARRAY (&iter) &&
        bson_iter_recurse (&iter, &sub_iter)) {
            while (bson_iter_next (&sub_iter)) {
				list_add((directorio->list_dirs), recibirDirectorioDeMongo(bson_iter_int32(&sub_iter), directorio));
			}
	}


	bson_destroy (query);
	mongoc_cursor_destroy (cursor);
	return directorio;
}

void iniciarMongo(){
	mongoc_init ();
	client = mongoc_client_new ("mongodb://localhost:27017/");

	directorioCollection = mongoc_client_get_collection (client, "test", "Directorios");
	archivoCollection = mongoc_client_get_collection (client, "test", "Archivos");
	bloqueCollection = mongoc_client_get_collection (client, "test", "Bloques");
	nodoCollection = mongoc_client_get_collection (client, "test", "Nodos");
}

void cerrarMongo(){
	mongoc_collection_destroy (bloqueCollection);
	mongoc_collection_destroy (archivoCollection);
	mongoc_collection_destroy (directorioCollection);
	mongoc_collection_destroy (nodoCollection);
	mongoc_client_destroy (client);

	mongoc_cleanup ();
}

struct t_dir * levantarRaizDeMongo(){
	struct t_dir * raiz;
	bson_t * query;


	query = bson_new ();
	BSON_APPEND_INT32 (query, "_id", 0);
	if(!mongoc_collection_count (directorioCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
		raiz = malloc(sizeof(struct t_dir));
			raiz -> id_directorio = 0;
			raiz -> nombre = strdup("root");
			raiz -> parent_dir = NULL;
			raiz -> list_archs = list_create();
			raiz -> list_dirs = list_create();
		crearDirectorioEn(raiz,NULL);
	}
	else
		raiz = recibirDirectorioDeMongo(0, NULL);

	bson_destroy (query);
	return raiz;
}

int ultimoIdDirectorio(){
	/*Retorna el último id_directorio guardado en mongo*/
	int id = 0;
	bson_t * query;
	const bson_t * doc;
	mongoc_cursor_t *cursor;
	bson_iter_t iter, id_iter;

	query = bson_new ();
	cursor = mongoc_collection_find(directorioCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
	while(mongoc_cursor_next (cursor, &doc)){
		if (bson_iter_init (&iter, doc) &&
			bson_iter_find_descendant (&iter, "_id", &id_iter)){
				if( id<(bson_iter_int32(&id_iter)) ) id = bson_iter_int32(&id_iter);
		}
	}

	bson_destroy (query);
	mongoc_cursor_destroy (cursor);
	return id;
}

int ultimoIdArchivo(){
	/*Retorna el último id_archivo guardado en mongo*/
	int id = 0;
	bson_t * query;
	const bson_t * doc;
	mongoc_cursor_t *cursor;
	bson_iter_t iter, id_iter;

	query = bson_new ();
	cursor = mongoc_collection_find(archivoCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
	while(mongoc_cursor_next (cursor, &doc)){
		if (bson_iter_init (&iter, doc) &&
			bson_iter_find_descendant (&iter, "_id", &id_iter)){
				if( id<(bson_iter_int32(&id_iter)) ) id = bson_iter_int32(&id_iter);
		}
	}

	bson_destroy (query);
	mongoc_cursor_destroy (cursor);
	return id;
}

void moverDirectorio(struct t_dir* dir, struct t_dir* parent_dir, char* new_name){
	bson_t *query, *update;
	bson_error_t error;

	/*Actualizo "hijos" en padre nuevo*/
	query = BCON_NEW ("_id", BCON_INT32(parent_dir->id_directorio));
	update = BCON_NEW ("$push", "{",
								 "directorios", BCON_INT32 (dir->id_directorio),
								 "}");
	if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
		  bson_destroy (query);
		  bson_destroy (update);
		  return;
	}

	bson_destroy (update);
	bson_destroy (query);

	/*Actualizo "hijos" en padre viejo*/
	query = BCON_NEW ("_id", BCON_INT32(dir->parent_dir->id_directorio));
	update = BCON_NEW ("$pull", "{",
								 "directorios", BCON_INT32 (dir->id_directorio),
								 "}");
	if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
		  bson_destroy (query);
		  bson_destroy (update);
		  return;
	}

	bson_destroy (update);
	bson_destroy (query);

	/*Actualizo dir*/
	query = BCON_NEW ("_id", BCON_INT32(dir->id_directorio));
	update = BCON_NEW ("$set", "{",
								 "nombre", BCON_UTF8 (new_name),
								 "}");
	if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
	}

	bson_destroy (update);
	bson_destroy (query);
}

void crearNodo(struct t_nodo * nodo){
  bson_t *doc;
  bson_error_t error;

  doc = BCON_NEW("_id", BCON_INT32(nodo->id_nodo),
		  	  	 "cantidad_bloques", BCON_INT32(nodo->cantidad_bloques)
				 );

  //Inserto en la coleccion
  if (!mongoc_collection_insert (nodoCollection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
      printf ("%s\n", error.message);
  }

  bson_destroy(doc);
}

void eliminarNodo(struct t_nodo* nodo){
  bson_error_t error;
  bson_t *query;

  /*Busco el nodo*/
  query = bson_new ();
  BSON_APPEND_INT32 (query, "_id", nodo->id_nodo);
  if(!mongoc_collection_count (nodoCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
    printf("Error: No existe el directorio\n");
    bson_destroy (query);
    return;
  }

  /*Borro el nodo*/
  if (!mongoc_collection_remove (nodoCollection, MONGOC_DELETE_SINGLE_REMOVE, query, NULL, &error)) {
    printf ("Error: %s\n", error.message);
  }

  bson_destroy (query);
}

void levantarNodos(t_list* lista_nodos){
  struct t_nodo * nodo;
  bson_t *query;
  const bson_t * doc;
  mongoc_cursor_t *cursor;
  bson_iter_t iter, id_iter;

  /*Busco el nodo*/
  query = bson_new ();
  if(!mongoc_collection_count (nodoCollection, MONGOC_QUERY_NONE, query, 0, 0, NULL, NULL)){
    bson_destroy (query);
    return;
  }

  cursor = mongoc_collection_find(nodoCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
  while(mongoc_cursor_next (cursor, &doc)){
	  	nodo = malloc(sizeof(struct t_nodo));
	    if (bson_iter_init (&iter, doc) &&
			bson_iter_find_descendant (&iter, "_id", &id_iter)){
				nodo->id_nodo = bson_iter_int32(&id_iter);
		}
		if (bson_iter_init (&iter, doc) &&
			bson_iter_find_descendant (&iter, "cantidad_bloques", &id_iter)){
				nodo->cantidad_bloques = bson_iter_int32(&id_iter);
		}
		nodo->estado = DESCONECTADO;
		nodo->bloquesLlenos = kbitarray_create_and_clean_all(nodo->cantidad_bloques);
		nodo->usando_socket = 0;
		nodo->socket_FS_nodo = 0;
		list_add(lista_nodos, nodo);
  }

  mongoc_cursor_destroy (cursor);
  bson_destroy (query);
}

void moverArchivo(struct t_arch* arch, struct t_dir* parent_dir, char* new_name){
	bson_t *query, *update;
	bson_error_t error;

	/*Actualizo "hijos" en padre nuevo*/
	query = BCON_NEW ("_id", BCON_INT32(parent_dir->id_directorio));
	update = BCON_NEW ("$push", "{",
								 "archivos", BCON_INT32 (arch->id_archivo),
								 "}");
	if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
		  bson_destroy (query);
		  bson_destroy (update);
		  return;
	}

	bson_destroy (update);
	bson_destroy (query);

	/*Actualizo "archivos" en padre viejo*/
	query = BCON_NEW ("_id", BCON_INT32(arch->parent_dir->id_directorio));
	update = BCON_NEW ("$pull", "{",
								 "archivos", BCON_INT32 (arch->id_archivo),
								 "}");
	if (!mongoc_collection_update (directorioCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
		  bson_destroy (query);
		  bson_destroy (update);
		  return;
	}

	bson_destroy (update);
	bson_destroy (query);

	/*Actualizo arch*/
	query = BCON_NEW ("_id", BCON_INT32(arch->id_archivo));
	update = BCON_NEW ("$set", "{",
								 "nombre", BCON_UTF8 (new_name),
								 "idDirectorio", BCON_INT32(parent_dir->id_directorio),
								 "}");
	if (!mongoc_collection_update (archivoCollection, MONGOC_UPDATE_NONE, query, update, NULL,  &error)) {
		  printf ("%s\n", error.message);
	}

	bson_destroy (update);
	bson_destroy (query);
}
