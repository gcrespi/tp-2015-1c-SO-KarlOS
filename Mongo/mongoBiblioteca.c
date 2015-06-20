#include <libbson-1.0/bson.h>
#include <libmongoc-1.0/mongoc.h>
#include <stdio.h>
#include <string.h>
#include <commons/collections/list.h>
#include <commons/string.h>

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
 *     copia: [{numero: numeroCopia, nodo: numeroNodo, bloque: bloqueNodo}]
 * }
 *
 */
 
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
 
void crearBloque(struct t_bloque *, int);
void eliminarBloque(int , int );
void crearDirectorio(struct t_dir*);
void eliminarDirectorio(int );
void crearArchivo(struct t_arch*);
void eliminarArchivo(int idArchivo, int idDirectorioPadre);

int main(){
struct t_dir directorio;        //Directorio de prueba
struct t_arch archivo;            //Archivo de prueba
struct t_bloque bloquePrueba;    //Bloque de prueba
struct t_copia_bloq copia;        //Copia bloque de prueba

/*
//Asgino valores para cada prueba

directorio.id_directorio = 1;
directorio.parent_dir = NULL;
directorio.nombre = string_duplicate("Pepe");
directorio.list_dirs = list_create();
directorio.list_archs = list_create();


archivo.id_archivo = 12345;
archivo.parent_dir = &directorio;
archivo.nombre = string_duplicate("Pelado");
archivo.cant_bloq = 1;

list_add(directorio.list_archs, &archivo);


bloquePrueba.nro_bloq = 0;
copia.id_nodo = 700;
copia.bloq_nodo = 3;
bloquePrueba.list_copias = list_create();
list_add(bloquePrueba.list_copias, &copia);
copia.id_nodo = 321;
copia.bloq_nodo = 999;
list_add(bloquePrueba.list_copias, &copia);
*/

mongoc_init ();
client = mongoc_client_new ("mongodb://localhost:27017/");
directorioCollection = mongoc_client_get_collection (client, "test", "Directorios");
archivoCollection = mongoc_client_get_collection (client, "test", "Archivos");
bloqueCollection = mongoc_client_get_collection (client, "test", "Bloques");


//FUNCIONES PARA ARCHIVOS
    //crearArchivo(&archivo);
    //eliminarArchivo(12345, 1);

//FUNCIONES PARA DIRECTORIOS
    //crearDirectorio(&directorio);
    //eliminarDirectorio(1);

//FUNCIONES PARA BLOQUES
    //eliminarBloque( 12345 ,120);
    //crearBloque(&bloquePrueba, 12345);

mongoc_collection_destroy (bloqueCollection);
mongoc_collection_destroy (archivoCollection);
mongoc_collection_destroy (directorioCollection);
mongoc_client_destroy (client);
return 0;  }


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
    doc = bson_new();
    array = bson_new();
    bloque = bson_new();

    
    BSON_APPEND_INT32 (doc, "idArchivo", idArchivo);
    BSON_APPEND_INT32 (doc, "numero", (bloqueNuevo->nro_bloq));
 
     bson_append_array_begin (doc, "copias", -1, array);
    for(i=0; i<2 ; i++){                                //Hardcode en el for
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
        bson_destroy (array);
        bson_destroy (bloque);
        bson_destroy (doc);
        return;
    }
    printf("Se ha creado el bloque\n");
    
    bson_destroy (query);
    bson_destroy (array);
    bson_destroy (bloque);
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
  printf("Bloque eliminado\n");
 
  bson_destroy (query);
}
 
void crearDirectorio(struct t_dir* directorioNuevo){
  /*Recibo un directorio y lo inserto en la colección Directorios*/
 
  bson_t *doc;
  bson_error_t error;
  bson_t *array;
  int i=0;
  struct t_dir * directorioHijo;
  struct t_arch * archivo;
 
/*Inserto el directorio*/
  doc = bson_new();
  //Preparo los valores id y nombre
 
  BSON_APPEND_INT32 (doc, "_id", (directorioNuevo->id_directorio));
  BSON_APPEND_UTF8 (doc, "nombre", (directorioNuevo->nombre));
 
  //Preparo el array para directorios hijos
  array = bson_new();
  bson_append_array_begin (doc, "directorios", 11, array);
 
  if(directorioNuevo->list_dirs){
    for(i=0; i < list_size(directorioNuevo->list_dirs); i++){
          directorioHijo = list_get(directorioNuevo->list_dirs, i);
        BSON_APPEND_INT32 (array, "directorios", (directorioHijo->id_directorio));
    }
  }
 
  bson_append_array_end (doc, array);   

  //Preparo el array para los archivos
 
  array = bson_new();
  bson_append_array_begin (doc, "archivos", 8, array);
 
  if(directorioNuevo->list_archs){
    for(i=0; i < list_size(directorioNuevo->list_archs); i++){
          archivo = list_get((directorioNuevo->list_archs), i);
        BSON_APPEND_INT32 (array, "archivos", (archivo->id_archivo));
    }
  }

  bson_append_array_end (doc, array);   
 
  //Inserto en la coleccion
  if (!mongoc_collection_insert (directorioCollection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
    printf ("%s\n", error.message);
    bson_destroy (array);
    bson_destroy(doc);
    return;
  }
    
  printf("Se creó el directorio %s\n", (directorioNuevo->nombre));
    
  bson_destroy (array);
  bson_destroy(doc);
}

void eliminarDirectorio(int idDirectorio){
  bson_error_t error;
  bson_t *query;
  const bson_t *doc;
  bson_iter_t iter;
  bson_iter_t sub_iter;
  mongoc_cursor_t *cursor;
 

  /*Busco el directorio*/
  query = bson_new ();
  BSON_APPEND_INT32 (query, "_id", idDirectorio);
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
                eliminarArchivo((int) bson_iter_int32(&sub_iter), idDirectorio);
            }
    }
  }
 
  /*Subdirectorios: */
  cursor = mongoc_collection_find(directorioCollection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);
 
  while (mongoc_cursor_next (cursor, &doc)) {
    if (bson_iter_init_find (&iter, doc, "directorios") &&
        BSON_ITER_HOLDS_ARRAY (&iter) &&
        bson_iter_recurse (&iter, &sub_iter)) {
            while (bson_iter_next (&sub_iter)) {
                eliminarDirectorio((int) bson_iter_int32(&sub_iter));
            }
    }
  }
 
  /*Borro el directorio*/
  if (!mongoc_collection_remove (directorioCollection, MONGOC_DELETE_SINGLE_REMOVE, query, NULL, &error)) {
    printf ("Error: %s\n", error.message);
  }
  printf("Directorio eliminado\n");
 
  mongoc_cursor_destroy (cursor);
  bson_destroy (query);
}

void crearArchivo(struct t_arch* archivoNuevo){
    /*Recibo el archivo y lo escribo en mongo*/
    bson_t *doc;
 
    /*Creo el documento*/
    doc = bson_new();

    BSON_APPEND_INT32 (doc, "_id", (archivoNuevo -> id_archivo));
    BSON_APPEND_UTF8 (doc, "nombre", (archivoNuevo->nombre));
    BSON_APPEND_INT32 (doc, "idDirectorio", (archivoNuevo->parent_dir->id_directorio));
    BSON_APPEND_INT32 (doc, "cant_bloq", (archivoNuevo -> cant_bloq));
          
    /*Hago el insert en Archivos*/
    if (!mongoc_collection_insert (archivoCollection, MONGOC_INSERT_NONE, doc, NULL, NULL)) {
        printf ("Error insertando nuevo archivo\n");
        bson_destroy (doc);
        return;
    }
    printf("Se ha creado el archivo\n");
   
    bson_destroy (doc);
}

void eliminarArchivo(int idArchivo, int idDirectorioPadre){
  bson_error_t error;
  bson_t *query;
  const bson_t *doc;
  mongoc_cursor_t *cursor;
  int i;
  bson_iter_t iter;

  /*Busco el archivo*/
  query = bson_new ();
  BSON_APPEND_INT32 (query, "_id", idArchivo);
  BSON_APPEND_INT32 (query, "idDirectorio", idDirectorioPadre);
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
                    eliminarBloque(idArchivo, i);
            }
    }
  }
 
  /*Borro el archivo*/
  if (!mongoc_collection_remove (archivoCollection, MONGOC_DELETE_SINGLE_REMOVE, query, NULL, &error)) {
    printf ("Error: %s\n", error.message);
  }
  printf("Archivo eliminado\n");
 
  bson_destroy (query);
}

     
