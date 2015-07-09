/*
 * connectionlib.c
 *
 *  Created on: 24/5/2015
 *      Author: utnso
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdint.h> //Esta la agregeue para poder definir int con tamaño especifico (uint32_t)
#include <ifaddrs.h>
#include <string.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include "../connectionlib/connectionlib.h"

#define BACKLOG 5

//############################# Buffer Functions ##################################################
//---------------------------------------------------------------------------
t_buffer* buffer_create() {
	t_buffer* self;
	self = malloc(sizeof(t_buffer));
	self->buffer = NULL;
	self->size = 0;

	return self;
}


//---------------------------------------------------------------------------
t_buffer* buffer_create_with_protocol(uint32_t protocolo) {

	t_buffer* self = buffer_create();

	buffer_add_int(self,protocolo);
	return self;
}

//---------------------------------------------------------------------------
void buffer_add_string(t_buffer* self, char *string_to_add) {
	uint32_t size_string = strlen(string_to_add) + 1;
	buffer_add_int(self,size_string);

	off_t offset_to_write = self->size;

	self->size += size_string;
	self->buffer = realloc(self->buffer, self->size);

	memcpy((self->buffer + offset_to_write), string_to_add, size_string);
}

//---------------------------------------------------------------------------
void buffer_add_int(t_buffer* self, uint32_t int_to_add) {

	off_t offset_to_write = self->size;

	self->size += sizeof(uint32_t);
	self->buffer = realloc(self->buffer, self->size);

	int_to_add = htonl(int_to_add);
	memcpy((self->buffer + offset_to_write), &int_to_add, sizeof(uint32_t));
}

//---------------------------------------------------------------------------
void buffer_add_buffer_and_destroy_added(t_buffer* self, t_buffer* added) {

	off_t offset_to_write = self->size;

	self->size += added->size;
	self->buffer = realloc(self->buffer, self->size);

	memcpy((self->buffer + offset_to_write), added->buffer, added->size);

	buffer_destroy(added);
}


//---------------------------------------------------------------------------
void buffer_destroy(t_buffer* self) {
	free(self->buffer);
	free(self);
}


//---------------------------------------------------------------------------
int send_buffer(int socket, t_buffer* self) {
	return send_stream_without_size(socket, self->buffer, self->size);
}

//---------------------------------------------------------------------------
int send_buffer_and_destroy(int socket, t_buffer* self) {
	int result = send_buffer(socket, self);
	buffer_destroy(self);

	return result;
}



//################################ Send and Receive High Level ####################################
//---------------------------------------------------------------------------
int send_protocol_in_order(int socket, uint32_t protocol) {
	return send_int_in_order(socket,protocol);
}

//---------------------------------------------------------------------------
int send_int_in_order(int socket, uint32_t entero) {
	entero = htonl(entero);
	return send_stream_without_size(socket,&entero,sizeof(uint32_t));
}

//---------------------------------------------------------------------------
int send_stream_with_size_in_order(int socket, void *buffer, uint32_t size_buffer) {
	int result;

	if((result = send_int_in_order(socket,size_buffer)) <= 0) {
		return result;
	}
	return send_stream_without_size(socket,buffer,size_buffer);
}


//---------------------------------------------------------------------------
uint32_t receive_protocol_in_order(int socket) {
	uint32_t prot;

	int result = receive_int_in_order(socket,&prot);

	if (result == 0)
		return DISCONNECTED;

	if (result == -1)
		return -1;

	return prot;
}

//---------------------------------------------------------------------------
int receive_int_in_order(int socket, uint32_t *number) {

	int result;

	result = receive_stream_without_size(socket, number, sizeof(uint32_t));
	*number = ntohl(*number);

	return result;
}

//---------------------------------------------------------------------------
int receive_static_array_in_order(int socket, void *buffer) {
	uint32_t result;
	uint32_t size_buffer;
	if((result = receive_int_in_order(socket, &size_buffer)) <= 0) {
		return result;
	}
	return receive_stream_without_size(socket, buffer, size_buffer);
}

//---------------------------------------------------------------------------
int receive_dinamic_array_in_order(int socket, void** buffer) {
	uint32_t result;
	uint32_t size_buffer;

	*buffer = malloc(sizeof(char)); //para que se tome en cuenta de que cada vez que esta funcion es llamada hace un malloc
	if((result = receive_int_in_order(socket, &size_buffer)) <= 0) {
		return result;
	}
	free(*buffer);

	*buffer = malloc(size_buffer);
	return receive_stream_without_size(socket, * buffer, size_buffer);
}


//################################ Send and Receive Low Level #####################################

//---------------------------------------------------------------------------
int send_stream_without_size(int socket, void *buffer, uint32_t size_buffer) {
	uint32_t size_sended = 0;
	uint32_t sending;

	while (size_sended < size_buffer) {
		if ((sending = send(socket, (buffer + size_sended), (size_buffer - size_sended), 0)) == -1) {
			mostrar_error(-1, "Error sending");
			return -1;
		}
		if (sending == 0) {
			return 0;
		}
		size_sended += sending;
	}
	return size_sended;
}

//---------------------------------------------------------------------------
int receive_stream_without_size(int socket, void* buffer, uint32_t size_buffer) {
	uint32_t size_received = 0;
	uint32_t receiving;

	while (size_received < size_buffer) {
		if ((receiving = recv(socket, (buffer + size_received), (size_buffer - size_received), 0)) == -1) {
			mostrar_error(-1, "Error receiving");
			return -1;
		}
		if (receiving == 0) {
			return 0;
		}
		size_received += receiving;
	}
	return size_received;
}

//########################### Write and Read Low Level ############################################

//---------------------------------------------------------------------------
int write_stream(int fd, void* buffer, uint32_t size_buffer) {
	uint32_t size_written = 0;
	uint32_t writing;

	while (size_written < size_buffer) {
		if ((writing = write(fd, (buffer + size_written), (size_buffer - size_written))) == -1) {
			mostrar_error(-1, "Error Writing");
			return -1;
		}
		if (writing == 0) {
			return 0;
		}
		size_written += writing;
	}
	return size_written;
}

//---------------------------------------------------------------------------
int read_stream(int fd, void* buffer, uint32_t size_buffer) {
	uint32_t size_read = 0;
	uint32_t reading;

	while (size_read < size_buffer) {
		if ((reading = read(fd, (buffer + size_read), (size_buffer - size_read))) == -1) {
			mostrar_error(-1, "Error Writing");
			return -1;
		}
		if (reading == 0) {
			return 0;
		}
		size_read += reading;
	}
	return size_read;
}

//############################# Read & Send , Recieve & Write #####################################

//---------------------------------------------------------------------------
int send_from_file_by_parts(int socket, int fd, size_t max_part_length, uint32_t total_size) {

	size_t readed = 0;
	char* buffer;
	int to_be_read;

	int result = send_int_in_order(socket, total_size);
	while((readed < total_size) && (result > 0)) {
		if(total_size - readed < max_part_length) {
			to_be_read = total_size - readed;
		} else {
			to_be_read = max_part_length;
		}
		buffer = malloc(to_be_read);
		result = read_stream(fd, buffer, to_be_read);
		result = (result > 0) ? send_stream_without_size(socket, buffer, to_be_read) : result;
		free(buffer);
		readed += result;
	}

	if(result <= 0) {
		mostrar_error(result, "Error sending from file");
	}

	return result;
}

//---------------------------------------------------------------------------
int receive_in_file_by_parts(int socket, int fd, size_t max_part_length) {

	char* buffer;
	uint32_t total_size;
	int to_be_written;
	size_t written = 0;

	int result = receive_int_in_order(socket, &total_size);
	while((written < total_size) && (result > 0)) {
		if(total_size - written < max_part_length) {
			to_be_written = total_size - written;
		} else {
			to_be_written = max_part_length;
		}
		buffer = malloc(to_be_written);
		result = receive_stream_without_size(socket, buffer, to_be_written);
		result =  (result > 0) ? write_stream(fd, buffer, to_be_written) : result;
		free(buffer);
		written += to_be_written;
	}

	if(result <= 0) {
		mostrar_error(result, "Error receiving in file");
	}

	return result;
}

//---------------------------------------------------------------------------
int send_entire_file_by_parts(int socket, char* src_path, size_t max_part_length) {
	int result;

	struct stat stat_file;
	if( stat(src_path, &stat_file) == -1) {
		mostrar_error(-2, "Error while stat");
		result = send_protocol_in_order(socket, ERROR_WITH_FILE);
		return (result > 0) ? -2 : result;
	}

	int file_descriptor;
	file_descriptor = open(src_path, O_RDONLY);
	if (file_descriptor != -1) {
		result = send_protocol_in_order(socket, HERE_COMES_FILE);
		result = (result > 0) ? send_from_file_by_parts(socket, file_descriptor, max_part_length, stat_file.st_size) : result;
		close(file_descriptor);
	} else {
		mostrar_error(-2, "Error opening file");
		result = send_protocol_in_order(socket, ERROR_WITH_FILE);
		return (result > 0) ? -2 : result;
	}

	if(result <= 0) {
		mostrar_error(result, "Error sending file");
	}

	return result;
}

//---------------------------------------------------------------------------
int receive_entire_file_by_parts(int socket, char* dest_path, size_t max_part_length) {

	int result;
	int file_descriptor;

	result = receive_protocol_in_order(socket);

	if(result <= 0) {
		return result;
	}

	if(result == ERROR_WITH_FILE) {
		return -2;
	}

	if(result != HERE_COMES_FILE) {
		return -2;
	}

	file_descriptor = creat(dest_path, S_IRWXU);
	if (file_descriptor != -1) {
		result = receive_in_file_by_parts(socket, file_descriptor, max_part_length);
		close(file_descriptor);
	} else {
		mostrar_error(-2, "Error creating file");
		return -2;
	}

	if(result <= 0) {
		mostrar_error(result, "Error receiving file");
	}

	return result;
}

//############################# Aparaeos #####################################

//-------------------------------------------------------------------------------------------------
void free_file_with_line(t_file_with_line* self) {
	if(self->line != NULL) {
		free(self->line);
	}
	free(self);
}

//-------------------------------------------------------------------------------------------------
int take_next_merged_line(t_list* files_with_lines, char** next_line) {

	t_file_with_line* _minLineInFile(t_file_with_line* file1,t_file_with_line* file2){
		if(file2->line == NULL) {
			return file1;
		}
		if(file1->line == NULL) {
			return file2;
		}

		if(strcmp(file1->line,file2->line)<0) {
			return file1;
		} else {
			return file2;
		}
	}

	t_file_with_line* file_ahead = foldl1((void *) _minLineInFile, files_with_lines);

	if(file_ahead == NULL) {
		return 0;
	}

	if(file_ahead->line == NULL) {
		return 0;
	}

	*next_line = strdup(file_ahead->line);

	size_t len = strlen(file_ahead->line);
	int result = getline(&(file_ahead->line), &len, file_ahead->fp);

	if(result <= 0) {
		list_remove_element(files_with_lines, (void *) file_ahead);
		fclose(file_ahead->fp);
		free_file_with_line(file_ahead);
	}

	return 1;
}

//-------------------------------------------------------------------------------------------------
int open_files_to_merge(t_list* paths, t_list** files_with_lines) {

	*files_with_lines = list_create();

	int any_file_lost = 0;

	void _open_file_and_take_first_line(char* path) {
		if(!any_file_lost) {
			FILE* fp = fopen(path, "r");
			char* line = NULL;
			if(fp != NULL) {
				size_t len = 0;
				int read = getline(&line, &len, fp);

				if(read > 0) {
					t_file_with_line* file_to_merge = malloc(sizeof(t_file_with_line));
					file_to_merge->fp = fp;
					file_to_merge->line = line;

					list_add(*files_with_lines, file_to_merge);
				} else {
					if(line != NULL) {
						free(line);
					}
				}
			} else {
				mostrar_error(-1,path);
				any_file_lost = 1;
			}
		}
	}

	list_iterate(paths, (void *) _open_file_and_take_first_line);

	if(any_file_lost) {
		list_destroy_and_destroy_elements(*files_with_lines, (void *) free_file_with_line);
		return -1;
	} else {
		return 1;
	}

}


//############################### Connections and Directions ######################################
//---------------------------------------------------------------------------
void mostrar_error(int number, char* cause) {
	perror(cause);
//	quizas debería distinguirse error insalvable de error ignorable
}

//---------------------------------------------------------------------------
int solicitarConexionCon(char* server_ip, int server_port) {
	struct sockaddr_in socketaddr_server;

	setSocketAddrStd(&socketaddr_server, server_ip, server_port);

	int socketfd_server;

	if ((socketfd_server = socket(AF_INET, SOCK_STREAM, 0)) == -1) { // crea un Socket
		mostrar_error(-1, "Error while socket()");
		return -1;
	}

	if (connect(socketfd_server, (struct sockaddr*) &socketaddr_server, sizeof(struct sockaddr)) == -1) { // conecta con el servidor
		mostrar_error(-1, "Error while connect()");
		return -1;
	}
	return socketfd_server;
}

//---------------------------------------------------------------------------
int escucharConexionesDesde(char* server_ip, int server_port) {
	struct sockaddr_in socketaddr_server;

	setSocketAddrStd(&socketaddr_server, server_ip, server_port);

	int listener;

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) { //Crea un Socket
		mostrar_error(-1, "Error while socket()");
		return -1;
	}

	int yes = 1;
	if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
		mostrar_error(-1, "setsockopt");
		return -1;
	}

	if (bind(listener, (struct sockaddr*) &socketaddr_server, sizeof(struct sockaddr)) == -1) { //Asignar puerto de escucha
		mostrar_error(-1, "Error while bind()");
		return -1;
	}

	if (listen(listener, BACKLOG) == -1) { //escuchar por clientes
		mostrar_error(-1, "Error while listen()");
		return -1;
	}

	return listener;
}

//--------------------------------------------------------------------
int aceptarCliente(int listener, struct sockaddr_in* direccionCliente) {
	int sin_size = sizeof(struct sockaddr_in);
	int nuevo_socket;

	if ((nuevo_socket = accept(listener, (struct sockaddr*) direccionCliente, (socklen_t*) &sin_size)) == -1) { //Crea un Socket
		mostrar_error(-1, "Error while accept()");
		return -1;
	}

	return nuevo_socket;
}

//---------------------------------------------------------------------------
char* get_IP() { //ojala sirva para algo jaja
	struct ifaddrs *interface_addr;
	struct sockaddr_in* sock_addr;
	char* addr;

	getifaddrs(&interface_addr);
	while (interface_addr) {
		if (interface_addr->ifa_addr->sa_family == AF_INET && strcmp(interface_addr->ifa_name, "eth0") == 0) {
			sock_addr = (struct sockaddr_in*) interface_addr->ifa_addr;
			addr = inet_ntoa(sock_addr->sin_addr);
		}
		interface_addr = interface_addr->ifa_next;
	}
	freeifaddrs(interface_addr);
	return addr;
}

//---------------------------------------------------------------------------
void setSocketAddrStd(struct sockaddr_in* address, char* ip, int port) {
	address->sin_family = AF_INET; // familia de direcciones (siempre AF_INET)
	address->sin_port = htons(port); // setea Puerto a conectarme

	if ((strlen(ip) != 0) && (strcmp(ip,"ANY IP")!=0)) {
		address->sin_addr.s_addr = inet_addr(ip); // Setea Ip a conectarme
	} else {
		address->sin_addr.s_addr = htonl(INADDR_ANY); // escucha todas las conexiones
	}

	memset(&(address->sin_zero), '\0', 8); // pone en ceros los bits que sobran de la estructura
}

//---------------------------------------------------------------------------
void getFromSocketAddrStd(struct sockaddr_in address, char** ip, int* port) {

	*port = ntohs(address.sin_port);

	if (htonl(INADDR_ANY) != address.sin_addr.s_addr) {
		*ip = strdup(inet_ntoa(address.sin_addr)); // Ip especificada
	} else {
		*ip = strdup("ANY IP"); // Cualquier Ip
	}
}

//---------------------------------------------------------------------------
char* from_int_to_inet_addr(uint32_t ip_int) {
	struct in_addr sin_addr;
	sin_addr.s_addr = ip_int;
	char *ip;

	if (htonl(INADDR_ANY) != sin_addr.s_addr) {
		ip = strdup(inet_ntoa(sin_addr)); // Ip especificada
	} else {
		ip = strdup("ANY IP"); // Cualquier Ip
	}

	return ip;
}


//############################### Should be in another library ####################################
//---------------------------------------------------------------------------
void free_string_splits(char** strings) {
	char **aux = strings;

	while (*aux != NULL) {
		free(*aux);
		aux++;
	}
	free(strings);
}

//---------------------------------------------------------------------------
int has_all_properties(int cant_properties, char** properties, t_config* conf_arch) {
	int i;

	for (i = 0; (i < cant_properties) && (config_has_property(conf_arch, properties[i])); i++)
		;

	if (i < cant_properties) {
		for (i = 0; i < cant_properties; i++) {
			if (!config_has_property(conf_arch, properties[i])) {
				printf("Error: el archivo de conf no tiene %s\n", properties[i]);
			}
		}
		return 0;
	}
	return 1;
}

//-------------------------------------------------------------------------------------
void leerStdin(char *leido, int maxLargo) {
	fgets(leido, maxLargo, stdin);
	if ((strlen(leido) > 0) && (leido[strlen(leido) - 1] == '\n')) {
		leido[strlen(leido) - 1] = '\0';
	}
}

//-------------------------------------------------------------------------------------
void string_static_trim_right(char* string) {
	int i;

	for(i=strlen(string)-1; (i>=0) && (string[i] == ' '); i--) {
		string[i] = string[i+1];
	}
}

//-------------------------------------------------------------------------------------
void string_static_trim_left(char* string) {
	int i,j;

	for (i = 0; (string[i] != '\0') && (string[i] == ' '); i++);

	if(i==0)
		return;

	for(j=i; (string[j-1] != '\0'); j++) {
		string[j-i] = string[j];
	}
}

//-------------------------------------------------------------------------------------
void string_static_trim(char* string) {
	string_static_trim_left(string);
	string_static_trim_right(string);
}

//---------------------------------------------------------------------------
int string_split_size(char** matriz){
	int i;
	for (i=0;matriz[i]!=NULL;i++);
	return i;
}

//---------------------------------------------------------------------------
int contains(void* elem, t_list* list){
	int _eq(void* any_elem){
		return elem == any_elem;
	}
	return list_any_satisfy(list, (void*) _eq);
}

//---------------------------------------------------------------------------
void* mayorSegun(void* elemento1, void* elemento2, int(*criterio)(void*)) {
	if(criterio(elemento1) > criterio(elemento2))
		return elemento1;

	return elemento2;
}

//---------------------------------------------------------------------------
void* menorSegun(void* elemento1, void* elemento2, int(*criterio)(void*)) {
	if(mayorSegun(elemento1,elemento2,criterio) == elemento2)
		return elemento1;

	return elemento2;
}

//---------------------------------------------------------------------------
void* foldl(void*(*function)(void*, void*), void* seed, t_list* list) {

	void* result = seed;

	void _subFunction(void* element) {
		result = function(result, element);
	}

	list_iterate(list,_subFunction);

	return result;
}


//---------------------------------------------------------------------------
void* foldl1(void*(*function)(void*, void*), t_list* list) {

	void* result = NULL;
	int primero = 1;

	void _subFunction(void* element) {
		if(primero) {
			result = element;
			primero = 0;
		} else {
			result = function(result, element);
		}
	}

	list_iterate(list,_subFunction);

	return result;
}

//---------------------------------------------------------------------------
void list_remove_element(t_list* self, void* element) {
	bool _eq(void* list_element){
		return list_element == element;
	}
	list_remove_by_condition(self, _eq);
}

//---------------------------------------------------------------------------
void list_remove_all_elements_in(t_list* self, t_list* to_be_removed) {
	void _remove_in_list(void* element){
		list_remove_element(self, element);
	}

	list_iterate(to_be_removed, _remove_in_list);
}
