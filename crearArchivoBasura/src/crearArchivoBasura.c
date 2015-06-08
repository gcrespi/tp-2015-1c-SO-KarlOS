/*
 ============================================================================
 Name        : crearArchivoBasura.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

int main(void) {

	int fd,i;
	char buffer[50];

	if ((fd = open("/home/gustavo/git/tp-2015-1c-karlos/fileSystem/Debug/archivoNodo.dat", O_CREAT | O_WRONLY)) == -1) {
		perror("open");
		exit(1);
	}

    for(i=0;i<1024;i++) {
    	sprintf(buffer,"%07u*Linea De Basura de 45 bytes         \n",i);
    	if ((write(fd,buffer,45)) == -1) {
			perror("write");
			exit(1);
		}
    }

	if ((fd = close(fd)) == -1) {
		perror("open");
		exit(1);
	}

	return EXIT_SUCCESS;
}
