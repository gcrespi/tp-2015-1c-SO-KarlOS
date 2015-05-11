#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

#define pathArchivoMapeado "/tmp/archivoBasura.dat"

int main(int argc, char *argv[])
{
    int fd, offset_byte;
    int offset_block=1;
    int block_size=20*1024*1024;
    char *data;
//    struct stat sbuf;

    if (argc != 2) {
        fprintf(stderr, "usage: mmapdemo offset\n");
        exit(1);
    }

    if ((fd = open(pathArchivoMapeado, O_RDONLY)) == -1) {
        perror("open");
        exit(1);
    }

    offset_byte = atoi(argv[1]);
    if (offset_byte < 0 || offset_byte > block_size-1) {
        fprintf(stderr, "mmapdemo: offset must be in the range 0-%d\n",(int)block_size-1);
        exit(1);
    }

    data = mmap((caddr_t)0, block_size, PROT_READ, MAP_SHARED, fd, offset_block*block_size);

    if (data == (caddr_t)(-1)) {
        perror("mmap");
        exit(1);
    }

    printf("byte at Block %d, offset %d is '%c'\n",offset_block, offset_byte, data[offset_byte]);

    return 0;
}
