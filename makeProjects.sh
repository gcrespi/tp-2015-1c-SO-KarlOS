#!/bin/bash

echo "Making libraries..."
cd kbitarray/Debug
make clean
make
cd ../..

cd connectionlib/Debug
make clean
make
cd ../..

cd mongobiblioteca/Debug
make clean
make
cd ../..

echo "Making process.."
cd fileSystem/Debug
make clean
make
cd ../..

cd job/Debug
make clean
make
cd ../..

cd marta/Debug
make clean
make
cd ../..

cd nodo/Debug
make clean
make
cd ../..
