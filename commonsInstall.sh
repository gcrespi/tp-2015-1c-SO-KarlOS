#!/bin/bash
echo "Instalando Commons..."

cd ..
git clone https://github.com/gcrespi/so-commons-library
cd so-commons-library
sudo make install
cd ..
cd tp-2015-1c-karlos
