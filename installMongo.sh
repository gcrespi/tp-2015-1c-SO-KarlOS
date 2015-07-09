#!/bin/bash
echo "Installing mongodb"
sudo apt-get update
sudo apt-get install -y --force-yes mongodb

echo "staring mongodb"
sudo /usr/bin/mongod --config /etc/mongodb.conf


tail -2 /var/log/mongodb/mongodb.log 


echo "Installing libraries.."
sudo apt-get update
sudo apt-get install -y --force-yes gcc automake autoconf libtool


echo "Getting mongoDriver.."

wget https://github.com/mongodb/mongo-c-driver/releases/download/1.1.7/mongo-c-driver-1.1.7.tar.gz
tar xzf mongo-c-driver-1.1.7.tar.gz
cd mongo-c-driver-1.1.7
./configure
make
sudo make install


echo "Deleting installer.."
cd ..
rm -rf mongo-c-driver-1.1.7
rm mongo-c-driver-1.1.7.tar.gz


echo "Adding Library.." 
echo "include /usr/local/lib" | sudo tee -a /etc/ld.so.conf
sudo ldconfig


