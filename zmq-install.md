# How to install ZeroMQ on Ubuntu
Before installing, make sure you have installed all the needed packages
```
sudo apt-get install libtool pkg-config build-essential autoconf automake
sudo apt-get install libzmq-dev
```

# How to source code install 
## Install libsodium
```
git clone git://github.com/jedisct1/libsodium.git
cd libsodium
./autogen.sh
./configure && make check
sudo make install
sudo ldconfig
```
## Install zeromq
```
# latest version as of this post is 4.1.2
wget http://download.zeromq.org/zeromq-4.1.2.tar.gz
tar -xvf zeromq-4.1.2.tar.gz
cd zeromq-4.1.2
./autogen.sh
./configure && make check
sudo make install
sudo ldconfig
```

# Other Notes

When build zmq-related plugins, maybe you need to edit libraries/plugins/zmq_objects/CMakeLists.txt, please use 
```
whereis zmq

whereis libzmq.a
```
to locate ZeroMQ_LIBRARY_DIRS and ZeroMQ_INCLUDE_DIRS, before cmake remeber reset both the DIRs.
