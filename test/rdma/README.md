A simple RDMA server-client example.

## Setting RDMA 
-------------
```bash
apt-get install -y chrpath libnl-3-dev libnl-route-3-dev gfortran tcl libusb-1.0-0 quilt linux-headers-4.14.1_newport_0.1+ ethtool pciutils dpatch tk libpci3 debhelper graphviz lsof libfuse2 swig pkg-config
```

```bash
wget http://www.mellanox.com/downloads/ofed/MLNX_OFED-5.4-3.5.8.0/MLNX_OFED_LINUX-5.4-3.5.8.0-ubuntu18.04-x86_64.tgz
tar -xvf MLNX_OFED_LINUX-5.4-3.5.8.0-ubuntu18.04-x86_64.tgz
cd MLNX_OFED_LINUX-5.4-3.5.8.0-ubuntu18.04-x86_64
./mlnxofedinstall
```

```bash
/etc/init.d/openibd restart
/etc/init.d/opensmd restart
```

```bash
nmtui
```

## How To Run
-------------
```bash
g++ -o server server.cc -libverbs -lrdmacm -pthread
g++ -o client client.cc -libverbs -lrdmacm -pthread
```
server
```bash
./server
```
client
```bash
./client SERVER_IP
```
