A simple RDMA server-client example.

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
