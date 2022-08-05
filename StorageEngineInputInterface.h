#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <vector>
#include <unordered_map>
#include <iostream>
#include <thread>
#include <atomic>

#include <boost/thread.hpp>
#include <boost/asio.hpp>

#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

//#include "TableManager.h"
// #include "CSDScheduler.h"
#include "keti_type.h"
#include "buffer_manager.h"
#include "SnippetManager.h"

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;

TableManager tblManager;
Scheduler csdscheduler;
BufferManager bufma(csdscheduler,tblManager);
// sumtest sumt;

atomic<int> WorkID;
unordered_map<string,int> fullquerymap;
int totalrow = 0;
int key;

void accept_connection(int server_fd);
//int my_LBA2PBA(char* req,char* res);
int my_LBA2PBA(std::string &req_json,std::string &res_json);