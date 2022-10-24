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
#include <codecvt>

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
// #include "TableAccess.h"
// #include "QueryExecutor.h"
typedef unsigned char uchar;


#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace std;
using namespace rapidjson;

// struct alltabledata{
//     // vector<TableManager::ColumnSchema> schema;
//     // string tablename;
//     unordered_map<string, vector<TableManager::ColumnSchema>> tableschema;
// };

// struct TableQuery{
//     string TableName;
//     string Query;
// };





// FullQueryData parsingjson(string query);
// void parsingSelect(Value &Select, SelectQueryData &SelectQueryData);
// void parsingFrom(Value &From, unordered_map<string,TableQueryData> &parsingdata);
// void parsingWhere(Value &Where, unordered_map<string,TableQueryData> &parsingdata);
// void parsingOrder(Value &Order, SelectQueryData &SelectQueryData);
// void parsingUnion(Value &Query, FullQueryData &fullQueryData);
void accept_connection(int server_fd);
string getTableName(string Column);
//int my_LBA2PBA(char* req,char* res);
int my_LBA2PBA(std::string &req_json,std::string &res_json);

void QueryExecuter();
void Optimization();

// string Base64Decoder(string value);
// string UnicodeDecoder(string Operator);
void initmap();

