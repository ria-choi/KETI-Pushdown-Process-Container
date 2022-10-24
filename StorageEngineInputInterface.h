#pragma once
#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <filesystem>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <thread>
#include <atomic>
#include <queue>

// Include boost thread
#include <boost/thread.hpp>
#include <boost/asio.hpp>

#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"

// Include rapidjson
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

//#include "TableManager.h"
#include "CSDScheduler.h"
#include "keti_type.h"
#include "buffer_manager.h"
#include "SnippetManager.h"
#include "mergequerykmc.h"
// #include "SnippetManager.h"
#include "CSDManager.h"
// #include "snippet_sample.pb.h"
// #include "snippet_sample.grpc.pb.h"
#include "WalManager.h"

using grpc::ServerReaderWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using snippetsample::SnippetSample;
using snippetsample::Snippet;
using snippetsample::Result;
using snippetsample::Request;

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;

TableManager tblManager;
CSDManager csdmanager;
Scheduler csdscheduler(csdmanager);
BufferManager bufma(csdscheduler);
// SnippetManager snippetmanager(tblManager,csdscheduler,csdmanager);
SnippetManager snippetmanager;


void accept_connection(int server_fd);
//int my_LBA2PBA(char* req,char* res);
int my_LBA2PBA(std::string &req_json,std::string &res_json);

void AppendProjection(SnippetStruct &snippetdata,Value &Projectiondata);
void AppendProjection(SnippetStruct &snippetdata, snippetsample::Snippet &snippet);
void AppendDependencyProjection(SnippetStruct &snippetdata,Value &Projectiondata);
void AppendDependencyProjection(SnippetStruct &snippetdata, snippetsample::Snippet_Dependency &dependency);
void AppendTableFilter(SnippetStruct &snippetdata,Value &filterdata);
void AppendTableFilter(SnippetStruct &snippetdata, snippetsample::Snippet &snippet);
void AppendDependencyFilter(SnippetStruct &snippetdata,Value &filterdata);
void AppendDependencyFilter(SnippetStruct &snippetdata, snippetsample::Snippet_Dependency &dependency);

void testrun(string dirname);

// void testsetdata();

// void testjoin();

// void testaggregation();
