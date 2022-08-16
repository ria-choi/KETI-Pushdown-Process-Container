#pragma once
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
#include <queue>

#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <grpcpp/grpcpp.h>

#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using snippetsample::SnippetSample;
using snippetsample::Snippet;
using snippetsample::Result;
using snippetsample::Request;
using google::protobuf::Empty;


#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

//#include "TableManager.h"
// #include "CSDScheduler.h"
#include "keti_type.h"
#include "buffer_manager.h"
#include "SnippetManager.h"
#include "mergequerykmc.h"
#include "SnippetManager.h"
#include "CSDManager.h"
// #include "snippet_sample.pb.h"
// #include "snippet_sample.grpc.pb.h"

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;



TableManager tblManager;
CSDManager csdmanager;
Scheduler csdscheduler;
BufferManager bufma(csdscheduler,tblManager);
SnippetManager snippetmanager;


// sumtest sumt;

// atomic<int> WorkID;
// unordered_map<string,int> fullquerymap;
// int totalrow = 0;
// int key;

void accept_connection(int server_fd);
//int my_LBA2PBA(char* req,char* res);
int my_LBA2PBA(std::string &req_json,std::string &res_json);

class SnippetSampleServiceImpl final : public SnippetSample::Service {
  Status SetSnippet(ServerContext* context,
                   ServerReaderWriter<Empty, Snippet>* stream) override;
  //                   {
  //   Snippet snippet;
  //   queue<SnippetStruct> snippetqueue;
  //   while (stream->Read(&snippet)) {
  //     Empty empty;

  //     std::cout << "SetSnippet" << std::endl;
  //     std::cout << "queryid :" << snippet.queryid() << std::endl;
  //     std::cout << "workid :" << snippet.workid() << std::endl;
  //     std::cout << "snippet :" << snippet.snippet() << std::endl << std::endl;

  //     query_result += "w_id :";
  //     query_result += std::to_string(snippet.workid());    
  //     query_result += "snippet str :";
  //     query_result += snippet.snippet();
  //     query_result += "\n";
  //     SnippetStruct snippetdata;
  //     snippetdata.query_id = snippet.queryid();
  //     snippetdata.work_id = snippet.workid();
  //     snippetqueue.push(snippetdata);
  //     stream->Write(empty);
  //   }
  //   //여기 매니저로 보내는거
  //   snippetmanager.NewQuery(snippetqueue);
  //   return Status::OK;
  // }
  Status Run(ServerContext* context, const Request* request, Result* result) override; 
  // {
  //   std::cout << "Run" << std::endl;
  //   std::cout << "req queryid :" << request->queryid() << std::endl << std::endl;
  //   unordered_map<string,vector<any>> RetTable;
  //   RetTable = snippetmanager.ReturnResult(request->queryid());
  //   result->set_value(query_result);

  //   query_result = "";
  //   return Status::OK;
  // }
  private:
    std::unordered_map<int, std::vector<std::string>> map;
    std::string query_result = "";
};

void RunServer();
void AppendProjection(SnippetStruct &snippetdata,Value &Projectiondata);