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
#include <mutex>

#include <boost/thread.hpp>
#include <boost/asio.hpp>

#include "TableManager.h"
#include "mergequerykmc.h"

#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

using namespace rapidjson;
using namespace std;


int GetSnippetType(SnippetStruct snippet, TableManager tablemanager);
bool hasBufM(string tablename);
void ReturnColumnType(SnippetStruct snippet);
void NewQuery(queue<SnippetStruct> newqueue);
void NewQueryMain(SavedRet &snippetdata);

class SavedRet{
    // SavedRet(queue<SnippetStruct> queue_){
    //     QueryQueue = queue_;
    // }
    public:
        void NewQuery(queue<SnippetStruct> newqueue);
        unordered_map<string,vector<any>> ReturnResult();
        void lockmutex();
        void unlockmutex();
        int GetSnippetSize();
        SnippetStruct Dequeue();
    private:
        queue<SnippetStruct> QueryQueue;
        unordered_map<string,vector<any>> result;
        mutex mutex_;
};


class SnippetManager{
    public:
        void NewQuery(queue<SnippetStruct> newqueue);
        unordered_map<string,vector<any>> ReturnResult(int queryid);
    private:
        unordered_map<int,SavedRet> SavedResult;
};

