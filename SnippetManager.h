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
#include "buffer_manager.h"
#include "mergequerykmc.h"

#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

using namespace rapidjson;
using namespace std;


int GetSnippetType(SnippetStruct snippet, BufferManager &buff);
void ReturnColumnType(SnippetStruct snippet, BufferManager &buff);
void NewQuery(queue<SnippetStruct> newqueue);
void NewQueryMain(SavedRet &snippetdata, BufferManager &buff);
void GetIndexNumber(string TableName, vector<int> &IndexNumber);
bool GetWALTable(SnippetStruct snippet);

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
        void SetResult(unordered_map<string,vector<any>> result);
    private:
        queue<SnippetStruct> QueryQueue;
        unordered_map<string,vector<any>> result_;
        mutex mutex_;
};


class SnippetManager{
    public:
        void InitSnippetManager(TableManager &table);
        void NewQuery(queue<SnippetStruct> newqueue, BufferManager &buff);
        unordered_map<string,vector<any>> ReturnResult(int queryid);
    private:
        unordered_map<int,SavedRet> SavedResult;
        TableManager &tableManager;
};

