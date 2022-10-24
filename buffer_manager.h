#pragma once
#include <vector>
#include <unordered_map>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <fstream>
#include <string>
#include <algorithm>
#include <queue>
#include <tuple>
#include <sys/socket.h>
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <mutex>
#include <condition_variable>
#include <string.h>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <list>
#include <unordered_set>
#include <map>
#include <any>
#include <bitset>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "CSDScheduler.h"
#include "TableManager.h"
#include "CSDManager.h"
#include "keti_type.h"

using namespace std;
using namespace rapidjson;

#define NUM_OF_BLOCKS 15
#define BUFF_SIZE (NUM_OF_BLOCKS * 4096)
#define PORT_BUF 8888
#define NCONNECTION 8

template <typename T>
class WorkQueue;
struct Block_Buffer;
struct Work_Buffer;


template <typename T>
class WorkQueue{
  condition_variable work_available;
  mutex work_mutex;
  queue<T> work;

public:
  void push_work(T item){
    unique_lock<mutex> lock(work_mutex);

    bool was_empty = work.empty();
    work.push(item);

    lock.unlock();

    if (was_empty){
      work_available.notify_one();
    }    
  }

  T wait_and_pop(){
    unique_lock<mutex> lock(work_mutex);
    while (work.empty()){
      work_available.wait(lock);
    }

    T tmp = work.front();
	
    work.pop();
    return tmp;
  }

  bool is_empty(){
    return work.empty();
  }

  void qclear(){
    work = queue<T>();
  }

  int get_size(){
    return work.size();
  }
};

struct BlockResult{
    int query_id;
    int work_id;  
    char data[BUFF_SIZE];
    int length;
    vector<int> row_offset; 
    int row_count;
    string csd_name;
    int result_block_count;

    BlockResult(){}
    BlockResult(const char* json_, char* data_){
        Document document;
        document.Parse(json_); 

        query_id = document["queryID"].GetInt();
        work_id = document["workID"].GetInt();
        row_count = document["rowCount"].GetInt();

        Value &row_offset_ = document["rowOffset"];
        int row_offset_size = row_offset_.Size();
        for(int i = 0; i<row_offset_size; i++){
            row_offset.push_back(row_offset_[i].GetInt());
        }

        length = document["length"].GetInt();

        memcpy(data, data_, length);

        csd_name = document["csdName"].GetString();
        result_block_count = document["resultBlockCount"].GetInt();
    }
};

struct Work_Buffer {
    string table_alias;//*결과 테이블의 별칭
    vector<string> table_column;//*결과의 컬럼 이름
    vector<int> return_datatype;//*결과의 컬럼 데이터 타입(돌아올때 확인)
    vector<int> table_datatype;//저장되는 결과의 컬럼 데이터 타입(위에서 확인)
    vector<int> table_offlen;//*결과의 컬럼 길이
    unordered_map<string,vector<vectortype>> table_data;//결과의 컬럼 별 데이터
    int left_block_count;//*남은 블록 수
    bool is_done;//작업 완료 여부
    condition_variable cond;
    mutex mu;
    // int table_type;//테이블 생성 타입?
    
    Work_Buffer(string table_alias_, vector<string> table_column_, 
                vector<int> return_datatype_, vector<int> table_offlen_,
                int total_blobk_cnt_){
        table_alias = table_alias_;
        table_column.assign(table_column_.begin(),table_column_.end());
        return_datatype.assign(return_datatype_.begin(),return_datatype_.end());
        table_offlen.assign(table_offlen_.begin(),table_offlen_.end());
        left_block_count = total_blobk_cnt_;
        is_done = false;
        table_datatype.clear();
        vector<string>::iterator ptr1;
        for(ptr1 = table_column_.begin(); ptr1 != table_column_.end(); ptr1++){
          table_data.insert({(*ptr1),{}});
        }
        vector<int>::iterator ptr2;
        for(ptr2 = return_datatype.begin(); ptr2 != return_datatype.end(); ptr2++){
          if((*ptr2)==MySQL_BYTE){
            table_datatype.push_back(KETI_INT8);
          }else if((*ptr2)==MySQL_VARSTRING){
            table_datatype.push_back(KETI_STRING);
          }else{
            table_datatype.push_back((*ptr2));
          }
        }
    }
};

struct Query_Buffer{
  int query_id;//쿼리ID
  int work_cnt;//저장된 워크 개수
  unordered_map<int,Work_Buffer*> work_buffer_list;//워크버퍼
  unordered_map<string,pair<int,int>> table_status;//테이블별 상태<key:table_name||alias, value:<work_id,is_done> >

  Query_Buffer(int qid)
  :query_id(qid){
    work_cnt = 0;
    work_buffer_list.clear();
    table_status.clear();
  }
};

struct TableData{
  bool valid;//결과의 유효성
  unordered_map<string,vector<vectortype>> table_data;//결과의 컬럼 별 데이터

  TableData(){
    valid = true;
    table_data.clear();
  }
};

struct TableInfo{
  vector<string> table_column;//*결과의 컬럼 이름
  vector<int> table_datatype;//저장되는 결과의 컬럼 데이터 타입
  vector<int> table_offlen;//*결과의 컬럼 길이

  TableInfo(){
    table_column.clear();
    table_datatype.clear();
    table_offlen.clear();
  }
};

class BufferManager{	
public:
    // BufferManager();
    BufferManager(Scheduler &scheduler){
      InitBufferManager(scheduler);
    }
    int InitBufferManager(Scheduler &scheduler);
    int Join();
    void BlockBufferInput();
    void BufferRunning(Scheduler &scheduler);
    void MergeBlock(BlockResult result, Scheduler &scheduler);
    // int GetData(Block_Buffer &dest);
    int InitWork(int qid, int wid, string table_alias,
                 vector<string> table_column_, vector<int> table_datatype,
                 vector<int> table_offlen_, int total_blobk_cnt_);
    void InitQuery(int qid);
    int CheckTableStatus(int qid, string tname);
    TableInfo GetTableInfo(int qid, string tname);
    TableData GetTableData(int qid, string tname);
    int SaveTableData(int qid, string tname, unordered_map<string,vector<vectortype>> table_data_);
    int DeleteTableData(int qid, string tname);
    int EndQuery(int qid);

    unordered_map<int, struct Query_Buffer*> my_buffer_m(){
        return this->m_BufferManager;
    }

private:
    unordered_map<int, struct Query_Buffer*> m_BufferManager;
    WorkQueue<BlockResult> BlockResultQueue;
    thread BufferManager_Input_Thread;
    thread BufferManager_Thread;
};

/*
// const std::string currentDateTime() {
//     time_t     now = time(0);
//     struct tm  tstruct;
//     char       buf[80];
//     tstruct = *localtime(&now);
//     strftime(buf, sizeof(buf), "%Y-%m-%d %X", &tstruct);

//     return buf;
// }

// #define log(fmt, ...) \
//     printf("[%s: function:%s > line:%d] ", fmt ,"\t\t\t (%s)\n", \
//     __FILE__, __LINE__, __func__, currentDateTime());
*/

