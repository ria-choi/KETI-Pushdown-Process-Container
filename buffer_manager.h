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

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "TableManager.h"
#include "QueryExecutor.h"
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

struct SnippetData{
        int work_id;
        string sstfilename;
        Value block_info_list;
        vector<string> table_col;
        Value table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> sstfilelist;
        string tablename;
        vector<string> column_projection;
        vector<string> column_filtering;
        vector<string> Group_By;
        vector<string> Order_By;
        vector<string> Expr;
};

class Scheduler{

    public:
        Scheduler() {init_scheduler();}
        vector<int> blockvec;
        vector<tuple<string,string,string>> savedfilter;
        vector<int> passindex;
        SnippetData snippetdata;
        vector<int> threadblocknum;
    struct Snippet{
        int work_id;
        string sstfilename;
        Value& block_info_list;
        vector<string> table_col;
        Value& table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> sstfilelist;
        vector<string> column_filtering;
        vector<string> Group_By;
        vector<string> Order_By;
        vector<string> Expr;
        vector<string> column_projection;

        

        Snippet(int work_id_, string sstfilename_,
            Value& block_info_list_,
            vector<string> table_col_, Value& table_filter_, 
            vector<int> table_offset_, vector<int> table_offlen_,
            vector<int> table_datatype_, vector<string> column_filtering_,
            vector<string> Group_By_, vector<string> Order_By_, vector<string> Expr_,
            vector<string> column_projection_)
            : work_id(work_id_), sstfilename(sstfilename_),
            block_info_list(block_info_list_),
            table_col(table_col_),
            table_filter(table_filter_),
            table_offset(table_offset_),
            table_offlen(table_offlen_),
            table_datatype(table_datatype_),
            column_filtering(column_filtering_),
            Group_By(Group_By_), Order_By(Order_By_), Expr(Expr_),
            column_projection(column_projection_){};
        Snippet();
    };

        typedef enum work_type{
            SCAN = 4,
            SCAN_N_FILTER = 5,
            REQ_SCANED_BLOCK = 6,
            WORK_END = 9
        }KETI_WORK_TYPE;

        void init_scheduler();
        void sched(int workid, Value& blockinfo,vector<int> offset, vector<int> offlen, vector<int> datatype, vector<string> tablecol, Value& filter,string sstfilename, string tablename, string res);
        void sched(int indexdata);
        void csdworkdec(string csdname, int num);
        void Serialize(PrettyWriter<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName);
        void Serialize(Writer<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName, int blockidnum);
        string BestCSD(string sstname, int blockworkcount);
        void sendsnippet(string snippet, string ipaddr);
        // void addcsdip(Writer<StringBuffer>& writer, string s);
        void printcsdblock(){
          for(auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
            pair<std::string, int> k = *i;
            cout << k.first << " " << k.second << endl;
          }
        }
    private:
        unordered_map<string,string> csd_; //csd의 ip정보가 담긴 맵 <csdname, csdip>
        unordered_map<string, int> csdworkblock_; //csd의 block work 수 가 담긴 맵 <csdname, csdworknum>
        vector<string> csdname_;
        unordered_map<string,string> sstcsd_; //csd의 sst파일 보유 내용 <sstname, csdlist>
        vector<string> csdpair_;
        unordered_map<string,string> csdreaplicamap_;
        int blockcount_;
};

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
    int rows;
    int result_block_count;
    string csd_name;

    BlockResult(const char* json_, char* data_){

        Document document;
        document.Parse(json_); 

        query_id = document["queryID"].GetInt();
        work_id = document["workID"].GetInt();
        rows = document["rowNum"].GetInt();

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
    map<string,vector<any>> table_data;//결과의 컬럼 별 데이터
    int left_block_count;//*남은 블록 수
    bool is_done;//워크 완료 여부
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
            table_datatype.push_back(MySQL_STRING);
          }else{
            table_datatype.push_back((*ptr2));
          }
        }
    }
};

struct Query_Buffer{
  int query_id;
  int work_cnt;//저장된 워크 개수
  unordered_map<int,Work_Buffer*> work_buffer_list;//워크버퍼
  unordered_map<string,pair<int,int>> table_status;//테이블별 상태<table_name/alias, <work_id,is_done> >

  Query_Buffer(int qid)
  :query_id(qid){
    work_cnt = 0;
    work_buffer_list.clear();
    table_status.clear();
  }
};

struct TableData{
  map<string,vector<any>> table_data;//결과의 컬럼 별 데이터

  TableData(){
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
    BufferManager(Scheduler &scheduler, TableManager &tblManager){
      InitBufferManager(scheduler, tblManager);
    }
    int InitBufferManager(Scheduler &scheduler, TableManager &tblManager);
    int Join();
    void BlockBufferInput();
    void BufferRunning(Scheduler &scheduler, TableManager &tblManager);
    void MergeBlock(BlockResult result, Scheduler &scheduler, TableManager &tblManager);
    // int GetData(Block_Buffer &dest);
    int InitWork(int qid, int wid, string table_alias,
                 vector<string> table_column_, vector<int> table_datatype,
                 vector<int> table_offlen_, int total_blobk_cnt_);
    void InitQuery(int qid);
    int CheckTableStatus(int qid, string tname);
    TableInfo GetTableInfo(int qid, string tname);
    TableData GetTableData(int qid, string tname);
    int SaveTableData(int qid, string tname, TableData table_data_);

    unordered_map<int, struct Query_Buffer*> my_buffer_m(){
        return this->m_BufferManager;
    }

private:
    unordered_map<int, struct Query_Buffer*> m_BufferManager;
    WorkQueue<BlockResult> BlockResultQueue;
    thread BufferManager_Input_Thread;
    thread BufferManager_Thread;
};