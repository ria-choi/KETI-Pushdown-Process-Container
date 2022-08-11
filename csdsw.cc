#include <thread>
#include <iostream>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <mutex>
#include <queue>
#include <list>
#include <algorithm>
#include <condition_variable>
#include <unordered_map>
#include <sys/socket.h>
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <stack>
#include <sys/stat.h>
#include <bitset>
#include <limits.h>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"

#include "rocksdb/sst_file_reader.h"
#include "rocksdb/slice.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/table_properties.h"

using namespace std;
using namespace rapidjson;
using namespace ROCKSDB_NAMESPACE;

#define NUM_OF_BLOCKS 15
#define BUFF_SIZE (NUM_OF_BLOCKS * 4096)
#define INPUT_IF_PORT 8080
#define BUFF_M_PORT 8888
#define PACKET_SIZE 36

WorkQueue<Snippet> ScanQueue;
WorkQueue<Result> FilterQueue;
WorkQueue<Result> MergeQueue;
WorkQueue<MergeResult> ReturnQueue;

template <typename T>
class WorkQueue;
struct ScanResult;
struct FilterResult;
struct Snippet;
struct Result;
struct MergeResult;

int main() {

    TableManager CSDTableManager = TableManager();
    CSDTableManager.InitCSDTableManager();

    thread InputInterface = thread(&Input::InputSnippet, Input());
    thread ScanLayer = thread(&Scan::Scanning, Scan(CSDTableManager));
    thread FilterLayer1 = thread(&Filter::Filtering, Filter());
    thread FilterLayer2 = thread(&Filter::Filtering, Filter());
    thread MergeLayer = thread(&MergeManager::Merging, MergeManager());
    thread ReturnInterface = thread(&Return::ReturnResult, Return());

    InputInterface.join();
    ScanLayer.join();
    FilterLayer1.join();
    FilterLayer2.join();
    MergeLayer.join();
    ReturnInterface.join();
    
    return 0;
}

typedef struct Data{
  int type;
  vector<string> str_col;
  vector<int> int_col;
  vector<float> flt_col;
}Data;

enum Scan_Type{
  Full_Scan_Filter,
  Full_Scan,
  Index_Scan_Filter,
  Index_Scan
};

struct PrimaryKey{
  string key_name;
  int key_type;
  int key_length;
};

class Input{
    public:
        Input(){}
        void InputSnippet();
        void EnQueueScan(Snippet snippet_);
};

struct Snippet{
    int work_id;
    int query_id;
    string csd_name;
    string table_name;
    list<BlockInfo> block_info_list;
    vector<string> table_col;
    string table_filter;
    vector<int> table_offset;
    vector<int> table_offlen;
    vector<int> table_datatype;
    int total_block_count;
    unordered_map<string, int> colindexmap;
    list<PrimaryKey> primary_key_list;
    uint64_t kNumInternalBytes;
    int primary_length;
    char index_num[4];
    string index_pk;
    int scan_type;
    vector<string> column_filter;
    vector<vector<string>> column_projection;
    vector<string> groupby_col;
    vector<pair<int,string>> orderby_col;

    Snippet(const char* json_){
        bool index_scan = false;
        bool only_scan = true;

        total_block_count = 0;
        Document document;
        document.Parse(json_);
        
        query_id = document["queryID"].GetInt();
        work_id = document["workID"].GetInt();
        csd_name = document["CSDName"].GetString();
        table_name = document["tableName"].GetString();

        Value &blockInfo = document["blockInfo"];
        int block_id_ = blockInfo["blockID"].GetInt(); 
        int block_list_size = blockInfo["blockList"].Size();

        int index_num_ = document["indexNum"].GetInt();
        union{
          int value;
          char byte[4];
        }inum;
        inum.value = index_num_;
        memcpy(index_num,inum.byte,4);

        for(int i = 0; i < document["columnFiltering"].Size(); i++){
          column_filter.push_back(document["columnFiltering"][i].GetString());
        }

        for(int i = 0; i < document["columnProjection"].Size(); i++){
          for(int j = 0; j < document["columnProjection"][i].Size(); j++){
            column_projection[i].push_back(document["columnProjection"][i][j].GetString());
          }
        }

        if(document.HasMember("groupBy")){
          for(int i = 0; i < document["groupBy"].Size(); i++){
            groupby_col.push_back(document["groupBy"][i].GetString());
          }
        }else{
          groupby_col.clear();
        }

        if(document.HasMember("orderBy")){
          for(int i = 0; i < document["orderBy"].Size(); i++){
            string op1 = document["orderBy"][i][0].GetString();
            string op2 = document["orderBy"][i][1].GetString();
            orderby_col.push_back(pair<int,string>(stoi(op1),op2));
          }
        }else{
          orderby_col.clear();
        }

        int primary_count = document["pk_len"].GetInt();

        if(primary_count == 0){
          kNumInternalBytes = 8;
        }else{
          kNumInternalBytes = 0;
        }

        uint64_t block_offset_, block_length_;
        
        for(int i = 0; i<block_list_size; i++){
            block_offset_ = blockInfo["BlockList"][i]["Offset"].GetInt64();
            for(int j = 0; j < blockInfo["BlockList"][i]["Length"].Size(); j++){
                block_length_ = blockInfo["BlockList"][i]["Length"][j].GetInt64();
                BlockInfo newBlock(block_id_, block_offset_, block_length_);
                block_info_list.push_back(newBlock);
                block_offset_ = block_offset_ + block_length_;
                block_id_++;
                total_block_count++;
            }
        }   

        primary_key_list.clear();
        primary_length = 0;

        Value &table_col_ = document["table_col"];
        for(int j=0; j<table_col_.Size(); j++){
            string col = table_col_[j].GetString();
            int startoff = document["table_offset"][j].GetInt();
            int offlen = document["table_offlen"][j].GetInt();
            int datatype = document["table_datatype"][j].GetInt();
            table_col.push_back(col);
            table_offset.push_back(startoff);
            table_offlen.push_back(offlen);
            table_datatype.push_back(datatype);
            colindexmap.insert({col,j});
            
            if(j<primary_count){
              string key_name_ = document["table_col"][j].GetString();
              int key_type_ = document["table_datatype"][j].GetInt();
              int key_length_ = document["table_offlen"][j].GetInt();
              primary_key_list.push_back(PrimaryKey{key_name_,key_type_,key_length_});
              primary_length += key_length_;
            }
        }

        if(document.HasMember("table_filter")){
          Value &table_filter_ = document["table_filter"];
          Document small_document;
          small_document.SetObject();
          rapidjson::Document::AllocatorType& allocator = small_document.GetAllocator();
          small_document.AddMember("table_filter",table_filter_,allocator);
          StringBuffer strbuf;
          rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
	        small_document.Accept(writer);
          table_filter = strbuf.GetString();
          only_scan = false;
        }else{
          cout << "+++ only scan!! +++" << endl;
        }

        if(document.HasMember("index_pk")){
          cout << "+++ index scan!! +++" << endl;
          Value &index_pk_ = document["index_pk"];
          Document small_document;
          small_document.SetObject();
          rapidjson::Document::AllocatorType& allocator = small_document.GetAllocator();
          small_document.AddMember("index_pk",index_pk_,allocator);
          StringBuffer strbuf;
          rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
	        small_document.Accept(writer);
          index_pk = strbuf.GetString();
          index_scan = true;
        }else{
          cout << "no index pk" << endl;
        }

        if(!index_scan && !only_scan){
          scan_type = Full_Scan_Filter;
        }else if(!index_scan && only_scan){
          scan_type = Full_Scan;
        }else if(index_scan && !only_scan){
          scan_type = Index_Scan_Filter;
        }else{
          scan_type = Index_Scan;
        }
       
    }
};

template <typename T>
class WorkQueue
{
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

  int length(){
	  int ret;
	  unique_lock<mutex> lock(work_mutex);

    ret = work.size();

    lock.unlock();
	  return ret;
  }
};

class Scan{
    public:
        Scan(TableManager table_m){
            CSDTableManager_ = table_m;
        }
        

        void Scanning();
        void BlockScan(SstBlockReader* sstBlockReader_, BlockInfo* blockInfo, 
                        Snippet *snippet_, Result *scan_result);
        void EnQueueData(Result scan_result, Snippet snippet_);
        void Column_Filtering(Result scan_result, Snippet snippet_);
        void GetColumnOff(char* data, Snippet snippet_, int colindex, int &varcharflag);

    private:
        TableManager CSDTableManager_;
        unordered_map<string,int> newoffmap;
        unordered_map<string,int> newlenmap;
};


class Filter{
public:
    Filter(){}
    vector<string> column_filter;
    unordered_map<string, int> newstartptr;
    unordered_map<string, int> newlengthraw;
    unordered_map<string,string> joinmap;
    char data[BUFF_SIZE];
    struct RowFilterData{
        vector<int> startoff;
        vector<int> offlen;
        vector<int> datatype;
        vector<string> ColName;
        vector<int> varcharlist;
        unordered_map<string,int> ColIndexmap;
        int offsetcount;
        int rowoffset;
        char *rowbuf;
    };
    
    RowFilterData rowfilterdata;
    int BlockFilter(Result &scanResult);
    void Filtering();
    void SavedRow(char *row, int startoff, Result &filterresult, int nowlength);
    vector<string> split(string str, char Delimiter);
    bool LikeSubString(string lv, string rv);
    bool LikeSubString_v2(string lv, string rv);
    bool InOperator(string lv, Value& rv,unordered_map<string, int> typedata,char *rowbuf);
    bool InOperator(int lv, Value& rv,unordered_map<string, int> typedata, char *rowbuf);
    bool BetweenOperator(int lv, int rv1, int rv2);
    bool BetweenOperator(string lv, string rv1, string rv2);
    bool IsOperator(string lv, char* nonnullbit, int isnot);
    bool isvarc(vector<int> datatype, int ColNum, vector<int> varcharlist);
    void makedefaultmap(vector<string> ColName, vector<int> startoff, vector<int> offlen, vector<int> datatype, int ColNum, unordered_map<string, int> &startptr, unordered_map<string, int> &lengthRaw, unordered_map<string, int> &typedata);
    void makenewmap(int isvarchar, int ColNum, unordered_map<string, int> &newstartptr, unordered_map<string, int> &newlengthraw, vector<int> datatype, unordered_map<string, int> lengthRaw, vector<string> ColName, int &iter, vector<int> startoff, vector<int> offlen, char *rowbuf);
    void compareGE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareGE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareLE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareLE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareGT(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareGT(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareLT(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareLT(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareET(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareET(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareNE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    void compareNE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot);
    int typeLittle(unordered_map<string, int> typedata, string colname, char *rowbuf);
    void JoinOperator(string colname);
    string ItoDec(int inum);
    string typeBig(string colname, char *rowbuf);
    string typeDecimal(string colname, char *rowbuf);
    void sendfilterresult(Result &filterresult);
    void GetColumnoff(string ColName);

    int row_offset;    
};

enum opertype
{
    GE,   
    LE,     
    GT,     
    LT,    
    ET,      
    NE,     
    LIKE,  
    BETWEEN, 
    IN,      
    IS,      
    ISNOT,   
    NOT,     
    AND,     
    OR,      
};

class MergeManager{
public:
    MergeManager(){}
    void Merging();
    void MergeBlock(Result &result);
    void SendDataToBufferManager(MergeResult &mergedBlock);

private:
    unordered_map<int, MergeResult> m_MergeManager;
};

struct FilterInfo{
    string table_filter;
    vector<string> table_col;
    vector<int> table_offset;
    vector<int> table_offlen;
    vector<int> table_datatype;
    unordered_map<string, int> colindexmap;
    vector<string> column_filter;
    vector<vector<string>> column_projection;
    vector<string> groupby_col;
    vector<pair<int,string>> orderby_col;
    
    FilterInfo(){}
    FilterInfo(
        string table_filter_, vector<string> table_col_,
        vector<int> table_offset_, vector<int> table_offlen_,
        vector<int> table_datatype_, unordered_map<string, int> colindexmap_,
        vector<string> column_filter_, vector<vector<string>> column_projection_,
        vector<string> groupby_col_, vector<pair<int,string>> orderby_col_)
        : table_filter(table_filter_),
          table_col(table_col_),
          table_offset(table_offset_),
          table_offlen(table_offlen_),
          table_datatype(table_datatype_),
          colindexmap(colindexmap_),
          column_filter(column_filter_),
          column_projection(column_projection_),
          groupby_col(groupby_col_),
          orderby_col(orderby_col_){}
};

struct Result{
    int work_id;
    int row_count;
    int length;
    char data[BUFF_SIZE];
    vector<int> row_offset;
    string csd_name;
    FilterInfo filter_info;
    int total_block_count;
    int result_block_count;
    list<PrimaryKey> primary_key_list;
    int last_valid_block_id;
    vector<vector<int>> row_column_offset;
    vector<string> column_name;

    Result(
        int work_id_, int total_block_count_, 
        string csd_name_, list<PrimaryKey> primary_key_list_)
        : work_id(work_id_), 
          total_block_count(total_block_count_),
          csd_name(csd_name_), 
          primary_key_list(primary_key_list_){
            result_block_count = 0;
            last_valid_block_id = INT_MAX;
          } 

    Result(
        int work_id_,
        int row_count_, int length_, char* data_,
        vector<int> row_offset_, string csd_name_,
        FilterInfo filter_info_, int total_block_count_,
        int result_block_count_, int last_valid_block_id_)
        : work_id(work_id_), 
          row_count(row_count_), length(length_),
          row_offset(row_offset_),
          csd_name(csd_name_),
          filter_info(filter_info_),
          total_block_count(total_block_count_),
          result_block_count(result_block_count_),
          last_valid_block_id(last_valid_block_id_){
            memcpy(data, data_, length);
          }      

    Result(
        int work_id_,
        int row_count_, int length_, string csd_name_, 
        FilterInfo filter_info_, int total_block_count_,
        int result_block_count_, int last_valid_block_id_)
        : work_id(work_id_), 
          row_count(row_count_), length(length_),
          csd_name(csd_name_), 
          filter_info(filter_info_),
          total_block_count(total_block_count_),
          result_block_count(result_block_count_),
          last_valid_block_id(last_valid_block_id_){} 
 
  void InitResult(){
    row_count = 0;
    length = 0;
    memset(&data, 0, sizeof(BUFF_SIZE));
    row_offset.clear();
    result_block_count = 0;
  }

};

class Return{
    public:
        Return(){}
        void ReturnResult();
        void SendDataToBufferManager(MergeResult &mergeResult);
};

struct message{
        long msg_type;
        char msg[2000];
};

struct MergeResult{
    int work_id;
    string csd_name;
    vector<int> row_offset; 
    int rows; 
    char data[BUFF_SIZE];
    int length;
    int total_block_count;
    int result_block_count;
    int current_block_count;
    int last_valid_block_id;
    map<string,vector<Data>> groupby_map;

    MergeResult(int workid_, string csdname_, int total_block_count_){
        work_id = workid_;
        csd_name = csdname_;
        row_offset.clear();
        rows = 0;
        length = 0;
        memset(&data, 0, sizeof(BUFF_SIZE));
        total_block_count = total_block_count_;
        result_block_count = 0;
        current_block_count = 0;
        last_valid_block_id = INT_MAX;
    }

    void InitMergeResult(){
        row_offset.clear();
        memset(&data, 0, sizeof(BUFF_SIZE));
        rows = 0;
        length = 0;
        result_block_count = 0;
    }
};

void Input::InputSnippet(){
	cout << "<-----------  Input Layer Running...  ----------->\n";

	int server_fd;
	int client_fd;
	int opt = 1;
	struct sockaddr_in serv_addr;
	struct sockaddr_in client_addr;
	socklen_t addrlen = sizeof(struct sockaddr_in);

	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
	
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(INPUT_IF_PORT);

	if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
		perror("bind");
		exit(EXIT_FAILURE);
	} 

	if (listen(server_fd, 3) < 0){
		perror("listen");
		exit(EXIT_FAILURE);
	} 

	while(1){
		if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&addrlen)) < 0){
			perror("accept");
        	exit(EXIT_FAILURE);
		}

		std::string json;
		char buffer[BUFF_SIZE] = {0};
		
		size_t length;
		read( client_fd , &length, sizeof(length));

		int numread;
		while(1) {
			if ((numread = read( client_fd , buffer, BUFF_SIZE - 1)) == -1) {
				cout << "read error" << endl;
				perror("read");
				exit(1);
			}
			length -= numread;
		    buffer[numread] = '\0';
			json += buffer;

		    if (length == 0)
				break;
		}

		Snippet parsedSnippet(json.c_str());
		
		EnQueueScan(parsedSnippet);

		close(client_fd);
	}

	close(server_fd);
	
}

void Input::EnQueueScan(Snippet parsedSnippet_){
	ScanQueue.push_work(parsedSnippet_);
}

uint64_t kNumInternalBytes_;
const int indexnum_size = 4;
bool index_valid;
bool first_row;
int ipk;
char sep = 0x03;
char gap = 0x20;
char fin = 0x02;

int getPrimaryKeyData(const char* ikey_data, char* dest, list<PrimaryKey> pk_list);

inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= kNumInternalBytes_);
  return Slice(internal_key.data(), internal_key.size() - kNumInternalBytes_);
}

class InternalKey {
 private:
  string rep_;

 public:
  InternalKey() {} 
  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  Slice user_key() const { return ExtractUserKey(rep_); }
};

void Scan::Scanning(){
    while (1){
        Snippet snippet = ScanQueue.wait_and_pop();

        TableRep table_rep = CSDTableManager_.GetTableRep(snippet.table_name);
        kNumInternalBytes_ = snippet.kNumInternalBytes;
		                
        Options options;
        SstBlockReader sstBlockReader(
            options, table_rep.blocks_maybe_compressed, table_rep.blocks_definitely_zstd_compressed, 
            table_rep.immortal_table, table_rep.read_amp_bytes_per_bit, table_rep.dev_name);

        list<BlockInfo>::iterator iter;
        Result scanResult(snippet.work_id, snippet.total_block_count, snippet.csd_name, snippet.primary_key_list);
        
        int current_block_count = 0;
        index_valid = true;
        ipk = 0;

        for(iter = snippet.block_info_list.begin(); iter != snippet.block_info_list.end(); iter++){//블록

            current_block_count++;
            scanResult.result_block_count++;

            BlockInfo blockInfo = *iter;
            first_row = true;
            BlockScan(&sstBlockReader, &blockInfo, &snippet, &scanResult);

            if(!index_valid){
              if(first_row){
                  scanResult.last_valid_block_id = blockInfo.block_id - 1;
                  cout << "<invalid> first row / " << scanResult.last_valid_block_id << "/" << scanResult.result_block_count << endl;
              }else{
                  scanResult.last_valid_block_id = blockInfo.block_id;
                  cout << "<invalid> not first row / " << scanResult.last_valid_block_id << "/" << scanResult.result_block_count << endl;
              }
              scanResult.result_block_count += snippet.total_block_count - current_block_count;
              current_block_count = snippet.total_block_count;
            }else{
            }

            if(current_block_count == snippet.total_block_count){
              EnQueueData(scanResult, snippet);
              scanResult.InitResult();
              break;
            }else if(current_block_count % NUM_OF_BLOCKS == 0){
              EnQueueData(scanResult, snippet);
              scanResult.InitResult();
            }
        }
    }
}

void Scan::BlockScan(SstBlockReader* sstBlockReader_, BlockInfo* blockInfo, Snippet *snippet_, Result *scan_result){

  Status s  = sstBlockReader_->Open(blockInfo);
  if(!s.ok()){
      cout << "open error" << endl;
  }

  const char* ikey_data;
  const char* row_data;
  size_t row_size;

  Iterator* datablock_iter = sstBlockReader_->NewIterator(ReadOptions());

  if(snippet_->scan_type == Full_Scan_Filter || snippet_->scan_type == Full_Scan){

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid(); datablock_iter->Next()) {
      scan_result->row_offset.push_back(scan_result->length);

      Status s = datablock_iter->status();
      if (!s.ok()) {
        cout << "Error reading the block - Skipped \n";
        break;
      }               

      const Slice& key = datablock_iter->key();
      const Slice& value = datablock_iter->value();

      InternalKey ikey;
      ikey.DecodeFrom(key);

      ikey_data = ikey.user_key().data();
      row_data = value.data();
      row_size = value.size();

      char index_num[indexnum_size];
      memcpy(index_num,ikey_data,indexnum_size);
      if(memcmp(snippet_->index_num, index_num, indexnum_size) != 0){
        cout << "different index number: ";
        for(int i=0; i<indexnum_size; i++){
          printf("(%02X %02X)",(u_char)snippet_->index_num[i],(u_char)index_num[i]);
        }
        cout << endl;
        index_valid = false;
        return;
      }

      first_row = false;
      if(snippet_->primary_length != 0){ 
        char total_row_data[snippet_->primary_length+row_size];
        int pk_length;

        pk_length = getPrimaryKeyData(ikey_data, total_row_data, snippet_->primary_key_list);//key
       
        memcpy(total_row_data + pk_length, row_data, row_size);
        memcpy(scan_result->data + scan_result->length, total_row_data, pk_length + row_size);//buff+key+value
        
        scan_result->length += row_size + pk_length;
        scan_result->row_count++;
      }else{
          memcpy(scan_result->data+scan_result->length, row_data, row_size);
          scan_result->length += row_size;
          scan_result->row_count++;
      }
    } 

  }else{
    string pk_str = snippet_->index_pk;
    Document document;
    document.Parse(pk_str.c_str());
    Value &index_pk = document["index_pk"];

    vector<char> target_pk;
    target_pk.assign(snippet_->index_num,snippet_->index_num+4);

    bool pk_valid = true;

    while(pk_valid){
      for(int i=0; i<index_pk.Size(); i++){
        int key_type = snippet_->table_datatype[i];

        if(key_type == 3 || key_type == 14){
          int key_length = snippet_->table_offlen[i];
          union{
            int value;
            char byte[4];
          }pk;
          pk.value = index_pk[i][ipk].GetInt();
          for(int j=0; j<key_length; j++){
            target_pk.push_back(pk.byte[j]);
          }

        }else if(key_type == 8 || key_type == 246){
          int key_length = snippet_->table_offlen[i];
          union{
            int64_t value;
            char byte[8];
          }pk;
          pk.value = index_pk[i][ipk].GetInt64();
          for(int j=0; j<key_length; j++){
            target_pk.push_back(pk.byte[j]);
          }

        }else if(key_type == 254 || key_type == 15){
          string pk = index_pk[i][ipk].GetString();
          int key_length = pk.length();
          for(int j=0; j<key_length; j++){
            target_pk.push_back(pk[j]);
          }
        }
      }

      char *p = &*target_pk.begin();
      Slice target_slice(p,target_pk.size());

      datablock_iter->Seek(target_slice);

      if(datablock_iter->Valid()){
        const Slice& key = datablock_iter->key();
        const Slice& value = datablock_iter->value();

        InternalKey ikey;
        ikey.DecodeFrom(key);
        ikey_data = ikey.user_key().data();

        std::cout << "[Row(HEX)] KEY: " << ikey.user_key().ToString(true) << " | VALUE: " << value.ToString(true) << endl;

        if(target_slice.compare(ikey.user_key()) == 0){
          row_data = value.data();
          row_size = value.size();

          char total_row_data[snippet_->primary_length+row_size];
          int pk_length;

          pk_length = getPrimaryKeyData(ikey_data, total_row_data, snippet_->primary_key_list);//key
        
          memcpy(total_row_data + pk_length, row_data, row_size);
          memcpy(scan_result->data + scan_result->length, total_row_data, pk_length + row_size);//buff+key+value
          
          scan_result->length += row_size + pk_length;
          scan_result->row_count++;
          
        }else{
          cout << "primary key error [no primary key!]" << endl;      

          cout << "target: ";
          for(int i=0; i<target_slice.size(); i++){
            printf("%02X ",(u_char)target_slice[i]);
          }
          cout << endl;

          cout << "ikey: ";
          for(int i=0; i<ikey.user_key().size(); i++){
            printf("%02X ",(u_char)ikey.user_key()[i]);
          }
          cout << endl;

          //check row index number
          char index_num[indexnum_size];
          memcpy(index_num,ikey_data,indexnum_size);
          if(memcmp(snippet_->index_num, index_num, indexnum_size) != 0){
            cout << "different index number: ";
            for(int i=0; i<indexnum_size; i++){
              printf("(%02X %02X)",(u_char)snippet_->index_num[i],(u_char)index_num[i]);
            }
            cout << endl;
            index_valid = false;
            pk_valid = false;
          }
        }
        ipk++;

      }else{
        pk_valid = false;
      }

    }
    
  }
  
}

int getPrimaryKeyData(const char* ikey_data, char* dest, list<PrimaryKey> pk_list){
  int offset = 4;
  int pk_length = 0;

  std::list<PrimaryKey>::iterator piter;
  for(piter = pk_list.begin(); piter != pk_list.end(); piter++){
      int key_length = (*piter).key_length;
      int key_type = (*piter).key_type;

      if(key_type == 3 || key_type == 8){
        char pk[key_length];

        pk[0] = 0x00;
        for(int i = 0; i < key_length; i++){
          pk[i] = ikey_data[offset+key_length-i-1];
        }

        memcpy(dest+pk_length, pk, key_length);

        pk_length += key_length;
        offset += key_length;

      }else if(key_type == 14){
        char pk[key_length];

        for(int i = 0; i < key_length; i++){
          pk[i] = ikey_data[offset+key_length-i-1];
        }

        memcpy(dest+pk_length, pk, key_length);

        pk_length += key_length;
        offset += key_length;

      }else if(key_type == 254 || key_type == 246){

        memcpy(dest+pk_length, ikey_data+offset, key_length);

        pk_length += key_length;
        offset += key_length;

      }else if(key_type == 15){
        char pk[key_length];
        int var_key_length = 0;
        bool end = false;

        while(!end){
          if(ikey_data[offset] == sep || ikey_data[offset] == gap){
            offset++;
          }else if(ikey_data[offset] == fin){
            offset++;   
            end = true;  
          }else{
            pk[var_key_length] = ikey_data[offset];
            offset++;
            var_key_length++;
          }
        }

        if(var_key_length < 255){
          char len = (char)var_key_length;
          dest[pk_length] = len;
          pk_length += 1;
        }else{
          char len[2];
          int l1 = var_key_length / 256;
          int l2 = var_key_length % 256;
          len[0] = (char)l1;
          len[1] = (char)l2;
          memcpy(dest+pk_length,len,2);
          pk_length += 2;
        }

        memcpy(dest+pk_length, pk, var_key_length);
        pk_length += var_key_length;

      }else{
        cout << "[Scan] New Type!!!! - " << key_type << endl;
      }
  }

  return pk_length;
}

void Scan::EnQueueData(Result scan_result, Snippet snippet_){
    FilterInfo filterInfo(
      snippet_.table_filter, snippet_.table_col, snippet_.table_offset,
      snippet_.table_offlen, snippet_.table_datatype, snippet_.colindexmap, 
      snippet_.column_filter, snippet_.column_projection, 
      snippet_.groupby_col, snippet_.orderby_col
    );

    if(snippet_.scan_type == Full_Scan_Filter){
      if(scan_result.length != 0){//scan->filter
        Result scanResult(
          snippet_.work_id,  scan_result.row_count, scan_result.length, 
          scan_result.data, scan_result.row_offset, snippet_.csd_name, filterInfo, 
          snippet_.total_block_count, scan_result.result_block_count, scan_result.last_valid_block_id
        );
        FilterQueue.push_work(scanResult);        
      }else{
        Result scanResult(
          snippet_.work_id, scan_result.row_count, scan_result.length, 
          scan_result.data, scan_result.row_offset, snippet_.csd_name, filterInfo,
          snippet_.total_block_count, scan_result.result_block_count, scan_result.last_valid_block_id
        );
        MergeQueue.push_work(scanResult);
      }

    }else if(snippet_.scan_type == Full_Scan){
      Column_Filtering(scan_result,snippet_);
      Result scanResult(
        snippet_.work_id, scan_result.row_count, scan_result.length, 
        scan_result.data, scan_result.row_offset, snippet_.csd_name, filterInfo,
        snippet_.total_block_count, scan_result.result_block_count, scan_result.last_valid_block_id
      );
      MergeQueue.push_work(scanResult);

    }else if(snippet_.scan_type == Index_Scan_Filter){
      if(scan_result.length != 0){
        Result scanResult(
          snippet_.work_id, scan_result.row_count, scan_result.length, 
          scan_result.data, scan_result.row_offset, snippet_.csd_name, filterInfo, 
          snippet_.total_block_count, scan_result.result_block_count, scan_result.last_valid_block_id
        );
        FilterQueue.push_work(scanResult);
      }else{
        Result scanResult(
          snippet_.work_id, scan_result.row_count, scan_result.length, 
          scan_result.data, scan_result.row_offset, snippet_.csd_name, filterInfo,
          snippet_.total_block_count, scan_result.result_block_count, scan_result.last_valid_block_id
        );
        MergeQueue.push_work(scanResult);
      }
      
    }else{
      Column_Filtering(scan_result,snippet_);
      Result scanResult(
        snippet_.work_id,  scan_result.row_count, scan_result.length, 
        scan_result.data, scan_result.row_offset, snippet_.csd_name, filterInfo,
        snippet_.total_block_count, scan_result.result_block_count, scan_result.last_valid_block_id
      );
      MergeQueue.push_work(scanResult);
    }
}


void Scan::Column_Filtering(Result scan_result, Snippet snippet_){
  if(snippet_.column_filter.size() == 0 || scan_result.length == 0){
    return;
  }

  int varcharflag = 1;
  scan_result.column_name = snippet_.column_filter;
  int max = 0;
  vector<int> newoffset;
  vector<vector<int>> row_col_offset;
  char tmpdatabuf[BUFF_SIZE];
  int newlen = 0;

  for(int i = 0; i < snippet_.column_filter.size(); i++){
    int tmp = snippet_.colindexmap[snippet_.column_filter[i]];
    if(tmp > max){
      max = tmp;
    }
  }

  for(int i = 0; i < scan_result.row_count; i++){
    int rowlen;
    if(i == scan_result.row_count - 1){
      rowlen = scan_result.length - scan_result.row_offset[i];
    }else{
      rowlen = scan_result.row_offset[i+1] - scan_result.row_offset[i];
    }

    char tmpbuf[rowlen];
    memcpy(tmpbuf,scan_result.data + scan_result.row_offset[i], rowlen);
    GetColumnOff(tmpbuf, snippet_, max, varcharflag);
    newoffset.push_back(newlen);
    vector<int> tmpvector;

    for(int j = 0; j < snippet_.column_filter.size(); j++){
      memcpy(tmpdatabuf + newlen, tmpbuf + newoffmap[snippet_.table_col[i]], newlenmap[snippet_.table_col[i]]);
      tmpvector.push_back(newlen);
      newlen += newlenmap[snippet_.table_col[i]];
    }

    row_col_offset.push_back(tmpvector);
    free(tmpbuf);
  }

  memcpy(scan_result.data,tmpdatabuf,newlen);
  scan_result.length = newlen;
  scan_result.row_column_offset = row_col_offset;
  scan_result.row_offset = newoffset;
}


void Scan::GetColumnOff(char* data, Snippet snippet_, int colindex, int &varcharflag){
  if(varcharflag == 0){
    return;
  }
  newlenmap.clear();
  newoffmap.clear();
  varcharflag = 0;
  for(int i = 0; i < colindex; i++){
    if(snippet_.table_datatype[i] == 15){
      if(snippet_.table_offlen[i] < 256){
        if(i == 0){
          newoffmap.insert(make_pair(snippet_.table_col[i],snippet_.table_offset[i]));
        }else{
          newoffmap.insert(make_pair(snippet_.table_col[i],newoffmap[snippet_.table_col[i - 1]] + newlenmap[snippet_.table_col[i - 1]] + 2));
        }
        int8_t len = 0;
        int8_t *lenbuf;
        char membuf[1];
        memset(membuf, 0, 1);
        membuf[0] = data[newoffmap[snippet_.table_col[i]]];
        lenbuf = (int8_t *)(membuf);
        len = lenbuf[0];
        newlenmap.insert(make_pair(snippet_.table_col[i],len));
        free(membuf);
      }else{
        int16_t len = 0;
        int16_t *lenbuf;
        char membuf[2];
        memset(membuf, 0, 2);
        membuf[0] = data[newoffmap[snippet_.table_col[i]]];
        membuf[1] = data[newoffmap[snippet_.table_col[i]] + 1];
        lenbuf = (int16_t *)(membuf);
        len = lenbuf[0];
        newlenmap.insert(make_pair(snippet_.table_col[i],len));
        free(membuf);
      }
      varcharflag = 1;
    }else{
      if(varcharflag){
        newoffmap.insert(make_pair(snippet_.table_col[i],newoffmap[snippet_.table_col[i - 1]] + newlenmap[snippet_.table_col[i - 1]]));
        newlenmap.insert(make_pair(snippet_.table_col[i],snippet_.table_offlen[i]));
      }else{
        newoffmap.insert(make_pair(snippet_.table_col[i],snippet_.table_offset[i]));
        newlenmap.insert(make_pair(snippet_.table_col[i],snippet_.table_offlen[i]));
      }
    }
  }
}

int rownum = 0;
// int test = 0;

int saverowcount = 0;

void Filter::Filtering()
{
    // cout << "<-----------  Filter Layer Running...  ----------->\n";
    // key_t key = 12345;
    // int msqid;
    // message msg;
    // msg.msg_type = 1;
    // if ((msqid = msgget(key, IPC_CREAT | 0666)) == -1)
    // {
    //     printf("msgget failed\n");
    //     exit(0);
    // }
    while (1)
    {
        Result scanResult = FilterQueue.wait_and_pop();//ScanResult scanResult = FilterQueue.wait_and_pop();
        
        // string rowfilter = "Filtering Using Filter Queue : Work ID " + to_string(scanResult.work_id) + " Block ID " + to_string(scanResult.block_id) + " Row Num " + to_string(scanResult.rows) + " Filter Json " + scanResult.table_filter;
        // strcpy(msg.msg, rowfilter.c_str());
        // if (msgsnd(msqid, &msg, sizeof(msg.msg), 0) == -1)
        // {
        //     printf("msgsnd failed\n");
        //     exit(0);
        // }

        // cout << " <------------Filter Block------------>" << endl;
        //  cout << "Filtering Using Filter Queue : Work ID " << scanResult.work_id << " Block ID " << scanResult.block_id <<" Row Num "<< scanResult.rows << " Filter Json " << scanResult.table_filter << endl;
        BlockFilter(scanResult);
        // test++;
    }
}

int Filter::BlockFilter(Result &scanResult)
{
    char *rowbuf = scanResult.data;
    int filteroffset = 0;

    unordered_map<string, int> startptr;
    unordered_map<string, int> lengthRaw;
    unordered_map<string, int> typedata;
    column_filter = scanResult.filter_info.column_filter;

    Result filterresult(scanResult.work_id, 0, 0, scanResult.csd_name, scanResult.filter_info,
                        scanResult.total_block_count, scanResult.result_block_count, scanResult.last_valid_block_id);
    filterresult.column_name = scanResult.filter_info.column_filter;
    int ColNum = scanResult.filter_info.table_col.size(); 
    int RowNum = scanResult.row_count;            

    vector<int> datatype = scanResult.filter_info.table_datatype;
    vector<int> varcharlist;

    string str = scanResult.filter_info.table_filter;
    Document document;
    document.Parse(str.c_str());
    Value &filterarray = document["table_filter"];

    bool CV, TmpV;          
    bool Passed;            
    bool isSaved, canSaved; 
    bool isnot;             
    bool isvarchar = 0;    
    
    isvarchar = isvarc(datatype, ColNum, varcharlist);

    rowfilterdata.ColIndexmap = scanResult.filter_info.colindexmap;
    rowfilterdata.ColName = scanResult.filter_info.table_col;
    rowfilterdata.datatype = scanResult.filter_info.table_datatype;
    rowfilterdata.offlen = scanResult.filter_info.table_offlen;
    rowfilterdata.startoff = scanResult.filter_info.table_offset;
    rowfilterdata.rowbuf = scanResult.data;
    for(int i = 0; i < ColNum; i ++){
        typedata.insert(make_pair(rowfilterdata.ColName[i],rowfilterdata.datatype[i]));
    }
    int iter = 0;
    for (int i = 0; i < RowNum; i++)
    {
       
        rowfilterdata.offsetcount = 0;
        newstartptr.clear();
        newlengthraw.clear();
       
        iter = scanResult.row_offset[i];
        rowfilterdata.rowoffset = iter;
        TmpV = true;
        Passed = false;
        isSaved = false;
        canSaved = false;
        isnot = false;
        bool isfirst1 = true;
        for (int j = 0; j < filterarray.Size(); j++)
        {
            if (Passed)
            {
               
                switch (filterarray[j]["OPERATOR"].GetInt())
                {
                case OR:
                    isnot = false;
                    if (CV == true)
                    {
                        isSaved = true;
                    }
                    else
                    {
                        TmpV = true;
                        Passed = false;
                    }
                    break;
                default:
                    break;
                }
            }
            else
            {
                switch (filterarray[j]["OPERATOR"].GetInt())
                {
                case GE:
                    if (filterarray[j]["LV"].IsString())
                    {
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            if (filterarray[j]["RV"].IsString())
                            {
                                RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetInt();
                            }
                            compareGE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                                {
                                    RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            {
                            }

                            compareGE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 246)
                                {
                                    RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            { 
                                int tmpint;
                                tmpint = filterarray[j]["RV"].GetInt();
                                RV = ItoDec(tmpint);
                            }
                            compareGE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            string RV;
                            if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else if (typedata[filterarray[j]["RV"].GetString()] == 246)
                            {
                                RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            compareGE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                    }
                    else
                    { 
                        int LV = filterarray[j]["LV"].GetInt();
                        int RV;
                        RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        compareGE(LV, RV, CV, TmpV, canSaved, isnot);
                    }
                    break;
                case LE:
                    if (filterarray[j]["LV"].GetType() == 5)
                    {                                                                                                            // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetInt();
                            }
                            compareLE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetString();
                                RV = RV.substr(1);
                            }
                            compareLE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 246)
                                {
                                    RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            {
                                int tmpint;
                                tmpint = filterarray[j]["RV"].GetInt();
                                RV = ItoDec(tmpint);
                            }
                            compareLE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            string RV;
                            if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else if (typedata[filterarray[j]["RV"].GetString()] == 246)
                            {
                                RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            compareLE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                    }
                    else
                    { 
                        int LV = filterarray[j]["LV"].GetInt();
                        int RV;
                        RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        compareLE(LV, RV, CV, TmpV, canSaved, isnot);
                    }
                    break;
                case GT:
                    if (filterarray[j]["LV"].GetType() == 5)
                    {                                                                                                            // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetInt();
                            }
                            compareGT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetString();
                                RV = RV.substr(1);
                            }
                            compareGT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 246)
                                {
                                    RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            { 
                                int tmpint;
                                tmpint = filterarray[j]["RV"].GetInt();
                                RV = ItoDec(tmpint);
                            }
                            compareGT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            string RV;
                            if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else if (typedata[filterarray[j]["RV"].GetString()] == 246)
                            {
                                RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            compareGT(LV, RV, CV, TmpV, canSaved, isnot); 
                        }
                    }
                    else
                    {
                        int LV = filterarray[j]["LV"].GetInt();
                        int RV;
                        RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        compareGT(LV, RV, CV, TmpV, canSaved, isnot);
                    }
                    break;
                case LT:
                    if (filterarray[j]["LV"].IsString())
                    {                                                                                                             // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetInt();
                            }
                            compareLT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetString();
                                RV = RV.substr(1);
                            }
                            compareLT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 246)
                                {
                                    RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            {
                                int tmpint;
                                tmpint = filterarray[j]["RV"].GetInt();
                                RV = ItoDec(tmpint);
                            }
                            compareLT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            string RV;
                            if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else if (typedata[filterarray[j]["RV"].GetString()] == 246)
                            {
                                RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            compareLT(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                    }
                    else
                    {
                        int LV = filterarray[j]["LV"].GetInt();
                        int RV;
                        RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        compareLT(LV, RV, CV, TmpV, canSaved, isnot);
                    }
                    break;
                case ET:
                    if (filterarray[j]["LV"].GetType() == 5)
                    {                                                                                                            // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetInt();
                            }
                            compareET(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetString();
                                RV = RV.substr(1);
                            }
                            compareET(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 246)
                                {
                                    RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            { 
                                int tmpint;
                                tmpint = filterarray[j]["RV"].GetInt();
                                RV = ItoDec(tmpint);
                            }
                            compareET(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            string RV;
                            if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else if (typedata[filterarray[j]["RV"].GetString()] == 246)
                            {
                                RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            compareET(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                    }
                    else
                    {
                        int LV = filterarray[j]["LV"].GetInt();
                        int RV;
                        RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        compareET(LV, RV, CV, TmpV, canSaved, isnot);
                    }
                    break;
                case NE:
                    if (filterarray[j]["LV"].GetType() == 5)
                    {                                                                                                            // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetInt();
                            }
                            compareNE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else
                            {
                                RV = filterarray[j]["RV"].GetString();
                                RV = RV.substr(1);
                            }
                            compareNE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246)
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            if (filterarray[j]["RV"].GetType() == 5)
                            {
                                if (typedata[filterarray[j]["RV"].GetString()] == 246)
                                {
                                    RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                                }
                                else
                                {
                                    RV = filterarray[j]["RV"].GetString();
                                    RV = RV.substr(1);
                                }
                            }
                            else
                            {
                                int tmpint;
                                tmpint = filterarray[j]["RV"].GetInt();
                                RV = ItoDec(tmpint);
                            }
                            compareNE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            string RV;
                            if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15)
                            {
                                RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            else if (typedata[filterarray[j]["RV"].GetString()] == 246)
                            {
                                RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                            }
                            compareNE(LV, RV, CV, TmpV, canSaved, isnot);
                        }
                    }
                    else
                    { 
                        int LV = filterarray[j]["LV"].GetInt();
                        int RV;
                        RV = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        compareNE(LV, RV, CV, TmpV, canSaved, isnot);
                    }
                    break;
                case LIKE:
                {
                    string RV;
                    string LV;
                    if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                    {
                        int tmplv;
                        tmplv = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                        LV = to_string(tmplv);
                    }

                    else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                    {
                        LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                    }
                    else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                    {
                        LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                    }
                    else
                    {
                        LV = filterarray[j]["LV"].GetString();
                        LV = LV.substr(1);
                    }
                    if (typedata[filterarray[j]["RV"].GetString()] == 3 || typedata[filterarray[j]["RV"].GetString()] == 14) //리틀에디안
                    {
                        int tmprv;
                        tmprv = typeLittle(typedata, filterarray[j]["RV"].GetString(), rowbuf);
                        RV = to_string(tmprv);
                    }

                    else if (typedata[filterarray[j]["RV"].GetString()] == 254 || typedata[filterarray[j]["RV"].GetString()] == 15) //빅에디안
                    {
                        RV = typeBig(filterarray[j]["RV"].GetString(), rowbuf);
                    }
                    else if (typedata[filterarray[j]["RV"].GetString()] == 246) 
                    {
                        RV = typeDecimal(filterarray[j]["RV"].GetString(), rowbuf);
                    }
                    else
                    {
                        RV = filterarray[j]["RV"].GetString();
                        RV = RV.substr(1);
                    }
                    CV = LikeSubString_v2(LV, RV);
                    if (isnot)
                    {
                        if (CV)
                        {
                            CV = false;
                            canSaved = false;
                        }
                        else
                        {
                            CV = true;
                            canSaved = true;
                        }
                    }
                    break;
                }
                case BETWEEN:
                    if (filterarray[j]["LV"].GetType() == 5)
                    {                                                                                             // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            int RV;
                            int RV1;
                            for (int k = 0; k < filterarray[j]["EXTRA"].Size(); k++)
                            {
                                if (filterarray[j]["EXTRA"][k].GetType() == 5)
                                { 
                                    if (typedata[filterarray[j]["EXTRA"][k].GetString()] == 3 || typedata[filterarray[j]["EXTRA"][k].GetString()] == 14)
                                    {
                                        if (k == 0)
                                        {
                                            RV = typeLittle(typedata, filterarray[j]["EXTRA"][k].GetString(), rowbuf);
                                        }
                                        else
                                        {
                                            RV1 = typeLittle(typedata, filterarray[j]["EXTRA"][k].GetString(), rowbuf);
                                        }
                                    }
                                    else
                                    { 
                                        if (k == 0)
                                        {
                                            try
                                            {
                                                RV = stoi(filterarray[j]["EXTRA"][k].GetString());
                                            }
                                            catch (...)
                                            {
                                                CV = true; 
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            try
                                            {
                                                RV1 = stoi(filterarray[j]["EXTRA"][k].GetString());
                                            }
                                            catch (...)
                                            {
                                                CV = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                else if (filterarray[j]["EXTRA"][k].GetType() == 6) 
                                {                                                   
                                    if (filterarray[j]["EXTRA"][k].IsInt())
                                    {
                                        if (k == 0)
                                        {
                                            RV = filterarray[j]["EXTRA"][k].GetInt();
                                        }
                                        else
                                        {
                                            RV1 = filterarray[j]["EXTRA"][k].GetInt();
                                        }
                                    }
                                    else
                                    { 
                                        float RV = filterarray[j]["EXTRA"][k].GetFloat();
                                    }
                                }
                            }
                            CV = BetweenOperator(LV, RV, RV1);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {

                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            string RV1;
                            for (int k = 0; k < filterarray[j]["EXTRA"].Size(); k++)
                            {
                                if (filterarray[j]["EXTRA"][k].GetType() == 5)
                                { 
                                    if (typedata[filterarray[j]["EXTRA"][k].GetString()] == 3 || typedata[filterarray[j]["EXTRA"][k].GetString()] == 14)
                                    {
                                        if (k == 0)
                                        {
                                            RV = typeBig(filterarray[j]["EXTRA"][k].GetString(), rowbuf);
                                        }
                                        else
                                        {
                                            RV1 = typeBig(filterarray[j]["EXTRA"][k].GetString(), rowbuf);
                                        }
                                    }
                                    else
                                    {
                                        if (k == 0)
                                        {
                                            try
                                            {
                                                RV = filterarray[j]["EXTRA"][k].GetString();
                                                RV = RV.substr(1);
                                            }
                                            catch (...)
                                            {
                                                CV = true; 
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            try
                                            {
                                                RV1 = filterarray[j]["EXTRA"][k].GetString();
                                                RV1 = RV.substr(1);
                                            }
                                            catch (...)
                                            {
                                                CV = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                else if (filterarray[j]["EXTRA"][k].GetType() == 6) 
                                {                                                   
                                    int tmpint;
                                    if (filterarray[j]["EXTRA"][k].IsInt())
                                    {
                                        if (k == 0)
                                        {
                                            tmpint = filterarray[j]["EXTRA"][k].GetInt();
                                            RV = to_string(tmpint);
                                        }
                                        else
                                        {
                                            tmpint = filterarray[j]["EXTRA"][k].GetInt();
                                            RV1 = to_string(tmpint);
                                        }
                                    }
                                    else
                                    {
                                    }
                                }
                            }
                            CV = BetweenOperator(LV, RV, RV1);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            string RV;
                            string RV1;
                            for (int k = 0; k < filterarray[j]["EXTRA"].Size(); k++)
                            {
                                
                                if (filterarray[j]["EXTRA"][k].GetType() == 5)
                                { 
                                    if (typedata[filterarray[j]["EXTRA"][k].GetString()] == 3 || typedata[filterarray[j]["EXTRA"][k].GetString()] == 14)
                                    {
                                        if (k == 0)
                                        {
                                            RV = typeDecimal(filterarray[j]["EXTRA"][k].GetString(), rowbuf);
                                        }
                                        else
                                        {
                                            RV1 = typeDecimal(filterarray[j]["EXTRA"][k].GetString(), rowbuf);
                                        }
                                    }
                                    else
                                    {
                                        if (k == 0)
                                        {
                                            try
                                            {
                                                RV = filterarray[j]["EXTRA"][k].GetString();
                                                RV = RV.substr(1);
                                            }
                                            catch (...)
                                            {
                                                CV = true; 
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            try
                                            {
                                                RV1 = filterarray[j]["EXTRA"][k].GetString();
                                                RV1 = RV1.substr(1);
                                            }
                                            catch (...)
                                            {
                                                CV = true;
                                                break;
                                            }
                                        }
                                    }

                                else if (filterarray[j]["EXTRA"][k].GetType() == 6) 
                                {                                                   
                                    int tmpint;
                                    if (filterarray[j]["EXTRA"][k].IsInt())
                                    {
                                        if (k == 0)
                                        {
                                            tmpint = filterarray[j]["EXTRA"][k].GetInt();
                                            RV = to_string(tmpint);
                                        }
                                        else
                                        {
                                            tmpint = filterarray[j]["EXTRA"][k].GetInt();
                                            RV1 = to_string(tmpint);
                                        }
                                    }
                                    else
                                    {
                                    }
                                }
                            }
                            CV = BetweenOperator(LV, RV, RV1);
                        }
                        else
                        { 
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                        }
                    }
                    else
                    {
                        int LV = filterarray[j]["LV"].GetInt();
                    }
                    if (isnot)
                    {
                        if (CV)
                        {
                            CV = false;
                            canSaved = false;
                        }
                        else
                        {
                            CV = true;
                            canSaved = true;
                        }
                    }
                    break;
                case IN:
                    if (filterarray[j]["LV"].GetType() == 5)
                    {                                                                                                            // 6은 스트링 --> 스트링이다는 컬럼이름이거나 char이거나 decimal이다
                        if (typedata[filterarray[j]["LV"].GetString()] == 3 || typedata[filterarray[j]["LV"].GetString()] == 14 || typedata[filterarray[j]["LV"].GetString()] == 8) //리틀에디안
                        {
                            int LV = typeLittle(typedata, filterarray[j]["LV"].GetString(), rowbuf);
                            Value &Extra = filterarray[j]["EXTRA"];
                            CV = InOperator(LV, Extra, typedata, rowbuf);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 254 || typedata[filterarray[j]["LV"].GetString()] == 15) //빅에디안
                        {
                            string LV = typeBig(filterarray[j]["LV"].GetString(), rowbuf);
                            Value &Extra = filterarray[j]["EXTRA"];
                            CV = InOperator(LV, Extra, typedata, rowbuf);
                        }
                        else if (typedata[filterarray[j]["LV"].GetString()] == 246) 
                        {
                            string LV = typeDecimal(filterarray[j]["LV"].GetString(), rowbuf);
                            Value &Extra = filterarray[j]["EXTRA"];
                            CV = InOperator(LV, Extra, typedata, rowbuf);
                        }
                        else
                        {
                            string LV = filterarray[j]["LV"].GetString();
                            LV = LV.substr(1);
                            Value &Extra = filterarray[j]["EXTRA"];
                            CV = InOperator(LV, Extra, typedata, rowbuf);
                        }
                    }
                    else
                    { 
                        int LV = filterarray[j]["LV"].GetInt();
                        Value &Extra = filterarray[j]["EXTRA"];
                        CV = InOperator(LV, Extra, typedata, rowbuf);
                    }
                    break;
                case IS:
                    break;
                case ISNOT:
                    break;
                case NOT:
                    if (isnot)
                    {
                        isnot = false;
                      
                    }
                    else
                    {
                        isnot = true;
                       
                    }
                    break;
                case AND:
                    isnot = false;
                    if (CV == false)
                    { 
                        Passed = true;
                    }
                    else
                    {
                        TmpV = CV;
                    }
                    break;
                case OR:
                    isnot = false;
                    if (CV == true)
                    {
                        isSaved = true;
                    }
                    else
                    {
                        TmpV = true;
                        Passed = false;
                    }
                    break;
                default:
                    cout << "error this is no default" << endl;
                    break;
                }
            }
            if (isSaved == true)
            {
                char *ptr = rowbuf;
                if (i == scanResult.row_count - 1)
                {
                    char *tmpsave = new char[scanResult.length - scanResult.row_offset[i]];

                    memcpy(tmpsave, ptr + scanResult.row_offset[i], scanResult.length - scanResult.row_offset[i]);
                    SavedRow(tmpsave, filteroffset, filterresult, scanResult.length - scanResult.row_offset[i]);
                    filteroffset += scanResult.length - scanResult.row_offset[i];
                    free(tmpsave);
                }
                else
                {
                    char *tmpsave = new char[scanResult.row_offset[i + 1] - scanResult.row_offset[i]];
                    memcpy(tmpsave, ptr + scanResult.row_offset[i], scanResult.row_offset[i + 1] - scanResult.row_offset[i]);
                    SavedRow(tmpsave, filteroffset, filterresult, scanResult.row_offset[i + 1] - scanResult.row_offset[i]);
                    filteroffset += scanResult.row_offset[i + 1] - scanResult.row_offset[i];
                    free(tmpsave);
                }
                break;
            }
        }

        if (canSaved == true && isSaved == false && Passed != true && CV == true)
        { 
            char *ptr = rowbuf;
            if (i == scanResult.row_count - 1)
            {
                char *tmpsave = new char[scanResult.length - scanResult.row_offset[i]];
                memcpy(tmpsave, ptr + scanResult.row_offset[i], scanResult.length - scanResult.row_offset[i]);
                SavedRow(tmpsave, filteroffset, filterresult, scanResult.length - scanResult.row_offset[i]);
                filteroffset += scanResult.length - scanResult.row_offset[i];
                free(tmpsave);
            }
            else
            {
                char *tmpsave = new char[scanResult.row_offset[i + 1] - scanResult.row_offset[i]];
                memcpy(tmpsave, ptr + scanResult.row_offset[i], scanResult.row_offset[i + 1] - scanResult.row_offset[i]);
                SavedRow(tmpsave, filteroffset, filterresult, scanResult.row_offset[i + 1] - scanResult.row_offset[i]);
                filteroffset += scanResult.row_offset[i + 1] - scanResult.row_offset[i];
                free(tmpsave);
            }

        }
    }
    sendfilterresult(filterresult);
    return 0;
}

void Filter::sendfilterresult(Result &filterresult_)
{
    MergeQueue.push_work(filterresult_);
}

bool Filter::LikeSubString(string lv, string rv)
{
    int len = rv.length();
    int LvLen = lv.length();
    std::string val;
    if (rv[0] == '%' && rv[len - 1] == '%')
    {
        val = rv.substr(1, len - 2);
        for (int i = 0; i < LvLen - len + 1; i++)
        {
            if (lv.substr(i, val.length()) == val)
            {
                return true;
            }
        }
    }
    else if (rv[0] == '%')
    {
        val = rv.substr(1, len - 1);
        if (lv.substr(lv.length() - val.length() - 1, val.length()) == val)
        {
            return true;
        }
    }
    else if (rv[len - 1] == '%')
    {
        val = rv.substr(0, len - 1);
        if (lv.substr(0, val.length()) == val)
        {
            return true;
        }
    }
    else
    {
        if (rv == lv)
        {
            return true;
        }
    }
    return false;
}

bool Filter::LikeSubString_v2(string lv, string rv)
{ 
    int len = rv.length();
    int LvLen = lv.length();
    int i = 0, j = 0;
    int substringsize = 0;
    bool isfirst = false, islast = false; 
    if (rv[0] == '%')
    {
        isfirst = true;
    }
    if (rv[len - 1] == '%')
    {
        islast = true;
    }
    vector<string> val = split(rv, '%');
    if (isfirst)
    {
        i = 1;
    }
    for (i; i < val.size(); i++)
    {

        for (j; j < LvLen - val[val.size() - 1].length() + 1; j++)
        { 
            substringsize = val[i].length();
            if (!isfirst)
            {

                if (lv.substr(0, substringsize) != val[i])
                {
                    return false;
                }
            }
            if (!islast)
            {

                if (lv.substr(LvLen - val[val.size() - 1].length(), val[val.size() - 1].length()) != val[val.size() - 1])
                {
                    return false;
                }
            }
            if (lv.substr(j, val[i].length()) == val[i])
            {
                if (i == val.size() - 1)
                {
                    return true;
                }
                else
                {
                    j = j + val[i].length();
                    i++;
                    continue;
                }
            }
        }
        return false;
    }

    return false;
}

bool Filter::InOperator(string lv, Value &rv, unordered_map<string, int> typedata, char *rowbuf)
{
    for (int i = 0; i < rv.Size(); i++)
    {
        string RV = "";
        if (rv[i].IsString())
        {
            if (typedata[rv[i].GetString()] == 3 || typedata[rv[i].GetString()] == 14)
            {
                int tmp = typeLittle(typedata, rv[i].GetString(), rowbuf);
                RV = ItoDec(tmp);
            }

            else if (typedata[rv[i].GetString()] == 254 || typedata[rv[i].GetString()] == 15) 
            {
                RV = typeBig(rv[i].GetString(), rowbuf);
            }
            else if (typedata[rv[i].GetString()] == 246) 
            {
                RV = typeDecimal(rv[i].GetString(), rowbuf);
            }
            else
            {
                string tmps;
                tmps = rv[i].GetString();
                RV = tmps.substr(1);
            }
        }
        else 
        {
            RV = ItoDec(rv[i].GetInt());
        }
        if (lv == RV)
        {
            return true;
        }
    }
    return false;
}
bool Filter::InOperator(int lv, Value &rv, unordered_map<string, int> typedata, char *rowbuf)
{
    for (int i = 0; i < rv.Size(); i++)
    {
        if (rv[i].IsString())
        {
            if (typedata[rv[i].GetString()] == 3 || typedata[rv[i].GetString()] == 14) 
            {
                int RV = typeLittle(typedata, rv[i].GetString(), rowbuf);
                if (lv == RV)
                {
                    return true;
                }
            }

            else if (typedata[rv[i].GetString()] == 254 || typedata[rv[i].GetString()] == 15) 
            {
                string RV = typeBig(rv[i].GetString(), rowbuf);
                try
                {
                    if (lv == atoi(RV.c_str()))
                    {
                        return true;
                    }
                }
                catch (...)
                {
                    continue;
                }
            }
            else if (typedata[rv[i].GetString()] == 246)
            {
                string RV = typeDecimal(rv[i].GetString(), rowbuf);

                if (ItoDec(lv) == RV)
                {
                    return true;
                }
            }
            else
            {
                string tmps;
                int RV;
                tmps = rv[i].GetString();
                try
                {
                    RV = atoi(tmps.substr(1).c_str());
                }
                catch (...)
                {
                    continue;
                }
            }
        }
        else
        {
            int RV = rv[i].GetInt();
            if (lv == RV)
            {
                return true;
            }
        }
    }
    return false;
}

bool Filter::BetweenOperator(int lv, int rv1, int rv2)
{
    if (lv >= rv1 && lv <= rv2)
    {
        return true;
    }
    return false;
}

bool Filter::BetweenOperator(string lv, string rv1, string rv2)
{
    if (lv >= rv1 && lv <= rv2)
    {
        return true;
    }
    return false;
}

bool Filter::IsOperator(string lv, char* nonnullbit, int isnot)
{
    int colindex = rowfilterdata.ColIndexmap[lv];
    if (lv.empty())
    {
        if (isnot == 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    if (isnot == 0)
    {
        return false;
    }
    else
    {
        return true;
    }
}

void Filter::SavedRow(char *row, int startoff, Result &filterresult, int nowlength)
{
    rownum++;
    filterresult.row_count++;
    filterresult.row_offset.push_back(startoff);
    int newlen = 0;
    vector<int> column_startptr;

    for(int i = 0; i < column_filter.size(); i++){
        GetColumnoff(column_filter[i]);
        memcpy(filterresult.data+filterresult.length, row + newstartptr[column_filter[i]], newlengthraw[column_filter[i]]);
        newlen += newlengthraw[column_filter[i]];
        filterresult.length += newlen;
        column_startptr.push_back(newstartptr[column_filter[i]]);
    }
    filterresult.row_column_offset.push_back(column_startptr);
}

vector<string> Filter::split(string str, char Delimiter)
{
    istringstream iss(str); 
    string buffer;          

    vector<string> result;

    while (getline(iss, buffer, Delimiter))
    {
        result.push_back(buffer); 
    }

    return result;
}

bool Filter::isvarc(vector<int> datatype, int ColNum, vector<int> varcharlist)
{
    int isvarchar = 0;
    for (int i = 0; i < ColNum; i++) // varchar 확인
    {
        if (datatype[i] == 15)
        {
            isvarchar = 1;
            varcharlist.push_back(i);
        }
    }
    return isvarchar;
}

void Filter::compareGE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{
    if (LV >= RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}

void Filter::compareGE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{
    if (LV >= RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}

void Filter::compareLE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{
    if (LV <= RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareLE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{
    if (LV <= RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareGT(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{
    if (LV > RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareGT(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{
    if (LV > RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareLT(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{

    if (LV < RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareLT(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{

    if (LV < RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareET(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{

    if (LV == RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareET(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{

    if (LV == RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        {
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}

void Filter::compareNE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{

    if (LV != RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        { 
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}
void Filter::compareNE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved, bool isnot)
{

    if (LV != RV)
    {
        if (TmpV == true)
        {
            CV = true;
            canSaved = true;
        }
        else
        { 
            CV = false;
            canSaved = false;
        }
    }
    else
    {
        CV = false;
        canSaved = false;
    }
    if (isnot)
    {
        if (CV)
        {
            CV = false;
            canSaved = false;
        }
        else
        {
            CV = true;
            canSaved = true;
        }
    }
}

int Filter::typeLittle(unordered_map<string, int> typedata, string colname, char *rowbuf)
{
    if (typedata[colname] == 14)
    {
        int *tmphex;
        char temphexbuf[4];
        int retint;

        memset(temphexbuf, 0, 4);
        GetColumnoff(colname);
        for (int k = 0; k < newlengthraw[colname]; k++)
        {
            temphexbuf[k] = rowbuf[newstartptr[colname] + k];
        }
        tmphex = (int *)temphexbuf;
        retint = tmphex[0];
        return retint;
    }
    else if (typedata[colname] == 8)
    { 
        char intbuf[8];
        int64_t *intbuff;
        int64_t retint;

        memset(intbuf, 0, 8);
        GetColumnoff(colname);
        for (int k = 0; k < newlengthraw[colname]; k++)
        {
            intbuf[k] = rowbuf[newstartptr[colname] + k];
        }
        intbuff = (int64_t*)intbuf;
        retint = intbuff[0];
        return retint;
    }
    else if (typedata[colname] == 3)
    {
        char intbuf[4];
        int *intbuff;
        int retint;

        memset(intbuf, 0, 4);
        GetColumnoff(colname);
        for (int k = 0; k < newlengthraw[colname]; k++)
        {
            intbuf[k] = rowbuf[newstartptr[colname] + k];
        }
        intbuff = (int *)intbuf;
        retint = intbuff[0];
        return retint;
    }else{
        string tmpstring = joinmap[colname];
        return stoi(tmpstring);
    }
    return 0;
}

string Filter::typeBig(string colname, char *rowbuf)
{
    GetColumnoff(colname);
    string tmpstring = "";
    for (int k = 0; k < newlengthraw[colname]; k++)
    {
        tmpstring = tmpstring + (char)rowbuf[newstartptr[colname] + k];
    }
    return tmpstring;
}
string Filter::typeDecimal(string colname, char *rowbuf)
{
    GetColumnoff(colname);

    char tmpbuf[4];
    string tmpstring = "";
    for (int k = 0; k < newlengthraw[colname]; k++)
    {
        ostringstream oss;
        int *tmpdata;
        tmpbuf[0] = 0x80;
        tmpbuf[1] = 0x00;
        tmpbuf[2] = 0x00;
        tmpbuf[3] = 0x00;
        tmpbuf[0] = rowbuf[newstartptr[colname] + k];
        tmpdata = (int *)tmpbuf;
        oss << hex << tmpdata[0];
        if (oss.str().length() <= 1)
        {
            tmpstring = tmpstring + "0" + oss.str();
        }
        else
        {
            tmpstring = tmpstring + oss.str();
        }
    }
    return tmpstring;
}

string Filter::ItoDec(int inum)
{
    std::stringstream ss;
    std::string s;
    ss << hex << inum;
    s = ss.str();
    string decimal = "80";
    for (int i = 0; i < 10 - s.length(); i++)
    {
        decimal = decimal + "0";
    }
    decimal = decimal + s;
    decimal = decimal + "00";
    return decimal;
}

void Filter::GetColumnoff(string colname){
    int startoff = 0;
    int offlen = 0;
    int tmpcount = rowfilterdata.offsetcount;
    for(int i = rowfilterdata.offsetcount; i < rowfilterdata.ColIndexmap[colname] + 1; i++){
        bool varcharflag = 0;
        if(i == 0){
            startoff = rowfilterdata.rowoffset + rowfilterdata.startoff[i];
        }else if (varcharflag == 0){
            startoff = rowfilterdata.startoff[i] + rowfilterdata.rowoffset;
        }else{
            startoff = newstartptr[rowfilterdata.ColName[i-1]] + newlengthraw[rowfilterdata.ColName[i-1]];
        }
        for(int j = 0; j < rowfilterdata.varcharlist.size(); j++){
            if(i == rowfilterdata.varcharlist[j]){
                if (rowfilterdata.offlen[i] < 256){
                    offlen = (int)rowfilterdata.rowbuf[startoff];
                    newstartptr.insert(make_pair(rowfilterdata.ColName[i],startoff + 1));
                    newlengthraw.insert(make_pair(rowfilterdata.ColName[i],offlen));
                }else{
                    char lenbuf[4];
                    memset(lenbuf,0,4);
                    int *lengthtmp;
                    for (int k = 0; k < 2; k++)
                    {
                        lenbuf[k] = rowfilterdata.rowbuf[startoff + k];
                        lengthtmp = (int *)lenbuf;
                    }
                    offlen = lengthtmp[0];
                    newstartptr.insert(make_pair(rowfilterdata.ColName[i],startoff + 2));
                    newlengthraw.insert(make_pair(rowfilterdata.ColName[i],offlen));
                }
                varcharflag = 1;
                break;
            }
        }
        if(varcharflag == 0){newstartptr.insert(make_pair(rowfilterdata.ColName[i],startoff));
            newlengthraw.insert(make_pair(rowfilterdata.ColName[i],rowfilterdata.offlen[i]));
        }
        tmpcount++;
    }
    rowfilterdata.offsetcount = tmpcount;
}

void Filter::JoinOperator(string colname){

}

void MergeManager::Merging(){
    while (1){
        Result result = MergeQueue.wait_and_pop();              
        MergeBlock(result);
    }
}

void MergeManager::MergeBlock(Result &result){
	    
    int row_len = 0;
    int row_num = 0;
    int key = result.work_id;

    if(m_MergeManager.find(key)==m_MergeManager.end()){
        MergeResult block(key, result.csd_name, result.total_block_count);
        m_MergeManager.insert(make_pair(key,block));
    }

    if(result.row_count != 0){
        vector<int> temp_offset;
        temp_offset.assign( result.row_offset.begin(), result.row_offset.end() );
        temp_offset.push_back(result.length);

        if(result.filter_info.groupby_col.size() == 0){



        }else{

        }

        row_len = temp_offset[1] - temp_offset[0];

        if(m_MergeManager[key].length + row_len > BUFF_SIZE){
            ReturnQueue.push_work(m_MergeManager[key]);
            m_MergeManager[key].InitMergeResult();
        }

        for(int i=0; i<result.row_count; i++){
            row_len = temp_offset[i+1] - temp_offset[i];
            if(m_MergeManager[key].length + row_len > BUFF_SIZE){
                ReturnQueue.push_work(m_MergeManager[key]);
                m_MergeManager[key].InitMergeResult();
            }

            m_MergeManager[key].row_offset.push_back(m_MergeManager[key].length); 
            m_MergeManager[key].rows += 1;
            int current_offset = m_MergeManager[key].length;
            for(int j = temp_offset[i]; j<temp_offset[i+1]; j++){
                m_MergeManager[key].data[current_offset] =  result.data[j]; 
                current_offset += 1;
            }
            m_MergeManager[key].length += row_len;
        }

    }

    m_MergeManager[key].current_block_count += result.result_block_count;
    m_MergeManager[key].result_block_count += result.result_block_count;

    if(m_MergeManager[key].total_block_count == m_MergeManager[key].current_block_count){
        ReturnQueue.push_work(m_MergeManager[key]);
        m_MergeManager[key].InitMergeResult();
        cout << "WORK [" << key << "] DONE" << endl;
    }
    
}

void Return::ReturnResult(){

    while (1){
        MergeResult mergeResult = ReturnQueue.wait_and_pop();
                
        SendDataToBufferManager(mergeResult);
    }
}

void Return::SendDataToBufferManager(MergeResult &mergeResult){
    cout << "\n[Send Data To Buffer Manager]" << endl;

    StringBuffer block_buf;
    PrettyWriter<StringBuffer> writer(block_buf);

    writer.StartObject();

    writer.Key("Work ID");
    writer.Int(mergeResult.work_id);

    writer.Key("nrows");
    writer.Int(mergeResult.rows);

    writer.Key("Row Offset");
    writer.StartArray();
    for (int i = 0; i < mergeResult.row_offset.size(); i ++){
        writer.Int(mergeResult.row_offset[i]);
    }
    writer.EndArray();

    writer.Key("Length");
    writer.Int(mergeResult.length);

    writer.Key("CSD Name");
    writer.String(mergeResult.csd_name.c_str());

    writer.Key("Last valid block id");
    writer.Int(mergeResult.last_valid_block_id);

    writer.Key("Result block count");
    writer.Int(mergeResult.result_block_count);

    writer.EndObject();

    string block_buf_ = block_buf.GetString();

    int sockfd;
    struct sockaddr_in serv_addr;
    sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("ERROR opening socket");
    }
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr("10.0.5.33");
    serv_addr.sin_port = htons(BUFF_M_PORT);

    connect(sockfd,(const sockaddr*)&serv_addr,sizeof(serv_addr));
	
	size_t len = strlen(block_buf_.c_str());
	send(sockfd, &len, sizeof(len), 0);
    send(sockfd, (char*)block_buf_.c_str(), len, 0);

    static char cBuffer[PACKET_SIZE];
    if (recv(sockfd, cBuffer, PACKET_SIZE, 0) == 0){
        cout << "client recv Error" << endl;
        return;
    };

    len = mergeResult.length;
    send(sockfd,&len,sizeof(len),0);
    send(sockfd, mergeResult.data, BUFF_SIZE, 0);
    
    close(sockfd);
}

struct TableRep{
    string dev_name;
    bool blocks_maybe_compressed;
    bool blocks_definitely_zstd_compressed;
    bool immortal_table;
    uint32_t read_amp_bytes_per_bit;
    string compression_name;
};

class TableManager{
public:
    TableManager(){}
    
    void InitCSDTableManager();
    TableRep GetTableRep(string table_name);

private:
    unordered_map<string,struct TableRep> table_rep_;
};

void TableManager::InitCSDTableManager(){
    TableRep tr = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
    table_rep_.insert({"small_line.lineitem111_Table1",tr});
	TableRep tr2 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_100000.lineitem_100000",tr2});
    TableRep tr3 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_100000.part",tr3});
    TableRep tr4 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch8test.part",tr4});
    TableRep tr5 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch8test.lineitem_100000",tr5});
    TableRep tr6 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch200.lineitem",tr6});
    TableRep tr7 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.lineitem_100000",tr7});
    TableRep tr8 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.part",tr8});
    TableRep tr9 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.customer",tr9});
    TableRep tr10 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.orders",tr10});
    TableRep tr11 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.partsupp",tr11});
    TableRep tr12 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.supplier",tr12});
    TableRep tr13 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch_line_part.nation",tr13});
    TableRep tr14 = {"/dev/ngd-blk2",false,false,false,0,"NoCompression"};
	table_rep_.insert({"tpch100.lineitem",tr14});
}

TableRep TableManager::GetTableRep(string table_name){
    TableRep temp = table_rep_[table_name];
	return table_rep_[table_name];
}

#pragma once

#ifndef ROCKSDB_LITE


namespace ROCKSDB_NAMESPACE {

class SstFileReader {
 public:
  SstFileReader(const Options& options);

  ~SstFileReader();

  Status Open(const std::string& file_path);

  Iterator* NewIterator(const ReadOptions& options);

  std::shared_ptr<const TableProperties> GetTableProperties() const;

  Status VerifyChecksum(const ReadOptions&);

  Status VerifyChecksum() { return VerifyChecksum(ReadOptions()); }

 private:
  struct Rep;
  std::unique_ptr<Rep> rep_;
};

class BlockInfo{
  public:
    const int block_id;
    const uint64_t block_offset;
    const uint64_t block_size;
    bool is_last_block;

    BlockInfo(int _blockid, uint64_t _block_offset ,
      uint64_t _block_size, bool is_last_block_ = false)
      : block_id(_blockid),
        block_offset(_block_offset),
        block_size(_block_size),
        is_last_block(is_last_block_) {}
};

class SstBlockReader {
  public:
    SstBlockReader(const Options& options,
                  bool _blocks_maybe_compressed,
                  bool _blocks_definitely_zstd_compressed,
                  const bool _immortal_table,
                  uint32_t _read_amp_bytes_per_bit,
                  std::string _dev_name);
    ~SstBlockReader();

    Status Open(BlockInfo* blockinfo_);

    Iterator* NewIterator(const ReadOptions& roptions);
    
  private:
    struct Rep;
    std::unique_ptr<Rep> rep_;

    struct SnippetInfo;
    std::unique_ptr<SnippetInfo> snippetinfo_;

};


struct SstBlockReader::Rep {
  Options options;
  EnvOptions soptions;
  ImmutableOptions ioptions;
  MutableCFOptions moptions;

  std::unique_ptr<InternalIterator> datablock_iter;

  Rep(const Options& opts)
      : options(opts),
        soptions(options),
        ioptions(options),
        moptions(ColumnFamilyOptions(options)){}
};

struct SstBlockReader::SnippetInfo{
  bool blocks_maybe_compressed;
  bool blocks_definitely_zstd_compressed;
  const bool immortal_table;
  uint32_t read_amp_bytes_per_bit;
  std::string dev_name;

  SnippetInfo(bool _blocks_maybe_compressed, 
              bool _blocks_definitely_zstd_compressed,
              const bool _immortal_table,
              uint32_t _read_amp_bytes_per_bit,
              std::string _dev_name)
              : immortal_table(_immortal_table) {
                blocks_maybe_compressed = _blocks_maybe_compressed;
                blocks_definitely_zstd_compressed = _blocks_definitely_zstd_compressed;
                read_amp_bytes_per_bit = _read_amp_bytes_per_bit;
                dev_name = _dev_name;
              }
}; 

SstBlockReader::SstBlockReader(const Options& options,
                              bool _blocks_maybe_compressed,
                              bool _blocks_definitely_zstd_compressed,
                              const bool _immortal_table,
                              uint32_t _read_amp_bytes_per_bit,
                              std::string _dev_name)
                              : rep_(new Rep(options)),
                                snippetinfo_(new SnippetInfo(
                                   _blocks_maybe_compressed, 
                                   _blocks_definitely_zstd_compressed,
                                   _immortal_table,
                                   _read_amp_bytes_per_bit, 
                                   _dev_name)) {}
SstBlockReader::~SstBlockReader() {}

Status SstBlockReader::Open(BlockInfo* blockinfo_) {
  
  auto r = rep_.get();
  auto si = snippetinfo_.get();
  Status s;

  ReadOptions ro;
  s = BlockBasedTable::BlockOpen(
      si->blocks_maybe_compressed, si->blocks_definitely_zstd_compressed, 
      si->immortal_table, si->read_amp_bytes_per_bit,
      BlockType::kData, blockinfo_->block_offset, blockinfo_->block_size, 
      blockinfo_->block_id, si->dev_name, r->ioptions.internal_comparator, 
      r->ioptions.stats, &r->datablock_iter); 

  return s;
}

Iterator* SstBlockReader::NewIterator(const ReadOptions& roptions) {
  auto r = rep_.get();
  auto sequence = roptions.snapshot != nullptr
                      ? roptions.snapshot->GetSequenceNumber()
                      : kMaxSequenceNumber;
  ArenaWrappedDBIter* res = new ArenaWrappedDBIter();
  res->Init(r->options.env, roptions, r->ioptions, r->moptions,
            nullptr, sequence,
            r->moptions.max_sequential_skip_in_iterations,
            0 , nullptr,
            nullptr , nullptr ,
            true , false );
  auto internal_iter = r->datablock_iter.get();
  res->SetIterUnderDBIter(internal_iter);
  return res;

}

Status BlockBasedTable::BlockOpen(
      bool _blocks_maybe_compressed, bool _blocks_definitely_zstd_compressed,
      const bool _immortal_table, uint32_t _read_amp_bytes_per_bit, BlockType _block_type,
      uint64_t _block_offset, uint64_t _block_size, int _block_id, std::string _dev_name,
      const InternalKeyComparator& _internal_comparator, Statistics* _stats, 
      std::unique_ptr<InternalIterator>* datablock_iter) {
  
  BlockRep* blockrep = new BlockBasedTable::BlockRep(
      _internal_comparator, _stats, _blocks_maybe_compressed, 
      _blocks_definitely_zstd_compressed, _immortal_table,
      _read_amp_bytes_per_bit, _block_type, _dev_name);
  
  BlockInfo* blockinfo = new BlockBasedTable::BlockInfo(_block_id, _block_offset,_block_size);

  std::unique_ptr<BlockBasedTable> new_table(new BlockBasedTable(blockrep,blockinfo));
  std::unique_ptr<InternalIterator> datablock_iter_;
  datablock_iter->reset();
  
  datablock_iter_.reset(new_table->KetiNewDataBlockIterator<DataBlockIter>(
      ReadOptions(),
      nullptr, BlockType::kData,
      nullptr, nullptr, Status(),
      nullptr));

  *datablock_iter = std::move(datablock_iter_);  

  return Status::OK();
}

template <typename TBlockIter>
TBlockIter* BlockBasedTable::KetiNewDataBlockIterator(
    const ReadOptions& ro, TBlockIter* input_iter,
    BlockType block_type, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, Status s,
    FilePrefetchBuffer* prefetch_buffer, bool for_compaction) const {

  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  TBlockIter* iter = input_iter != nullptr ? input_iter : new TBlockIter;
  if (!s.ok()) {
    iter->Invalidate(s);
    return iter;
  }
  CachableEntry<Block> block;

  s = RetrieveBlockByPBA(prefetch_buffer, ro, &block, get_context, 
                      lookup_context, for_compaction);

  if (!s.ok()) {
    assert(block.IsEmpty());
    iter->Invalidate(s);
    return iter;
  }

  assert(block.GetValue() != nullptr);
  const bool block_contents_pinned =
      block.IsCached() ||
      (!block.GetValue()->own_bytes() && blockrep_->immortal_table);
  
  iter = KetiInitBlockIterator<TBlockIter>(blockrep_, block.GetValue(), block_type, iter,
                                       block_contents_pinned);

  block.TransferTo(iter);

  return iter;
}

Status BlockBasedTable::RetrieveBlockByPBA(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& ro,
    CachableEntry<Block>* block_entry, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, bool for_compaction) const {
  assert(block_entry);
  assert(block_entry->IsEmpty());
  Status s;

  if(lookup_context==nullptr && for_compaction){
    std::cout<<"."<<std::endl;
  }

  assert(block_entry->IsEmpty());

  const bool no_io = ro.read_tier == kBlockCacheTier;
  if (no_io) {
    return Status::Incomplete("no blocking io");
  }

  std::unique_ptr<Block> block;
  {
    s = ReadBlockFromFileByPBA(
        blockinfo_->block_size, blockrep_->dev_name, blockinfo_->block_offset, 
        prefetch_buffer, ro, &block, blockrep_->block_type, 
        blockrep_->blocks_definitely_zstd_compressed,
        blockrep_->block_type == BlockType::kData
            ? blockrep_->read_amp_bytes_per_bit
            : 0);

    if (get_context) {
      switch (blockrep_->block_type) {
        case BlockType::kIndex:
          ++(get_context->get_context_stats_.num_index_read);
          break;
        case BlockType::kFilter:
          ++(get_context->get_context_stats_.num_filter_read);
          break;
        case BlockType::kData:
          ++(get_context->get_context_stats_.num_data_read);
          break;
        default:
          break;
      }
    }
  }

  if (!s.ok()) {
    return s;
  }

  block_entry->SetOwnedValue(block.release());

  assert(s.ok());

  return s;
}

Status ReadBlockFromFileByPBA(
  uint64_t block_size_, std::string dev_name_, uint64_t block_offset_, 
  FilePrefetchBuffer* prefetch_buffer_, const ReadOptions& options_, 
  std::unique_ptr<Block>* result_, BlockType block_type_, bool using_zstd,
  size_t read_amp_bytes_per_bit_) {
  assert(result_);

  BlockContents contents;
  PbaBlockFetcher pba_block_fetcher(
    block_size_, dev_name_, block_offset_, prefetch_buffer_, 
    options_, &contents, block_type_, using_zstd);
  Status s = pba_block_fetcher.ReadBlockContentsByPBA();
  if (s.ok()) {
    result_->reset(BlocklikeTraits<Block>::Create(
        std::move(contents), read_amp_bytes_per_bit_, nullptr, using_zstd,nullptr));
  }

  return s;
}


}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
