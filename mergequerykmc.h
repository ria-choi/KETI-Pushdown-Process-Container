#include <iostream>
#include <vector>
#include <unordered_map>
#include <stack>
#include <any>
#include <algorithm>
#include <string>
#include <typeinfo>

#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "keti_type.h"

#include "buffer_manager.h"

using namespace std;
using namespace rapidjson;

#define BufferSize 40960 //사용자 지정 wal 버퍼 크기


struct groupby{
    int count;
    vector<any> value;
    vector<any> savedkey;
};

class sortclass{
    public:
        unordered_map<string,any> value;
        int ordercount;
        vector<string> ordername;

        bool operator <(sortclass &sortbuf){
            for(int i = 0; i < ordercount; i++){
                if(this->value[ordername[i]].type() == typeid(int&)){
                    if(any_cast<int>(this->value[ordername[i]]) == any_cast<int>(sortbuf.value[ordername[i]])){
                        continue;
                    }else{
                        // if() asc desc 구분 필요
                        return any_cast<int>(this->value[ordername[i]]) < any_cast<int>(sortbuf.value[ordername[i]]);
                    }
                }else if(this->value[ordername[i]].type() == typeid(float&)){
                    if(any_cast<float>(this->value[ordername[i]]) == any_cast<float>(sortbuf.value[ordername[i]])){
                        continue;
                    }else{
                        // if() asc desc 구분 필요
                        return any_cast<float>(this->value[ordername[i]]) < any_cast<float>(sortbuf.value[ordername[i]]);
                    }
                }else{
                    if(any_cast<string>(this->value[ordername[i]]) == any_cast<string>(sortbuf.value[ordername[i]])){
                        continue;
                    }else{
                        // if() asc desc 구분 필요
                        return any_cast<string>(this->value[ordername[i]]) < any_cast<string>(sortbuf.value[ordername[i]]);
                    }
                }
            }
            if(this->value[ordername[0]].type() == typeid(int&)){
                return any_cast<int>(this->value[ordername[0]]) < any_cast<int>(sortbuf.value[ordername[0]]);
            }else if(this->value[ordername[0]].type() == typeid(float&)){
                return any_cast<float>(this->value[ordername[0]]) < any_cast<float>(sortbuf.value[ordername[0]]);
            }else{
                return any_cast<string>(this->value[ordername[0]]) < any_cast<string>(sortbuf.value[ordername[0]]);
            }
        }
};

bool compare(sortclass &sortbuffer);

void Init(Value snippet);

unordered_map<string,vector<any>> GetBufMTable(string tablename,SnippetStruct snippet, BufferManager &buff);

void GetAccessData();

void Join();

void DependencyJoin();

void GetColOff();

void ColumnProjection(SnippetStruct snippet);

void SaveTable();

void makeTable(SnippetStruct snippet);

void JoinTable(SnippetStruct snippet, BufferManager &buff);

bool IsJoin(SnippetStruct snippet);

void Aggregation(SnippetStruct snippet, BufferManager &buff);

any Postfix(unordered_map<string,vector<any>> tablelist, vector<Projection> data, unordered_map<string,vector<any>> savedTable);

void InnerJoin(SnippetStruct snippet, BufferManager &buff);

void NaturalJoin(SnippetStruct snippet, BufferManager &buff);

void OuterFullJoin(SnippetStruct snippet, BufferManager &buff);

void OuterLeftJoin(SnippetStruct snippet, BufferManager &buff);

void OuterRightJoin(SnippetStruct snippet, BufferManager &buff);

void CrossJoin(SnippetStruct snippet, BufferManager &buff);

void SelfJoin(SnippetStruct snippet, BufferManager &buff);

void GroupBy(SnippetStruct snippet, BufferManager &buff);

void OrderBy(SnippetStruct snippet, BufferManager &buff);

void GetWALManager(char *data, string TableName);

void DependencyLoopQuery();

void Having();

void Filtering(SnippetStruct snippet);

struct Projection{
    string value;
    int type; // 0 int, 1 float, 2 string, 3 column
};

struct VectorType{
    vector<int> intvec;
    vector<string> stringvec;
    vector<float> floatvec;
    int type;
};

struct StackType{
    stack<int> intstack;
    stack<float> floatstack;
    // type_info type;
    int type;
};

struct SnippetStruct{
        int query_id;
        int work_id;
        string sstfilename;
        Value& block_info_list;
        vector<string> table_col;
        Value& table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> column_alias;
        int tableblocknum;
        int tablerownum;
        vector<string> tablename;
        string tableAlias;
        vector<vector<Projection>> columnProjection;
        vector<string> columnFiltering;
        vector<string> groupBy;
        vector<string> orderBy;
        vector<int> savetype;
        unordered_map<string,VectorType> tabledata;
        unordered_map<string,VectorType> resultdata;
        unordered_map<string, StackType> resultstack;
        char data[BufferSize];

        SnippetStruct(int work_id_, string sstfilename_,
            Value& block_info_list_,
            vector<string> table_col_, Value& table_filter_, 
            vector<int> table_offset_, vector<int> table_offlen_,
            vector<int> table_datatype_)
            : work_id(work_id_), sstfilename(sstfilename_),
            block_info_list(block_info_list_),
            table_col(table_col_),
            table_filter(table_filter_),
            table_offset(table_offset_),
            table_offlen(table_offlen_),
            table_datatype(table_datatype_) {};

        SnippetStruct();
};