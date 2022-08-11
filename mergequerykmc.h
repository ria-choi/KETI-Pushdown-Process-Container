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

using namespace std;
using namespace rapidjson;


void Init(Value snippet);

unordered_map<string,vector<any>> GetBufMTable(string tablename,SnippetStruct snippet);

void GetAccessData();

void Join();

void DependencyJoin();

void GetColOff();

void ColumnProjection(SnippetStruct snippet);

void SaveTable();

void makeTable(SnippetStruct snippet);

void JoinTable(SnippetStruct snippet);

bool IsJoin(SnippetStruct snippet);

void Aggregation(SnippetStruct snippet);

any Postfix(unordered_map<string,vector<any>> tablelist, vector<Projection> data, unordered_map<string,vector<any>> savedTable);

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
        vector<string> tableAlias;
        vector<vector<Projection>> columnProjection;
        vector<string> columnFiltering;
        vector<string> groupBy;
        vector<string> orderBy;
        vector<int> savetype;
        unordered_map<string,VectorType> tabledata;
        unordered_map<string,VectorType> resultdata;
        unordered_map<string, StackType> resultstack;
        char* data;

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