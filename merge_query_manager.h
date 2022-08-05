#include "buffer_manager.h"

extern WorkQueue<Merge_Snippet> MergeSnippetQueue;

struct Merge_Snippet {
    int query_id;
    int work_id;
    string table_alias;//결과 테이블의 별칭
    vector<string> table_name;//작업에 필요한 테이블
    vector<pair<int,vector<string>>> select_clause_info;//select aggregation,column
    vector<pair<int,vector<string>>> x_clause_info;//groupby,orderby,having,limit,desc/asc
    // int work_type;

    Merge_Snippet(int qid, int wid, string table_alias_, vector<string> table_name_, 
                  vector<pair<int,vector<string>>> select_clause_info_,
                  vector<pair<int,vector<string>>> x_clause_info_){
      query_id = qid;
      work_id = wid;
      table_alias = table_alias_;
      table_name.assign(table_name_.begin(),table_name_.end());
      select_clause_info.assign(select_clause_info_.begin(),select_clause_info_.end());
      x_clause_info.assign(x_clause_info_.begin(),x_clause_info_.end());
    }
};

// struct Merge_Query_Buffer{
//   int query_id;
//   unordered_map<int,Merge_Snippet*> merge_work_map;
//   bool is_done;

//   Merge_Query_Buffer(int qid)
//   :query_id(qid){
//     work_info_map.clear();
    
//     is_done = false;
//   }
// };

class MergeQueryManager{	
public:
    MergeQueryManager(Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m){
      InitMergeQueryManager(scheduler, buffer_m, table_m);
    }
    int InitMergeQueryManager(Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m);
    int Join();
    void MergeRunning(Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m);
    void DoMergeSnippet(Merge_Snippet result, Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m);
    int InitWork(int qid, int wid, string table_alias, vector<string> table_name_,
                vector<pair<int,vector<string>>> select_clause_info_,
                vector<pair<int,vector<string>>> x_clause_info_,
                vector<string> table_col_, vector<int> table_offset_,
                vector<int> table_offlen_, vector<int> table_datatype_,
                int bcnt, int table_type_, int is_last, BufferManager &buffer_m);             
    // int InitQuery(int qid, BufferManager &buffer_m);

private:
    // unordered_map<int,Merge_Query_Buffer*> m_MergeQueryManager;
    // WorkQueue<Merge_Snippet> MergeSnippetQueue;
    thread MergeQueryManager_Thread;
};