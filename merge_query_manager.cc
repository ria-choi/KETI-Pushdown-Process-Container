#include "merge_query_manager.h"

int MergeQueryManager::InitMergeQueryManager(Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m){
    MergeQueryManager_Thread = thread([&](){MergeQueryManager::MergeRunning(scheduler, buffer_m, table_m);});
    return 0;
}

int MergeQueryManager::Join(){
    MergeQueryManager_Thread.join();
    return 0;
}

void MergeQueryManager::MergeRunning(Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m){
    while(1){
        Merge_Snippet mergeSnippet = MergeSnippetQueue.wait_and_pop();

        if(buffer_m.my_buffer_m().find(mergeSnippet.query_id) == buffer_m.my_buffer_m().end()){
            cout << "<error> No Query ID, Init Query first! " << endl;
            continue;
        }else if(buffer_m.my_buffer_m()[mergeSnippet.query_id]->work_buffer_list.find(mergeSnippet.work_id) 
                    == buffer_m.my_buffer_m()[mergeSnippet.query_id]->work_buffer_list.end()){
            cout << "<error> No Work ID, Init Work first!" << endl;
            continue;            
        }   
        
        vector<string>::iterator iter;
        for (iter = mergeSnippet.table_name.begin(); iter != mergeSnippet.table_name.end(); ++iter){
            if(!buffer_m.CheckTableStatus(mergeSnippet.query_id, (*iter))){
                //작업 아직 시작하면 안됨
            }
        }
        
        DoMergeSnippet(mergeSnippet, scheduler, buffer_m, table_m);
    }
}

void DoMergeSnippet(Merge_Snippet result, Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m){

}

int MergeQueryManager::InitWork(int qid, int wid, string table_alias, vector<string> table_name_,
                                vector<pair<int,vector<string>>> select_clause_info_,
                                vector<pair<int,vector<string>>> x_clause_info_,
                                vector<string> table_col_, vector<int> table_offset_,
                                vector<int> table_offlen_, vector<int> table_datatype_,
                                int bcnt, int table_type_, int is_last, BufferManager &buffer_m){
    cout << "#Init Work! [" << qid << "-" << wid << "] (MergeQueryManager)" << endl;
    
    buffer_m.InitWork(qid, wid, table_alias, select_clause_info_, x_clause_info_, 
                        table_col_, table_offset_, table_offlen_, table_datatype_, bcnt,
                        table_type_, is_last);

    Merge_Snippet mergeSnippet(qid, wid, table_alias, table_name_, select_clause_info_, x_clause_info_);
    MergeSnippetQueue.push_work(mergeSnippet);

    return 1;
}

// int MergeQueryManager::SetWork(int qid, int wid,
//                     vector<pair<int,vector<string>>> select_clause_info_,
//                     vector<pair<int,vector<string>>> x_clause_info_,
//                     Scheduler &scheduler, BufferManager &buffer_m, TableManager &table_m){
//     WorkInfo* workInfo = m_MergeQueryManager[qid]->work_info_map[wid];

//     for(int i=0; i<workInfo->table_name.size(); i++){
//         string table_name_ = workInfo->table_name[i];
//         if(m_MergeQueryManager[qid]->table_status.find(table_name_) 
//                 == m_MergeQueryManager[qid]->table_status.end()){
//             cout << "<error> No Table Init, Snippet Order Error! " << endl;
//             return -1;
//         }else if(!m_MergeQueryManager[qid]->table_status[table_name_]){
//             cout << "Table Work Not Done. Wait Work To Be Done... " << endl;
//             return -1;
//         }
//     }

//     workInfo->select_clause_info.assign(select_clause_info_.begin(),select_clause_info_.end());
//     workInfo->x_clause_info.assign(x_clause_info_.begin(),x_clause_info_.end());

//     //merge Snippet정보로 그 안에 있는 테이블 확인해서 버퍼에서 찾아서 테이블끼리 연산 수행

// }

// int MergeQueryManager::InitQuery(int qid, BufferManager &buffer_m){//query_id, work 개수
//     cout << "#Init Query! [" << qid << "] (MergeQueryManager)" << endl;

//     if(!(m_MergeQueryManager.find(qid)==m_MergeQueryManager.end())){
//         cout << "Query ID Duplicate Error" << endl;
//         return -1;
//     }

//     Merge_Query_Buffer* mergeQueryBuffer = new Merge_Query_Buffer(qid);

//     m_MergeQueryManager.insert(pair<int,Merge_Query_Buffer*>(qid,mergeQueryBuffer));

//     buffer_m.InitQuery(qid);

//     return 1;
    
// }