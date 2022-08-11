#include "SnippetManager.h"

void SnippetManager::NewQuery(queue<SnippetStruct> newqueue){
    //스레드 생성
    SavedRet tmpquery;
    tmpquery.NewQuery(newqueue);

    // thread t1 = thread(NewQueryMain,tmpquery);
    // t1.join(); //고민필요
    NewQueryMain(tmpquery);
}

unordered_map<string,vector<any>> SnippetManager::ReturnResult(int queryid){
    //스레드 생성
    unordered_map<string,vector<any>> ret;
    SavedResult[queryid].lockmutex();
    ret = SavedResult[queryid].ReturnResult();
    SavedResult[queryid].unlockmutex();
    return ret;
}

void SavedRet::NewQuery(queue<SnippetStruct> newqueue){
    QueryQueue = newqueue;
}

unordered_map<string,vector<any>> SavedRet::ReturnResult(){
    return result;
}

void SavedRet::lockmutex(){
    mutex_.lock();
}

void SavedRet::unlockmutex(){
    mutex_.unlock();
}

SnippetStruct SavedRet::Dequeue(){
    SnippetStruct tmp = QueryQueue.front();
    QueryQueue.pop();
    return tmp;
}

int SavedRet::GetSnippetSize(){
    return QueryQueue.size();
}

void NewQueryMain(SavedRet &snippetdata){
    //스레드로 동작하는 각 쿼리별 동작
    snippetdata.lockmutex();
    int LQID = 0; //last query id
    vector<string> LTName; //last table name
    for(int i = 0; i < snippetdata.GetSnippetSize(); i++){
        SnippetStruct snippet = snippetdata.Dequeue();

        GetSnippetType(snippet);
        LQID = snippet.query_id;
        LTName = snippet.tableAlias;
    }
}

int GetSnippetType(SnippetStruct snippet, TableManager tablemanager){
    //비트 마스킹으로 교체 예정
    if(snippet.tablename.size() > 1){
        return 18;
    }
    // if(hasBufM(Snippet["table_name"][0].GetString())){
    //     return 18;
    // }
    if(buffermanager.CheckTableStatus(snippet.query_id, snippet.tablename[1])){
        return 18;
    }
    // if(tablemanager.get_IndexList())
    return 16;

}

bool hasBufM(string tablename){
    //버퍼매니저가 해당 테이블 가지고 있는지 확인
}

void ReturnColumnType(SnippetStruct snippet){
    unordered_map<string,int> columntype;
    vector<int> return_datatype;
    bool bufferflag = false;
    // vector<Snippet> bufferManagerTableinfolist;
    if(snippet.tablename.size() > 1){
        bufferflag = true;
        // bufferManager.gettableinfo()
        for(int i = 0; i < snippet.tablename.size();i++){
            SnippetStruct bufferManagerTableinfo;
            bufferManager.gettableinfo(bufferManagerTableinfo);
            // bufferManagerTableinfolist.push_back(bufferManagerTableinfo);
            for(int j = 0; j < bufferManagerTableinfo.table_col.size(); j++){
                //만약 같은 이름이라면에 대한 처리 필요
                columntype.insert(make_pair(bufferManagerTableinfo.table_col[j],bufferManagerTableinfo.table_datatype[j]));
            }
        }
        //무조건 buffermanager
    }else{
        if(bufferManager.istable(snippet.query_id, snippet.tablename[1])){
            //있다 --> buffermanager
            bufferflag = true;
            SnippetStruct bufferManagerTableinfo;
            bufferManager.gettableinfo(bufferManagerTableinfo);
            // bufferManagerTableinfolist.push_back(bufferManagerTableinfo);
            for(int j = 0; j < bufferManagerTableinfo.table_col.size(); j++){
                columntype.insert(make_pair(bufferManagerTableinfo.table_col[j],bufferManagerTableinfo.table_datatype[j]));
            }
        }else{
            //없다 --> 여기 데이터
            for(int i = 0; i < snippet.table_col.size(); i++){
                columntype.insert(make_pair(snippet.table_col[i],snippet.table_datatype[i]));
            }
        }
    }
    for(int i = 0; i <snippet.columnProjection.size(); i++){
        for(int j = 1; j < snippet.columnProjection[i].size(); j++){
            if(snippet.columnProjection[i][0].value == "3" || snippet.columnProjection[i][0].value == "4"){
                //카운트 일 경우 리턴타입은 int
                return_datatype.push_back(KETI_INT32);
                break;
            }
            if(snippet.columnProjection[i][j].type == 3){
                return_datatype.push_back(columntype[snippet.columnProjection[i][j].value]);
                break;
            }else{
                continue;
            }
            
        }
    }

}