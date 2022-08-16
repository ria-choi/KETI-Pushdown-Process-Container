#include "SnippetManager.h"

void SnippetManager::NewQuery(queue<SnippetStruct> newqueue, BufferManager &buff){
    //스레드 생성
    SavedRet tmpquery;
    tmpquery.NewQuery(newqueue);

    // thread t1 = thread(NewQueryMain,tmpquery);
    // t1.join(); //고민필요
    NewQueryMain(tmpquery,buff);
}

unordered_map<string,vector<any>> SnippetManager::ReturnResult(int queryid){
    //스레드 생성
    unordered_map<string,vector<any>> ret;
    SavedResult[queryid].lockmutex();
    ret = SavedResult[queryid].ReturnResult();
    SavedResult[queryid].unlockmutex();
    return ret;
}

 void SnippetManager::InitSnippetManager(TableManager &table){
    // bufferManager = buff;
    tableManager = table;
 }

void SavedRet::NewQuery(queue<SnippetStruct> newqueue){
    QueryQueue = newqueue;
}

unordered_map<string,vector<any>> SavedRet::ReturnResult(){
    return result_;
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

void SavedRet::SetResult(unordered_map<string,vector<any>> result){
    result_ = result;
}

void NewQueryMain(SavedRet &snippetdata, BufferManager &buff){
    //스레드로 동작하는 각 쿼리별 동작
    snippetdata.lockmutex();
    int LQID = 0; //last query id
    string LTName; //last table name
    for(int i = 0; i < snippetdata.GetSnippetSize(); i++){
        SnippetStruct snippet = snippetdata.Dequeue();

        GetSnippetType(snippet,buff);
        LQID = snippet.query_id;
        LTName = snippet.tableAlias;
    }
    snippetdata.SetResult(buff.GetTableData(LQID,LTName).table_data);
    snippetdata.unlockmutex();
}

int GetSnippetType(SnippetStruct snippet, BufferManager &buff){
    //비트 마스킹으로 교체 예정
    //여기서 각 작업별 수행 위치 선정
    if(GetWALTable(snippet)){
        //머지쿼리에 데이터 필터 요청
        Filtering(snippet); //wal 데이터 필터링
    }
    // if(snippet.tablename.size() > 1){
    //     return 18;
    // }
    // // if(hasBufM(Snippet["table_name"][0].GetString())){
    // //     return 18;
    // // }
    // if(buff.CheckTableStatus(snippet.query_id, snippet.tablename[1])){
    //     return 18;
    // }
    // if(tablemanager.get_IndexList())
    return 16;

}

void ReturnColumnType(SnippetStruct snippet, BufferManager &buff){
    unordered_map<string,int> columntype;
    vector<int> return_datatype;
    bool bufferflag = false;
    // vector<Snippet> bufferManagerTableinfolist;
    if(snippet.tablename.size() > 1){
        bufferflag = true;
        // bufferManager.gettableinfo()
        for(int i = 0; i < snippet.tablename.size();i++){
            SnippetStruct bufferManagerTableinfo;
            TableInfo tmpinfo = buff.GetTableInfo(snippet.query_id, snippet.tablename[i]);
            // bufferManagerTableinfolist.push_back(bufferManagerTableinfo);
            for(int j = 0; j < tmpinfo.table_column.size(); j++){
                //만약 같은 이름이라면에 대한 처리 필요
                columntype.insert(make_pair(tmpinfo.table_column[j],tmpinfo.table_datatype[j]));
            }
        }
        //무조건 buffermanager
    }else{
        if(buff.CheckTableStatus(snippet.query_id, snippet.tablename[0])){
            //있다 --> buffermanager
            bufferflag = true;
            SnippetStruct bufferManagerTableinfo;
            TableInfo tmpinfo = buff.GetTableInfo(snippet.query_id,snippet.tablename[0]);
            // bufferManagerTableinfolist.push_back(bufferManagerTableinfo);
            for(int j = 0; j < bufferManagerTableinfo.table_col.size(); j++){
                columntype.insert(make_pair(tmpinfo.table_column[j],tmpinfo.table_datatype[j]));
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

void GetIndexNumber(string TableName, vector<int> &IndexNumber){
    //메타 매니저에 테이블 이름을 주면 인덱스 넘버 리턴
}

bool GetWALTable(SnippetStruct snippet){
    //WAL 매니저에 인덱스 번호를 주면 데이터를 리턴 없으면 false
    vector<int> indexnumber;
    // GetIndexNumber(TableName,indexnumber);
    if(snippet.tablename.size() > 1){
        return false;
    }
    GetIndexNumber(snippet.tablename[0],indexnumber);
    //wal manager에 인덱스 넘버 데이터 있는지 확인
    return false;
}