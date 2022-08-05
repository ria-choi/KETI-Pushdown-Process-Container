#include "SnippetManager.h"



int GetSnippetType(Value &Snippet, TableManager tablemanager){
    if(Snippet["table_name"].Size() > 1){
        return 18;
    }
    if(hasBufM(Snippet["table_name"][0].GetString())){
        return 18;
    }
    // if(tablemanager.get_IndexList())
    return 16;

}

bool hasBufM(string tablename){
    //버퍼매니저가 해당 테이블 가지고 있는지 확인
}

