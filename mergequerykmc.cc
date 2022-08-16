#include "mergequerykmc.h"

// typedef string any;

void Init(Value Query)
{
    SnippetStruct snippet;
    // snippet.tablename = Query["tableName"].GetString();
    if (IsJoin(snippet))
    { //버퍼매니저에 테이블의 정보가 있을 경우
        //일단 1번 기준으로만 작성
        ColumnProjection(snippet);
    }
    else
    { 
        JoinTable(snippet);
    }
}

unordered_map<string,vector<any>> GetBufMTable(string tablename, SnippetStruct snippet, BufferManager &buff)
{

        unordered_map<string,vector<any>> table = buff.GetTableData(snippet.query_id, tablename).table_data;
        buff.GetTableInfo(snippet.query_id, tablename); //테이블 데이터 말고 type, name, rownum, blocknum까지 채워줌
        return table;    
     
}

any Postfix(unordered_map<string,vector<any>> tablelist, vector<Projection> data, unordered_map<string,vector<any>> savedTable){
    unordered_map<string,int> stackmap;
    pair<string,vector<any>> tmppair;
    auto tmpiter = tablelist.begin();
    tmppair = *tmpiter;
    int rownum = tmppair.second.size();
    if(data[0].value == "0"){
        // StackType tmpstack;
        stack<any> tmpstack;
        // tmpstack.type = typeid(tablelist[data[1].value]).name;
        for(int i = 0; i < rownum; i++){
            for(int j = 1; j < data.size(); j++){
                if(data[j].type == 3){
                    tmpstack.push(tablelist[data[j].value][i]);
                }else if(data[j].type == 2){
                    if(data[j].value == "+"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else if(data[j].value == "-"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) - any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) - any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else if(data[j].value == "*"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) * any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) * any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else if(data[j].value == "/"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) / any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) / any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else{
                        //string
                    }
                }else if(data[j].type == 1){
                    tmpstack.push(stof(data[j].value));
                }else{
                    tmpstack.push(stoi(data[j].value));
                }

            }
            savedTable["asd"].push_back(tmpstack.top());
        }
    }else if(data[0].value == "1"){ //sum
        stack<any> tmpstack;
        // tmpstack.type = typeid(tablelist[data[1].value]).name;
        for(int i = 0; i < rownum; i++){
            for(int j = 1; j < data.size(); j++){
                if(data[j].type == 3){
                    tmpstack.push(tablelist[data[j].value][i]);
                }else if(data[j].type == 2){
                    if(data[j].value == "+"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) + any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) + any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else if(data[j].value == "-"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) - any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) - any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else if(data[j].value == "*"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) * any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) * any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else if(data[j].value == "/"){
                        any value1 = tmpstack.top();
                        tmpstack.pop();
                        any value2 = tmpstack.top();
                        tmpstack.pop();
                        // if(value1.type == typeid(int&))
                        if(value1.type() == typeid(int&)){
                            int retnum = any_cast<int>(value1) / any_cast<int>(value2);
                            tmpstack.push(retnum);
                        }else if(value1.type() == typeid(float&)){
                            float retnum = any_cast<float>(value1) / any_cast<float>(value2);
                            tmpstack.push(retnum);
                        }
                    }else{
                        //string
                    }
                }else if(data[j].type == 1){
                    tmpstack.push(stof(data[j].value));
                }else{
                    tmpstack.push(stoi(data[j].value));
                }

            }
            //이부분이 sum을 해야함
            any data = 0;
            any tmpnum = tmpstack.top(); //int인지 flaot인지 구분 필요
            if(tmpnum.type() == typeid(int&)){
                data = any_cast<int>(data) + any_cast<int>(tmpnum);
            }else{
                data = any_cast<float>(data) + any_cast<float>(tmpnum);
            }

        }
        savedTable["asd"].push_back(data);
    }
    

}

void Aggregation(SnippetStruct snippet, BufferManager &buff){
    unordered_map<string,vector<any>> tablelist;

    for(int i = 0; i < snippet.tablename.size(); i++){
        //혹시 모를 중복 제거 필요
        unordered_map<string,vector<any>> table = GetBufMTable(snippet.tablename[i], snippet, buff);
        for(auto it = table.begin(); it != table.end(); i++){
            pair<string,vector<any>> pair;
            pair = *it;
            tablelist.insert(pair);
        }
    }

    unordered_map<string,vector<any>> savedTable;
    for(int i = 0; i < snippet.columnProjection.size(); i++){
        any ret;
        ret = Postfix(tablelist,snippet.columnProjection[i], savedTable);
        // savedTable.insert(make_pair(snippet.column_alias[i],ret));
    }
}

void JoinTable(SnippetStruct snippet, BufferManager &buff){
    vector<unordered_map<string,vector<any>>> tablelist;

    for(int i = 0; i < 2; i++){
        unordered_map<string,vector<any>> table = GetBufMTable(snippet.tablename[i], snippet, buff);
        tablelist.push_back(table);
    }

    string joinColumn1 = snippet.table_filter[0]["LV"].GetString();
    string joinColumn2 = snippet.table_filter[0]["RV"].GetString();

    unordered_map<string,vector<any>> savedTable;
    for(auto i = tablelist[0].begin(); i != tablelist[0].end(); i++){
        pair<string,vector<any>> tabledata;
        tabledata = *i;
        vector<any> table;
        savedTable.insert(make_pair(tabledata.first,table));
        
    }
    for(auto i = tablelist[1].begin(); i != tablelist[1].end(); i++){
        pair<string,vector<any>> tabledata;
        tabledata = *i;
        vector<any> table;
        if(savedTable.find(tabledata.first) != savedTable.end()){
            savedTable.insert(make_pair(tabledata.first + "_2", table));
        }else{
            savedTable.insert(make_pair(tabledata.first, table));
        }
    }

    for(int i = 0; i < tablelist[0][joinColumn1].size(); i++){
        for(int j = 0; j < tablelist[1][joinColumn2].size(); j++){
            if(tablelist[0][joinColumn1][i].type() == typeid(int)){
                if(any_cast<int&>(tablelist[0][joinColumn1][i]) == any_cast<int&>(tablelist[1][joinColumn2][j])){
                    for(auto it = tablelist[0].begin(); it != tablelist[0].end(); it++){
                        pair<string,vector<any>> tabledata;
                        tabledata = *it;
                        vector<any> tmptable = tabledata.second;
                        savedTable[tabledata.first].push_back(tmptable[i]);
                    }
                    for(auto it = tablelist[1].begin(); it != tablelist[1].end(); it++){
                        pair<string,vector<any>> tabledata;
                        tabledata = *it;
                        if(savedTable.find(tabledata.first + "_v2") != savedTable.end()){
                            vector<any> tmptable = tabledata.second;
                            savedTable[tabledata.first + "_v2"].push_back(tmptable[j]);
                        }else{
                            vector<any> tmptable = tabledata.second;
                            savedTable[tabledata.first].push_back(tmptable[j]);
                        }
                    }

                }
            }else if(tablelist[0][joinColumn1][i].type() == typeid(float)){
                if(any_cast<float&>(tablelist[0][joinColumn1][i]) == any_cast<float&>(tablelist[1][joinColumn2][j])){
                    for(auto it = tablelist[0].begin(); it != tablelist[0].end(); it++){
                        pair<string,vector<any>> tabledata;
                        tabledata = *it;
                        vector<any> tmptable = tabledata.second;
                        savedTable[tabledata.first].push_back(tmptable[i]);
                    }
                    for(auto it = tablelist[1].begin(); it != tablelist[1].end(); it++){
                        pair<string,vector<any>> tabledata;
                        tabledata = *it;
                        if(savedTable.find(tabledata.first + "_v2") != savedTable.end()){
                            vector<any> tmptable = tabledata.second;
                            savedTable[tabledata.first + "_v2"].push_back(tmptable[j]);
                        }else{
                            vector<any> tmptable = tabledata.second;
                            savedTable[tabledata.first].push_back(tmptable[j]);
                        }
                    }

                }

            }else if(tablelist[0][joinColumn1][i].type() == typeid(string)){
                if(any_cast<string&>(tablelist[0][joinColumn1][i]) == any_cast<string&>(tablelist[1][joinColumn2][j])){
                    for(auto it = tablelist[0].begin(); it != tablelist[0].end(); it++){
                        pair<string,vector<any>> tabledata;
                        tabledata = *it;
                        vector<any> tmptable = tabledata.second;
                        savedTable[tabledata.first].push_back(tmptable[i]);
                    }
                    for(auto it = tablelist[1].begin(); it != tablelist[1].end(); it++){
                        pair<string,vector<any>> tabledata;
                        tabledata = *it;
                        if(savedTable.find(tabledata.first + "_v2") != savedTable.end()){
                            vector<any> tmptable = tabledata.second;
                            savedTable[tabledata.first + "_v2"].push_back(tmptable[j]);
                        }else{
                            vector<any> tmptable = tabledata.second;
                            savedTable[tabledata.first].push_back(tmptable[j]);
                        }
                    }

                }

            }

        }
    }
    buff.SaveTableData(snippet.query_id,snippet.tableAlias,savedTable);
}

void NaturalJoin(SnippetStruct snippet, BufferManager &buff){
    //조인 조건 없음 컬럼명이 같은 모든 데이터 비교해서 같으면 조인
}

void OuterFullJoin(SnippetStruct snippet, BufferManager &buff){
    //같은 값이 없을경우 null을 채워 모든 데이터 유지
}

void OuterLeftJoin(SnippetStruct snippet, BufferManager &buff){
    //왼쪽 테이블 기준으로 같은 값이 없으면 null을 채움
}

void OuterRightJoin(SnippetStruct snippet, BufferManager &buff){
    //오른쪽 테이블 기준으로 같은 값이 없으면 null을 채움
}

void CrossJoin(SnippetStruct snippet, BufferManager &buff){
    //테이블 곱 조인
}

void InnerJoin(SnippetStruct snippet, BufferManager &buff){

}

void GroupBy(SnippetStruct snippet, BufferManager &buff){
    int groupbycount = snippet.groupBy.size();
    unordered_map<string,vector<any>> table = GetBufMTable(snippet.tableAlias, snippet, buff);
    //무슨 테이블로 저장이 되어있을지에 대한 논의 필요(스니펫)

    

}

void OrderBy(SnippetStruct snippet, BufferManager &buff){
    unordered_map<string,vector<any>> table = GetBufMTable(snippet.tableAlias, snippet, buff);
    int ordercount = snippet.orderBy.size();
    vector<sortclass> sortbuf;
    for(int i = 0; i < table[snippet.orderBy[0]].size(); i++){
        sortclass tmpclass;
        unordered_map<string,any> value;
        for(int j = 0; j < ordercount; j++){
            tmpclass.ordername.push_back(snippet.orderBy[j]);
        }
        for(auto j = table.begin(); j != table.end(); j++){
            pair<string,vector<any>> tmppair = *j;
            value.insert(make_pair(tmppair.first,tmppair.second[i]));
        }
        tmpclass.value = value;
        tmpclass.ordercount = ordercount;
        sortbuf.push_back(tmpclass);
    }
    sort(sortbuf.begin(),sortbuf.end());

    unordered_map<string,vector<any>> orderedtable;
    for(int i = 0; i < sortbuf.size(); i++){
        // orderedtable.insert(make_pair())
        for(auto j = sortbuf[i].value.begin(); j != sortbuf[i].value.end(); j++){
            pair<string,any> tmppair = *j;
            if(i == 0){
                vector<any> tmpvector;
                orderedtable.insert(make_pair(tmppair.first,tmpvector));
            }
            orderedtable[tmppair.first].push_back(tmppair.second);
        }
    }
    buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
    buff.SaveTableData(snippet.query_id,snippet.tableAlias,orderedtable);
}





// void GetAccessData()
// { // access lib 구현 후 작성(구현 x)
// }

// void ColumnProjection(SnippetStruct snippet)
// {
//     int nullsize = 0; //논널비트에 대한 정보 테이블 매니저에 추가 해야함
//     //결과를 저장할 벡터 + 데이터를 가져올 벡터 필요
//     // unordered_map<string, int> typemap;
//     snippet.tabledata.clear();
//     snippet.resultstack.clear();
//     snippet.resultdata.clear();
//     for (int i = 0; i < snippet.tableAlias.size(); i++)
//     {
//         VectorType vectortype;
//         vectortype.type = snippet.savetype[i];
//         snippet.resultdata.insert(make_pair(snippet.tableAlias[i], vectortype));
//         StackType stacktype;
//         stacktype.type = snippet.savetype[i];
//         snippet.resultstack.insert(make_pair(snippet.tableAlias[i], stacktype));
//         // snippet.resultstack.insert(make_pair(snippet.tableAlias[i], StackType{}));
//     }
//     for (int i = 0; i < snippet.table_col.size(); i++)
//     {
//         // typemap.insert(make_pair(snippet.table_col[i], snippet.table_datatype[i]));
//         VectorType tmpvector;

//         snippet.tabledata.insert(make_pair(snippet.table_col[i], tmpvector));
//     }
//     // for (int n = 0; n < snippet.tableblocknum; n++)
//     // {
//     for (int i = 0; i < snippet.columnProjection.size(); i++)
//     {
//         // vector<string> a = buffermanager.gettable(snippet.tableProjection[i])
//         // stack<string> tmpstack;
//         switch (atoi(snippet.columnProjection[i][0].value.c_str()))
//         {
//         case KETI_Column_name:
//             for (int j = 1; j < snippet.columnProjection[i].size(); j++)
//             {
//                 for (int k = 0; k < snippet.tablerownum; k++)
//                 {
//                     switch (snippet.columnProjection[i][j].type)
//                     {
//                     case PROJECTION_STRING:
//                         //진짜 string or decimal
//                         /* code */
//                         break;
//                     case PROJECTION_INT:
//                         /* code */
//                         break;
//                     case PROJECTION_FLOAT:
//                         /* code */
//                         break;
//                     case PROJECTION_COL:
//                         //버퍼매니저에서 데이터 가져와야함
//                         /* code */
//                         // snippet.tabledata[snippet.tableProjection[i][j].value].type
//                         // if(typemap[snippet.tableProjection[i][j].value] == 3){
//                         //     // buffermanager.gettable()


//                         // }else if(typemap[snippet.tableProjection[i][j].value] == 246){

//                         // }else if(typemap[snippet.tableProjection[i][j].value] == 14){

//                         // }else if(typemap[snippet.tableProjection[i][j].value] == 4){

//                         // }
//                         if(snippet.resultstack[snippet.tableAlias[i]].type == 1){ //int
                            
//                         }else if(snippet.resultstack[snippet.tableAlias[i]].type == 2){ //float

//                         }

//                         break;
//                     case PROJECTION_OPER:
//                         break;
//                     default:
//                         break;
//                     }
//                 }
//             }

//             /* code */
//             break;
//         case KETI_SUM:
//             /* code */
//             break;
//         case KETI_AVG:
//             /* code */
//             break;
//         case KETI_COUNT:
//             /* code */
//             break;
//         case KETI_COUNTALL:
//             /* code */
//             break;
//         case KETI_MIN:
//             /* code */
//             break;
//         case KETI_MAX:
//             /* code */
//             break;
//         case KETI_CASE:
//             /* code */
//             break;
//         case KETI_WHEN:
//             /* code */
//             break;
//         case KETI_THEN:
//             /* code */
//             break;
//         case KETI_ELSE:
//             /* code */
//             break;
//         case KETI_LIKE:
//             /* code */
//             break;
//         default:
//             break;
//         }
//         // }
//     }
// }

// void GetColOff()
// {
// }