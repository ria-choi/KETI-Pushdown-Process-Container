#include "mergequerykmc.h"

void Init(Value Query)
{
    Snippet snippet;
    snippet.tablename = Query["tableName"].GetString();
    if (GetBufMTable(snippet))
    { //버퍼매니저에 테이블의 정보가 있을 경우
        //일단 1번 기준으로만 작성
        ColumnProjection(snippet);
    }
    else
    { //버퍼매니저에 테이블 정보가 없을 경우 --> 새로 스캔
        GetAccessData();
    }
}

bool GetBufMTable(Snippet &snippet)
{
    if (buffermanager.istable(snippet.tablename))
    {
        buffermanager.gettableinfo(snippet); //테이블 데이터 말고 type, name, rownum, blocknum까지 채워줌
        return 1;                            //있다
        // snippet 구조체에 table.name을 바탕으로 offset, offlen, datatype를 가져옴
    }
    else
    {
        return 0; //없다
    }
}

void GetAccessData()
{ // access lib 구현 후 작성
}

void ColumnProjection(Snippet snippet)
{
    int nullsize = 0; //논널비트에 대한 정보 테이블 매니저에 추가 해야함
    //결과를 저장할 벡터 + 데이터를 가져올 벡터 필요
    // unordered_map<string, int> typemap;
    snippet.tabledata.clear();
    snippet.resultstack.clear();
    snippet.resultdata.clear();
    for (int i = 0; i < snippet.tableAlias.size(); i++)
    {
        VectorType vectortype;
        vectortype.type = snippet.savetype[i];
        snippet.resultdata.insert(make_pair(snippet.tableAlias[i], vectortype));
        StackType stacktype;
        stacktype.type = snippet.savetype[i];
        snippet.resultstack.insert(make_pair(snippet.tableAlias[i], stacktype));
        // snippet.resultstack.insert(make_pair(snippet.tableAlias[i], StackType{}));
    }
    for (int i = 0; i < snippet.table_col.size(); i++)
    {
        // typemap.insert(make_pair(snippet.table_col[i], snippet.table_datatype[i]));
        VectorType tmpvector;

        snippet.tabledata.insert(make_pair(snippet.table_col[i], tmpvector));
    }
    // for (int n = 0; n < snippet.tableblocknum; n++)
    // {
    for (int i = 0; i < snippet.tableProjection.size(); i++)
    {
        // vector<string> a = buffermanager.gettable(snippet.tableProjection[i])
        // stack<string> tmpstack;
        switch (atoi(snippet.tableProjection[i][0].value.c_str()))
        {
        case KETI_Column_name:
            for (int j = 1; j < snippet.tableProjection[i].size(); j++)
            {
                for (int k = 0; k < snippet.tablerownum; k++)
                {
                    switch (snippet.tableProjection[i][j].type)
                    {
                    case PROJECTION_STRING:
                        //진짜 string or decimal
                        /* code */
                        break;
                    case PROJECTION_INT:
                        /* code */
                        break;
                    case PROJECTION_FLOAT:
                        /* code */
                        break;
                    case PROJECTION_COL:
                        //버퍼매니저에서 데이터 가져와야함
                        /* code */
                        // snippet.tabledata[snippet.tableProjection[i][j].value].type
                        // if(typemap[snippet.tableProjection[i][j].value] == 3){
                        //     // buffermanager.gettable()


                        // }else if(typemap[snippet.tableProjection[i][j].value] == 246){

                        // }else if(typemap[snippet.tableProjection[i][j].value] == 14){

                        // }else if(typemap[snippet.tableProjection[i][j].value] == 4){

                        // }
                        if(snippet.resultstack[snippet.tableAlias[i]].type == 1){ //int
                            
                        }else if(snippet.resultstack[snippet.tableAlias[i]].type == 2){ //float

                        }

                        break;
                    case PROJECTION_OPER:
                        break;
                    default:
                        break;
                    }
                }
            }

            /* code */
            break;
        case KETI_SUM:
            /* code */
            break;
        case KETI_AVG:
            /* code */
            break;
        case KETI_COUNT:
            /* code */
            break;
        case KETI_COUNTALL:
            /* code */
            break;
        case KETI_MIN:
            /* code */
            break;
        case KETI_MAX:
            /* code */
            break;
        case KETI_CASE:
            /* code */
            break;
        case KETI_WHEN:
            /* code */
            break;
        case KETI_THEN:
            /* code */
            break;
        case KETI_ELSE:
            /* code */
            break;
        case KETI_LIKE:
            /* code */
            break;
        default:
            break;
        }
        // }
    }
}

void GetColOff()
{
}