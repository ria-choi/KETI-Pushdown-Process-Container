#include "StorageEngineInputInterface.h"



TableManager tblManager;
Scheduler csdscheduler;
BufferManager bufma(csdscheduler,tblManager);
atomic<int> WorkID;
unordered_map<string,int> fullquerymap;
int totalrow = 0;
int key;
// unordered_map<char,int> base64map;
// std::vector<int> base64map(256, -1);
// void initmap(){
//     static const std::string b = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
//     for(int i = 0; i < 64; i++){
//         base64map[b[i]] = i;
//     }
// }

// string Base64Decoder(string value){
//     // static const std::string b = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
//     std::string out;
//     // std::vector<int> T(256, -1);
 
//     // for (int i = 0; i < 64; i++)
//     //     T[b[i]] = i;
 
//     int val = 0, valb = -8;
 
//     for (uchar c : value) {
//         if (base64map[c] == -1)
//             break;
 
//         val = (val << 6) + base64map[c];
//         valb += 6;
 
//         if (valb >= 0) {
//             out.push_back(char((val >> valb) & 0xFF));
//             valb -= 8;
//         }
//     }
//     return out;
// }

// string UnicodeDecoder(string Operator){

//     wchar_t const * utf16_string = wstring(Operator.begin(), Operator.end()).c_str();

//     std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>,wchar_t> convert;

//     std::string utf8_string = convert.to_bytes(utf16_string);

//     return utf8_string;

// }

// FullQueryData parsingjson(string query){
//     vector<string> tablelist;
//     Document queryparse;
//     FullQueryData fullQueryData;
//     // unordered_map<string,TableQueryData> parsingdata;
//     // TableQuery tablequery;
//     // unordered_map<string,string> TableQuery;
// 	queryparse.Parse(query.c_str());
//     for(int i = 0; i < queryparse["Select"].Size(); i++){
//         SelectQueryData selectQueryData;
//         parsingFrom(queryparse["Select"][i]["From"],selectQueryData.parsingdata);
//         parsingSelect(queryparse["Select"][i]["Select"],selectQueryData);
//         parsingWhere(queryparse["Select"][i]["Where"],selectQueryData.parsingdata);
//         parsingOrder(queryparse["Select"][i]["OrderBy"],selectQueryData);
//         fullQueryData.selectquerys.push_back(selectQueryData);
//         fullQueryData.operators.push_back(queryparse["Operator"][i].GetString());
//     }
//     // for(auto& v : queryparse["Table"].GetArray()){
//     //     // tablelist.push_back(v.GetString());
//     //     vector<TableManager::ColumnSchema> tmptableschema;
//     //     tblManager.get_table_schema(v.GetString(),tmptableschema);
//     //     parsingdata.insert(make_pair(v.GetString(),tmptableschema));
//     //     TableQuery.insert(make_pair(v.GetString(),""));
//     // }
    
//     // tblManager.get_table_schema();
//     return fullQueryData;
// }

// void parsingOrder(Value &Order, SelectQueryData &fullQueryData){
//     if(Order.IsNull()){
//         return;
//     }
//     for(int i = 0; i < Order.Size(); i++){
//         OrderData orderdata;
//         orderdata.ColumnName = Order[i]["Name"].GetString();
//         string tablename = getTableName(orderdata.ColumnName);
//         fullQueryData.orderData.push_back(orderdata);
//         fullQueryData.parsingdata[tablename].orderData.push_back(orderdata);
//     }
// }

// void parsingSelect(Value &Select, SelectQueryData &fullQueryData){
//     // for(int i = 0; i < Select.Size(); i++){
//     //     if(Select[i]["Expr"].HasMember("Exprs")){

//     //     }
//     // }
//     if(Select.HasMember("UseResult")){
//         for(int i = 0; i < Select["UseResult"].Size(); i++){
//             string ColumnName = Select["UseResult"][i].GetString();
//             string TableName = "a";
//             Column column;
//             column.Result = true;
//             column.as = "asd";
//             fullQueryData.parsingdata[TableName].tableColumn.insert(make_pair(Select["UseResult"][i].GetString(),column));
//         }
//     }
//     if(Select.HasMember("UseStorageQuery")){
//         for(int i = 0; i < Select["UseStorageQuery"].Size(); i++){
//             string ColumnName = Select["UseStorageQuery"][i].GetString();
//             string TableName = "a";
//             Column column;
//             column.Storage = true;
//             column.as = "asd";
//             fullQueryData.parsingdata[TableName].tableColumn.insert(make_pair(Select["UseStorageQuery"][i].GetString(),column));
//         }
//     }
//     if(Select.HasMember("UseCsdQuery")){
//         for(int i = 0; i < Select["UseCsdQuery"].Size(); i++){
//             string ColumnName = Select["UseCsdQuery"][i].GetString();
//             string TableName = "a";
//             Column column;
//             column.CSD = true;
//             column.as = "asd";
//             fullQueryData.parsingdata[TableName].tableColumn.insert(make_pair(Select["UseCsdQuery"][i].GetString(),column));
//         }
//     }
//     if(Select.HasMember("Result")){
//         vector<SelectResult> selectResults;
//         for(int i = 0; i < Select["Result"].Size(); i++){
//             SelectResult selectResult;
//             selectResult.Left = Select["Result"][i]["Left"].GetString();
//             selectResults.push_back(selectResult);
//         }
//         fullQueryData.selectResult = selectResults;
//     }
// }

// void parsingFrom(Value &From, unordered_map<string,TableQueryData> &parsingdata){
//     // unordered_map<string,vector<TableManager::ColumnSchema>> tabledata;
//     vector<TableManager::ColumnSchema> tableSchema;
//     if(From.HasMember("TableList")){
//         for(int i = 0; i < From["TableList"].Size(); i++){
//             tblManager.get_table_schema(From["TableList"][i].GetString(),tableSchema);
//             // parsingdata.tableSchema.insert(make_pair(From["TableList"][i].GetString(), tableSchema));
//             parsingdata.insert(make_pair(From["TableList"][i].GetString(),TableQueryData{}));
//             parsingdata[From["TableList"][i].GetString()].tableSchema = tableSchema;
//             parsingdata[From["TableList"][i].GetString()].tablename = From["TableList"][i].GetString();
//         }
//         parsingdata.insert(make_pair("Storage",TableQueryData{}));
//     }
// }

// void parsingWhere(Value &Where, unordered_map<string,TableQueryData> &parsingdata){
//     for(int i = 0; i < Where.Size(); i++){
//         vector<Query> tableQuery;
//         vector<Aggregation> tableaAggregation;
//         for(int j = 0; j < Where[i].Size(); j++){
//             if(Where[i][j].HasMember("Aggregation")){
//                 for(int k = 0; k < Where[i][j]["Aggregation"].Size(); k++){
//                     Aggregation aggregation;
//                     aggregation.Left = Where[i][j]["Aggregation"][k]["Left"].GetString();
//                     aggregation.Right = Where[i][j]["Aggregation"][k]["Rigth"].GetString();
//                     aggregation.Operator = Where[i][j]["Aggregation"][k]["Operator"].GetInt();
//                     tableaAggregation.push_back(aggregation);
//                 }
//             }else{
//                 Query query;
//                 query.Left = Where[i][j]["Left"].GetString();
//                 query.Operator = Where[i][j]["Operator"].GetInt();
//                 query.Right = Where[i][j]["Right"].GetString();
//                 tableQuery.push_back(query);
//             }
//         }
//         parsingdata[Where[i].GetString()].tableQuery = tableQuery;
//         parsingdata[Where[i].GetString()].tableAggregation = tableaAggregation;
//     }
// }

string getTableName(string Column){
    int pos = Column.find_last_of(".",1);
    return Column.substr(0,pos);
}

// sumtest sumt;

int main(int argc, char const *argv[])
{
    initmap();
	//init table manager	
	tblManager.init_TableManager();
	//init WorkID
	WorkID=0;
	
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    // char *hello = "Hello from server";
       
    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
       
    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                                                  &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons( PORT );
       
    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr *)&address, 
                                 sizeof(address))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, NCONNECTION) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }
	
	thread(accept_connection, server_fd).detach();

	while (1);
	
	close(server_fd);
	bufma.Join();

    //send(new_socket , test_buf , 1024 , 0 );
    //printf("Hello message sent\n");
	
    return 0;
}
void accept_connection(int server_fd){
	while (1) {
		int new_socket;
		struct sockaddr_in address;
		int addrlen = sizeof(address);
        int numread;
		char buffer[4096] = {0};
        string full_query = "";
        size_t length;
        FullQueryData fullquerydata;

		if ((new_socket = accept(server_fd, (struct sockaddr *)&address, 
			(socklen_t*)&addrlen))<0)
		{
			perror("accept");
			exit(EXIT_FAILURE);
		}
		
		read( new_socket , &length, sizeof(length));

        while(1) {
            if ((numread = read( new_socket , buffer, BUFF_SIZE - 1)) == -1) {
                perror("read");
                exit(1);
            }
        
            length -= numread;
            buffer[numread] = '\0';
            full_query += buffer;

            if (length == 0)
                break;
	    }
		// std::cout << buffer << std::endl;

        fullquerydata = parsingjson(full_query);
        //수정 필요(유니온 추가해야댐)
				
		//parse json	
		// Document document;
		// document.Parse(buffer);
		
		// Value &type = document["type"];
		// string full_query;
		// if(document.HasMember("full_query")){
			// full_query = document["full_query"].GetString();
		if(fullquerymap.find(full_query) == fullquerymap.end()){
			bufma.InitQuery(full_query);
			fullquerymap.insert(make_pair(full_query,1));
		}
		// }
		boost::thread_group tg;

		// key = document["key"].GetInt();

        //excutor
        //여기서 유니온 먼저 한번 걸러줘야 함
        // 1.BufferManager::InitQuery(int qid, vector<int> select_list)
        // 2. BufferManager::InitSelect(int qid, int sid, vector<int> wid_list)
        // 3. BufferManager::SetWork(int qid, int sid, int wid, string table_name_,
        // vector<tuple<string,string,string>> join_, vector<int> block_list_)
        for(int uq = 0; uq < fullquerydata.selectquerys.size(); uq++){
            //유니온 등의 실행 순서 설정
            SelectQueryData selectquerydata = ExecuteFullQuery(fullquerydata);
            //여기서 buffer manager와 통신을 통해 Union 초기 설정 해야함


            
            for(int k = 0; k < selectquerydata.tablename.size(); k++){
                string ExecuteTable = ExecuteQuery(selectquerydata, tblManager);
                // int type = 0;

                //index Used Check
                int checknum = IsUsedIndex(selectquerydata.parsingdata[ExecuteTable],tblManager);
                if(checknum == -1){
                    //여긴 인덱스 없음
                    //인덱스가 없으므로 그냥 풀 스캔
                    //어디서 할 것인지 정의를 위한 predict 필요
                    IsPushdownPredicate();
                }else if(checknum == 1){
                    SEIndexPredicate();
                }


                switch(selectquerydata.parsingdata[ExecuteTable].scantype){
                    case SE_INDEX_SCAN:
                        SEIndexScan();
                        //내부적으로 complex scan 수행 or se index seek 수행
                        break;
                    case SE_FULL_SCAN:
                        SEFullScan();
                        break;
                    case SE_COVERING_INDEX:
                        SECoveringIndex();
                        break;
                    // case SE_INDEX_SEEK:
                    //     SEIndexSeek();
                    //     break;
                    case CSD_FULL_SCAN:{
                        std::cout << "DO CSD_FULL_SCAN" << std::endl;

                        int work_id = WorkID.load();
                        WorkID++;
                        
                        std::string req_json;
                        std::string res_json;
                        
                        tblManager.generate_req_json(ExecuteTable,req_json);

                        
                        //do LBA2PBA
                        my_LBA2PBA(req_json,res_json);

                        Document reqdoc;
                        reqdoc.Parse(req_json.c_str());
                        vector<string> sstfilename;


                        for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i ++){
                            sstfilename.push_back(reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
                        }
                        

                        Document blockdoc;
                        blockdoc.Parse(res_json.c_str());

                        vector<int> offset;
                        vector<int> offlen;
                        vector<int> datatype;
                        vector<string> colname;
                        string comma = ".";
                        // tblManager.print_TableManager();
                        
                        vector<TableManager::ColumnSchema>::iterator itor = selectquerydata.parsingdata[ExecuteTable].tableSchema.begin();
                        for(; itor != selectquerydata.parsingdata[ExecuteTable].tableSchema.end(); itor++){
                            // std::cout << "column_name : " << (*itor).column_name << " " << (*itor).type << " " << (*itor).length << " " << (*itor).offset << std::endl;
                            offset.push_back((*itor).offset);
                            offlen.push_back((*itor).length);
                            datatype.push_back((*itor).type);
                            colname.push_back(ExecuteTable + comma + (*itor).column_name);
                        }


                        Value &Blcokinfo = blockdoc["RES"]["Chunk List"];
                        // Value &filter = document["Extra"]["filter"];
                        int count = 0;
                        // cout << Blcokinfo.Size() << endl;
                        for(int i = 0; i < Blcokinfo.Size(); i ++){

                            csdscheduler.threadblocknum.push_back(count);
                            for (int j = 0; j < Blcokinfo[i][sstfilename[i].c_str()].Size(); j++){
                                csdscheduler.blockvec.push_back(count);
                                // cout << count << endl;
                                count++;
                            }
                        }

                        
                        csdscheduler.snippetdata.work_id = work_id;
                        csdscheduler.snippetdata.table_offset = offset;
                        csdscheduler.snippetdata.table_offlen = offlen;
                        csdscheduler.snippetdata.table_filter = selectquerydata.parsingdata[ExecuteTable].tableQuery;
                        csdscheduler.snippetdata.table_datatype = datatype;
                        csdscheduler.snippetdata.sstfilelist = sstfilename;
                        csdscheduler.snippetdata.table_col = colname;
                        csdscheduler.snippetdata.block_info_list = Blcokinfo;

                        csdscheduler.snippetdata.tablename = ExecuteTable[k];


                        for (int i = 0; i < csdscheduler.snippetdata.table_filter.size(); i++)
                        {
                            //여기서 먼저 확인을 진행 후 빼야할 필터 절 빼기
                            if (csdscheduler.snippetdata.table_filter[i].Left == "" || csdscheduler.snippetdata.table_filter[i].Right == "")
                            {
                                continue;
                            }
                            //스트링인지 확인
                            // if (!csdscheduler.snippetdata.table_filter[i]["LV"].IsString() || !csdscheduler.snippetdata.table_filter[i]["RV"].IsString())
                            // {
                            //     continue;
                            // }
                            string LV = csdscheduler.snippetdata.table_filter[i].Left;
                            int filteroper = csdscheduler.snippetdata.table_filter[i].Operator;
                            string cmpoper = "=";
                            char SubLV = LV[0];
                            string RV = csdscheduler.snippetdata.table_filter[i].Right;
                            string tmpv;
                            char SubRV = RV[0];

                            if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), LV) == csdscheduler.snippetdata.table_col.end())
                            {
                                //내 테이블이 아닐때의 조건도 추가를 해야할듯?
                                if (find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), RV) != csdscheduler.snippetdata.table_col.end())
                                {
                                    tmpv = RV;
                                    RV = LV;
                                    LV = tmpv;
                                }
                                else
                                {
                                    continue;
                                }
                                csdscheduler.savedfilter.push_back(make_tuple(LV, cmpoper, RV));
                                csdscheduler.passindex.push_back(i);
                                csdscheduler.passindex.push_back(i + 1);
                            }
                            else if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), LV) != csdscheduler.snippetdata.table_col.end())
                            {
                                // cout << "LV : " << LV << endl;
                                if (find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), RV) == csdscheduler.snippetdata.table_col.end())
                                {
                                    csdscheduler.savedfilter.push_back(make_tuple(LV, cmpoper, RV));
                                    csdscheduler.passindex.push_back(i);
                                    csdscheduler.passindex.push_back(i + 1);
                                }
                            }
                        }
                        bufma.SetWork(full_query,work_id,selectquerydata.tablename[k],csdscheduler.savedfilter,csdscheduler.blockvec);

                        for(int i = 0; i < sstfilename.size(); i++){

                            tg.create_thread(boost::bind(&Scheduler::sched,&csdscheduler,i));
                        }
                        tg.join_all();
                        csdscheduler.savedfilter.clear();
                        csdscheduler.passindex.clear();
                        csdscheduler.threadblocknum.clear();
                        csdscheduler.blockvec.clear();
                        // cout << table_name.GetString() << endl;
                        //after sched
                        send(new_socket,&work_id,sizeof(work_id),0);
                        std::cout << "WorkID : " << work_id << std::endl;

                        break;
                    }
                        break;
                    case CSD_INDEX_SEEK:
                        break;

                    default: {
                        break;
                    }
                }
                close(new_socket);
                
            }
        }
    }
}

int my_LBA2PBA(std::string &req_json,std::string &res_json){
	int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char buffer[BUFF_SIZE] = {0};
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }
   
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(LBA2PBAPORT);
       
    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, "10.0.5.119", &serv_addr.sin_addr)<=0) //csd 정보를 통해 ip 입력(std::string 타입)
    {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }
   
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed %s\n",strerror(errno));
		//sql_print_information("connect error %s", strerror(errno));
        return -1;
    }
	
	//send json
	size_t len = strlen(req_json.c_str());
	send(sock,&len,sizeof(len),0);
	send(sock,req_json.c_str(),strlen(req_json.c_str()),0);

	//read(sock,res_json,BUFF_SIZE);

	size_t length;
	read( sock , &length, sizeof(length));

	int numread;
	while(1) {
		if ((numread = read( sock , buffer, BUFF_SIZE - 1)) == -1) {
			perror("read");
			exit(1);
		}
		length -= numread;
	    buffer[numread] = '\0';
		res_json += buffer;

	    if (length == 0)
			break;
	}

	::close(sock);

	return 0;
}

