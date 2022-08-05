#include "buffer_manager.h"

int BufferManager::InitBufferManager(Scheduler &scheduler, TableManager &tblManager){
    BufferManager_Input_Thread = thread([&](){BufferManager::BlockBufferInput();});
    BufferManager_Thread = thread([&](){BufferManager::BufferRunning(scheduler, tblManager);});
    return 0;
}

int BufferManager::Join(){
    BufferManager_Input_Thread.join();
    BufferManager_Thread.join();
    return 0;
}

int BufferManager::CheckTableStatus(int qid, string tname){
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        return -2;//쿼리 ID 오류
    }else if(m_BufferManager[qid]->table_status.find(tname) == m_BufferManager[qid]->table_status.end()){
        return -1;//테이블이 없음
    }else if(!m_BufferManager[qid]->table_status[tname]){
        return 0;//작업 아직 안끝남
    }else{
        return 1;//작업 완료되어 테이블 데이터 존재
    }
}

int GetColumnValue(TableManager &tblManager, string col_name, string table_name, int &col_offset, int &col_length, char* rowdata){
    vector<TableManager::ColumnSchema> schema;
    int resp = tblManager.get_table_schema(table_name, schema);
    string col_name_;
    bool isvarchar = false;
    int coltype;
    for(int j = 0; j < schema.size(); j++){
        if (schema[j].type == 15){
            //varchar일 경우 내 길이 구하기 + 뒤에값에 영향주기
            col_offset = schema[j].offset + 1;
            char varcharlenbuf[4];
            memset(varcharlenbuf,0,4);
            varcharlenbuf[3] = rowdata[col_offset];
            int varcharlen = *((int*)varcharlenbuf);
            col_length = varcharlen;
            isvarchar = true;
        }else if(isvarchar){
            col_offset = col_offset + col_length;
            col_length = schema[j].length;
        }else{
            col_offset = schema[j].offset;
            col_length = schema[j].length;
        }
        col_name_ = table_name + '.' + schema[j].column_name;
        if(col_name_ == col_name){
            // col_offset = schema[j].offset;
            // col_length = schema[j].length;
            //schema[j].type;
            coltype = schema[j].type;
            return coltype;
        }
    }
}

void BufferManager::BlockBufferInput(){
    int server_fd, client_fd;
	int opt = 1;
	struct sockaddr_in serv_addr, client_addr;
	socklen_t addrlen = sizeof(client_addr);
    static char cMsg[] = "ok";

	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
	
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(PORT_BUF); // port
 
	if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
		perror("bind");
		exit(EXIT_FAILURE);
	} 

	if (listen(server_fd, NCONNECTION) < 0){
		perror("listen");
		exit(EXIT_FAILURE);
	}

	while(1){
		if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&addrlen)) < 0){
			perror("accept");
        	exit(EXIT_FAILURE);
		}

		std::string json = "";//크기?
		// char buffer[BUFF_SIZE] = {0};
        int njson;
		size_t ljson;

		recv( client_fd , &ljson, sizeof(ljson), 0);

        char buffer[ljson] = {0};
		
		while(1) {
			if ((njson = recv(client_fd, buffer, BUFF_SIZE-1, 0)) == -1) {
				perror("read");
				exit(1);
			}
			ljson -= njson;
		    buffer[njson] = '\0';
			json += buffer;

		    if (ljson == 0)
				break;
		}
		
        send(client_fd, cMsg, strlen(cMsg), 0);

		char data[BUFF_SIZE];//크기?
        char* dataiter = data;
		memset(data, 0, BUFF_SIZE);
        int ndata = 0;
        int totallen = 0;
        size_t ldata = 0;
        recv(client_fd , &ldata, sizeof(ldata),0);
        totallen = ldata;

        // cout << "totallen: " << totallen << endl;

		while(1) {
			if ((ndata = recv( client_fd , dataiter, ldata,0)) == -1) {
				perror("read");
				exit(1);
			}
            dataiter = dataiter+ndata;
			ldata -= ndata;

		    if (ldata == 0)
				break;
		}

        send(client_fd, cMsg, strlen(cMsg), 0);

        // cout << "## BufferQueue.push_work(BlockResult(json.c_str(), data)) ##" << endl;
		BlockResultQueue.push_work(BlockResult(json.c_str(), data));		
        
        close(client_fd);		
	}   
	close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler, TableManager &tblManager){
    while (1){
        BlockResult blockResult = BlockResultQueue.wait_and_pop();
        // cout << "result csdname: " << blockResult.csd_name << endl;
        // cout << "result rows: " << blockResult.rows << endl;
        // cout << "result length: " << blockResult.length << endl;

        if(m_BufferManager.find(blockResult.query_id) == m_BufferManager.end()){
            cout << "<error> No Query ID, Init Query first! " << endl;
            continue;
        }else if(m_BufferManager[blockResult.query_id]->work_buffer_list.find(blockResult.work_id) 
                    == m_BufferManager[blockResult.query_id]->work_buffer_list.end()){
            cout << "<error> No Work ID, Init Work first!" << endl;
            continue;            
        }   

        MergeBlock(blockResult, scheduler, tblManager);
    }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler, TableManager &tblManager){
    int qid = result.query_id;
    int wid = result.work_id;

    Work_Buffer* myWorkBuffer = m_BufferManager[qid]->work_buffer_list[wid];

    //작업 종료된 id의 데이터인지 확인
    if(myWorkBuffer->is_done){
        cout << "<error> Work(" << qid << "-" << wid << ") is already done!" << endl;
        return;
    }

    cout << "(before left/result/after left)" << "(" << myWorkBuffer->left_block_count <<"/" << result.result_block_count << "/" << myWorkBuffer->left_block_count-result.result_block_count << ")" << endl;
    scheduler.csdworkdec(result.csd_name, result.result_block_count);
    myWorkBuffer->left_block_count -= result.result_block_count;

    

    //필요한 블록이 다 모였는지 확인
    if((myWorkBuffer->left_block_count == 0)){
        cout << "Work(" << qid << "-" << wid << ") is done!" << endl;
        
        myWorkBuffer->is_done = true;
        m_BufferManager[qid]->work_status[wid] = true;
        m_BufferManager[qid]->table_status[myWorkBuffer->table_alias] = true;  

        if(m_BufferManager[qid]->result_work_id == wid){//쿼리의 작업이 다 끝났음을 의미
            cout << "Query(" << qid << ") is done!" << endl;
            m_BufferManager[qid]->is_done = true;
        }               
    }
}

int BufferManager::InitWork(int qid, int wid, string table_alias,
                            vector<vector<string>> column_projection_,
                            vector<string> group_by_col_,
                            vector<pair<int,string>> order_by_col_,
                            vector<string> table_col_, vector<int> table_offset_,
                            vector<int> table_offlen_, vector<int> table_datatype_,
                            int bcnt, int table_type_, int is_last){
    cout << "#Init Work! [" << qid << "-" << wid << "] (BufferManager)" << endl;
   
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        InitQuery(qid);
    }else if(m_BufferManager[qid]->work_buffer_list.find(wid) 
                != m_BufferManager[qid]->work_buffer_list.end()){
        cout << "<error> Work ID Duplicate Error" << endl;
        return -1;            
    }   

    Work_Buffer* workBuffer = new Work_Buffer(table_alias, bcnt, table_type_, column_projection_,
                        table_col_, table_offset_, table_offlen_, table_datatype_);

    if(group_by_col_.size() != 0){
        workBuffer->groupby_col.assign(group_by_col_.begin(), group_by_col_.end());
    }
    
    m_BufferManager[qid]->work_cnt++;
    m_BufferManager[qid]->work_status[wid] = false;
    m_BufferManager[qid]->table_status[table_alias] = false;

    if(is_last){
        m_BufferManager[qid]->result_work_id = wid;
    }

    return 1;
}

void BufferManager::InitQuery(int qid){
    cout << "#Init Query! [" << qid << "] (BufferManager)" << endl;

    Query_Buffer* queryBuffer = new Query_Buffer(qid);
    m_BufferManager.insert(pair<int,Query_Buffer*>(qid,queryBuffer));
}