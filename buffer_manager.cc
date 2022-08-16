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

		BlockResultQueue.push_work(BlockResult(json.c_str(), data));		
        
        close(client_fd);		
	}   
	close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler, TableManager &tblManager){
    while (1){
        BlockResult blockResult = BlockResultQueue.wait_and_pop();

        if((m_BufferManager.find(blockResult.query_id) == m_BufferManager.end()) || 
           (m_BufferManager[blockResult.query_id]->work_buffer_list.find(blockResult.work_id) 
                    == m_BufferManager[blockResult.query_id]->work_buffer_list.end())){
            cout << "<error> Work(" << blockResult.query_id << "-" << blockResult.work_id << ") Initialize First!" << endl;
        }   

        MergeBlock(blockResult, scheduler, tblManager);
    }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler, TableManager &tblManager){
    int qid = result.query_id;
    int wid = result.work_id;

    Work_Buffer* myWorkBuffer = m_BufferManager[qid]->work_buffer_list[wid];

    unique_lock<mutex> lock(myWorkBuffer->mu);

    //작업 종료된 id의 데이터인지 확인(종료된게 들어오면 오류임)
    if(myWorkBuffer->is_done){
        cout << "<error> Work(" << qid << "-" << wid << ") Is Already Done!" << endl;
        return;
    }

    //데이터가 있는지 확인
    if(result.length != 0){
        int col_type, col_offset, col_len = 0;
        string col_name;
        vector<int> temp_offset;//테스트용
        temp_offset.assign(result.row_offset.begin(), result.row_offset.end());
        temp_offset.push_back(result.length);

        //한 row씩 읽기
        for(int i=0; i<result.rows; i++){
            col_offset = result.row_offset[i];
            int temp_test = col_offset + temp_offset[i+1] - temp_offset[i];//테스트용

            //한 column씩 읽으며 vector에 저장
            for(int j=0; j<myWorkBuffer->table_datatype.size(); j++){
                col_name = myWorkBuffer->table_column[j];
                col_type = myWorkBuffer->table_datatype[j];

                //가변 길이 데이터인 경우 길이 확인
                if(col_type == MySQL_VARSTRING){ 
                    col_len = myWorkBuffer->table_offlen[j];
                    char tempbuf[4]; 
                    memset(tempbuf,0,4);
                    if(col_len < 255){
                        tempbuf[0] = result.data[col_offset];
                        col_len = *((int *)tempbuf);
                        col_offset += 1;
                    }else{
                        tempbuf[0] = result.data[col_offset];
                        tempbuf[1] = result.data[col_offset+1];
                        col_len = *((int *)tempbuf);
                        col_offset += 2;
                    }
                }else{
                    col_len = myWorkBuffer->table_offlen[j];   
                }

                switch (col_type){
                    case MySQL_BYTE:{
                        char tempbuf[col_len];//col_len = 1
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        int8_t my_value = *((int8_t *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_INT16:{
                        char tempbuf[col_len];//col_len = 2
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        int16_t my_value = *((int16_t *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_INT32:{
                        char tempbuf[col_len];//col_len = 3
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        int32_t my_value = *((int32_t *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_INT64:{
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        int64_t my_value = *((int64_t *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_FLOAT32:{
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        float my_value = *((float *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_DOUBLE:{
                        char tempbuf[col_len];//col_len = 8
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        double my_value = *((double *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_NEWDECIMAL:{
                        char tempbuf[col_len];
                        for(int k=0; k<col_len; k++){
                            tempbuf[col_len-k-1] = result.data[col_offset+k];
                        }
                        tempbuf[col_len-1] = 0x00;
                        int my_value = *((int *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_DATE:{
                        char tempbuf[col_len+1];//col_len = 3
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        tempbuf[3] = 0x00;
                        int my_value = *((int *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_TIMESTAMP:{
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        int my_value = *((int *)tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }case MySQL_STRING:
                     case MySQL_VARSTRING:{
                        char tempbuf[col_len];
                        memcpy(tempbuf,result.data+col_offset,col_len);
                        string my_value(tempbuf);
                        myWorkBuffer->table_data[col_name].push_back(my_value);
                        break;
                    }default:{
                        cout << "<error> Type:" << col_type << " Is Not Defined!!" << endl;
                    }
                }
                
                col_offset += col_len;
            }

            if(temp_test != col_offset){//테스트용
                cout << "<error> offset different! " << temp_test << "/" << col_offset << endl;
            }
        }
    }

    //남은 블록 수 조정
    cout << "(before left/result cnt/after left)|(" << myWorkBuffer->left_block_count << "/" << result.result_block_count << "/" << myWorkBuffer->left_block_count-result.result_block_count << ")" << endl;
    scheduler.csdworkdec(result.csd_name, result.result_block_count);
    myWorkBuffer->left_block_count -= result.result_block_count;

    //블록을 다 처리했는지 개수 확인
    if(myWorkBuffer->left_block_count == 0){
        cout << "#Work(" << qid << "-" << wid << ") Is Done!" << endl;
        myWorkBuffer->is_done = true;
        m_BufferManager[qid]->table_status[myWorkBuffer->table_alias].second = true;
        myWorkBuffer->cond.notify_all();      
    }else if(myWorkBuffer->left_block_count < 0){
        cout << "<error> Work(" << qid << "-" << wid << ") Block Count = " << myWorkBuffer->left_block_count << endl;
    }
}

int BufferManager::InitWork(int qid, int wid, string table_alias,
                            vector<string> table_column_, vector<int> table_datatype,
                            vector<int> table_offlen_, int total_block_cnt_){
    cout << "#Init Work! [" << qid << "-" << wid << "] (BufferManager)" << endl;
   
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        InitQuery(qid);
    }else if(m_BufferManager[qid]->work_buffer_list.find(wid) 
                != m_BufferManager[qid]->work_buffer_list.end()){
        cout << "<error> Work ID Duplicate Error" << endl;
        return -1;            
    }   

    Work_Buffer* workBuffer = new Work_Buffer(table_alias, table_column_, table_datatype, 
                                                table_offlen_, total_block_cnt_);
    
    m_BufferManager[qid]->work_cnt++;
    m_BufferManager[qid]->work_buffer_list[wid]= workBuffer;
    m_BufferManager[qid]->table_status[table_alias] = make_pair(wid,false);

    return 1;
}

void BufferManager::InitQuery(int qid){
    cout << "#Init Query! [" << qid << "] (BufferManager)" << endl;

    Query_Buffer* queryBuffer = new Query_Buffer(qid);
    m_BufferManager.insert(pair<int,Query_Buffer*>(qid,queryBuffer));
}

int BufferManager::CheckTableStatus(int qid, string tname){
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        return QueryIDError;//쿼리 ID 오류
    }else if(m_BufferManager[qid]->table_status.find(tname) == m_BufferManager[qid]->table_status.end()){
        return NonInitTable;//Init한 테이블이 없음
    }else if(!m_BufferManager[qid]->table_status[tname].second){
        return NotFinished;//Init 되었지만 작업 아직 안끝남
    }else{
        return WorkDone;//작업 완료되어 테이블 데이터 존재
    }
}

TableInfo BufferManager::GetTableInfo(int qid, string tname){
    int status = CheckTableStatus(qid,tname);
    TableInfo tableInfo;
    if(status!=1){
        return tableInfo;
    }

    int wid = m_BufferManager[qid]->table_status[tname].first;
    Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
    tableInfo.table_column = workBuffer->table_column;
    tableInfo.table_datatype = workBuffer->table_datatype;
    tableInfo.table_offlen = workBuffer->table_offlen;
   
    return tableInfo;
}

TableData BufferManager::GetTableData(int qid, string tname){
    //여기서 데이터 완료될때까지 대기
    int status = CheckTableStatus(qid,tname);
    TableData tableData;

    switch(status){
        case QueryIDError:
        case NonInitTable:{
            tableData.valid = false;
            break;
        }case NotFinished:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            
            unique_lock<mutex> lock(workBuffer->mu);
            workBuffer->cond.wait(lock);

            tableData.table_data = workBuffer->table_data;
            break;
        }case WorkDone:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            unique_lock<mutex> lock(workBuffer->mu);
            tableData.table_data = workBuffer->table_data;
            break;
        }
    }
   
    return tableData;
}

//이건 MQM에서 연산한 결과 테이블 전체 저장할 때
int BufferManager::SaveTableData(int qid, string tname, unordered_map<string,vector<any>> table_data_){
    int status = CheckTableStatus(qid,tname);
    if(status!=WorkDone){
        return 0;
    }
    //wal에서 save하는거 따로 구현해야함!!!
    int wid = m_BufferManager[qid]->table_status[tname].first;
    Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
    unique_lock<mutex> lock(workBuffer->mu);

    m_BufferManager[qid]->work_buffer_list[wid]->table_data = table_data_;
    m_BufferManager[qid]->work_buffer_list[wid]->is_done = true;
    m_BufferManager[qid]->table_status[tname].second = true;

    return 1;
}

int BufferManager::DeleteTableData(int qid, string tname){
    int status = CheckTableStatus(qid,tname);

    switch(status){
        case QueryIDError:
        case NonInitTable:{
            return -1;
        }case NotFinished:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            
            unique_lock<mutex> lock(workBuffer->mu);
            workBuffer->cond.wait(lock);

        }case WorkDone:{
            //꼭 MQM에서 Get Table Data 한 다음에 Delete Table Data 해야함
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            unique_lock<mutex> lock(workBuffer->mu);

            for(auto i : workBuffer->table_data){//테이블 컬럼 데이터 삭제
                i.second.clear();
            }
            return 0;
        }
    }

    return 1;
}