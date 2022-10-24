#include "buffer_manager.h"
#include "keti_util.h"

void getColOffset(const char* row_data, int* col_offset_list, vector<int> return_datatype, vector<int> table_offlen);

int BufferManager::InitBufferManager(Scheduler &scheduler){
    BufferManager_Input_Thread = thread([&](){BufferManager::BlockBufferInput();});
    BufferManager_Thread = thread([&](){BufferManager::BufferRunning(scheduler);});
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
	serv_addr.sin_port = htons(PORT_BUF); 
 
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

		std::string json = "";\
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

		char data[BUFF_SIZE];
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

void BufferManager::BufferRunning(Scheduler &scheduler){
    while (1){
        BlockResult blockResult = BlockResultQueue.wait_and_pop();

        keti_log("Buffer Manager", "Receive Data from CSD Return Interface {ID : " + std::to_string(blockResult.work_id) + "}");

        if((m_BufferManager.find(blockResult.query_id) == m_BufferManager.end()) || 
           (m_BufferManager[blockResult.query_id]->work_buffer_list.find(blockResult.work_id) 
                    == m_BufferManager[blockResult.query_id]->work_buffer_list.end())){
            cout << "<error> Work(" << blockResult.query_id << "-" << blockResult.work_id << ") Initialize First!" << endl;
        }   

        MergeBlock(blockResult, scheduler);
    }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler){
    int qid = result.query_id;
    int wid = result.work_id;

    Work_Buffer* myWorkBuffer = m_BufferManager[qid]->work_buffer_list[wid];

    unique_lock<mutex> lock(myWorkBuffer->mu);

    if(myWorkBuffer->is_done){
        cout << "<error> Work(" << qid << "-" << wid << ") Is Already Done!" << endl;
        return;
    }
    
    if(result.length != 0){
        int col_type, col_offset, col_len, origin_row_len, new_row_len, col_count = 0;
        string col_name;
        vector<int> new_row_offset;
        new_row_offset.assign( result.row_offset.begin(), result.row_offset.end() );
        new_row_offset.push_back(result.length);

        for(int i=0; i<result.row_count; i++){
            origin_row_len = new_row_offset[i+1] - new_row_offset[i];
            char row_data[origin_row_len];
            memcpy(row_data,result.data+result.row_offset[i],origin_row_len);

            new_row_len = 0;
            col_count = myWorkBuffer->table_column.size();
            int col_offset_list[col_count + 1];
            
            getColOffset(row_data, col_offset_list, myWorkBuffer->return_datatype, myWorkBuffer->table_offlen);
            col_offset_list[col_count] = origin_row_len;

            for(int j=0; j<myWorkBuffer->table_column.size(); j++){
                col_name = myWorkBuffer->table_column[j];
                col_offset = col_offset_list[j];
                col_len = col_offset_list[j+1] - col_offset_list[j];
                col_type = myWorkBuffer->return_datatype[j];

                vectortype tmpvect;
                switch (col_type){
                    case MySQL_BYTE:{
                        char tempbuf[col_len];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int8_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        
                        break;
                    }case MySQL_INT16:{
                        char tempbuf[col_len];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int16_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);                     
                        break;
                    }case MySQL_INT32:{
                        char tempbuf[col_len];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int32_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_INT64:{
                        char tempbuf[col_len];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int64_t my_value = *((int64_t *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_FLOAT32:{
                        //아직 처리X
                        char tempbuf[col_len];//col_len = 4
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        double my_value = *((float *)tempbuf);
                        tmpvect.type = 2;
                        tmpvect.floatvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_DOUBLE:{
                        //아직 처리X
                        char tempbuf[col_len];//col_len = 8
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        double my_value = *((double *)tempbuf);
                        tmpvect.type = 2;
                        tmpvect.floatvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_NEWDECIMAL:{
                        //decimal(15,2)만 고려한 상황 -> col_len = 7 or 8 (integer:6/real:1 or 2)
                        char tempbuf[col_len];//col_len = 7 or 8
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        bool is_negative = false;
                        if(std::bitset<8>(tempbuf[0])[7] == 0){//음수일때 not +1
                            is_negative = true;
                            for(int i = 0; i<7; i++){
                                tempbuf[i] = ~tempbuf[i];//not연산
                            }
                            // tempbuf[6] = tempbuf[6] +1;//+1
                        }   

                        char integer[8];
                        memset(&integer, 0, 8);
                        for(int k=0; k<5; k++){
                            integer[k] = tempbuf[5-k];
                        }
                        int64_t ivalue = *((int64_t *)integer); 
                        double rvalue;
                        if(col_len == 7){
                            char real[1];
                            real[0] = tempbuf[6];
                            rvalue = *((int8_t *)real); 
                            rvalue *= 0.01;
                        }else if(col_len == 8){
                            char real[2];
                            real[0] = tempbuf[7];
                            real[1] = tempbuf[6];
                            rvalue = *((int16_t *)real); 
                            rvalue *= 0.0001;
                        }else{
                            cout << "Mysql_newdecimal>else" << endl;
                        }

                        double my_value = ivalue + rvalue;
                        if(is_negative){
                            my_value *= -1;
                        }
                        tmpvect.type = 2;
                        tmpvect.floatvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        
                        break;
                    }case MySQL_DATE:{
                        char tempbuf[col_len+1];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        tempbuf[3] = 0x00;
                        int64_t my_value = *((int *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_TIMESTAMP:{
                        //아직 처리X
                        char tempbuf[col_len];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        int my_value = *((int *)tempbuf);
                        tmpvect.type = 1;
                        tmpvect.intvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }case MySQL_STRING:
                     case MySQL_VARSTRING:{
                        char tempbuf[col_len+1];
                        memcpy(tempbuf,row_data+col_offset,col_len);
                        tempbuf[col_len] = '\0';
                        string my_value(tempbuf);
                        tmpvect.type = 0;
                        tmpvect.strvec = my_value;
                        myWorkBuffer->table_data[col_name].push_back(tmpvect);
                        break;
                    }default:{
                        cout << "<error> Type:" << col_type << " Is Not Defined!!" << endl;
                    }
                }
            }
        }
    }
    scheduler.csdworkdec(result.csd_name, result.result_block_count);
    myWorkBuffer->left_block_count -= result.result_block_count;
    
    keti_log("Buffer Manager","Merging Data ... (Left Block : " + std::to_string(myWorkBuffer->left_block_count) + ")");

    if(myWorkBuffer->left_block_count == 0){
        string tmpstring = "Merging Data {ID : " +  std::to_string(wid) + "} Done (Table : " + myWorkBuffer->table_alias + ")";
        keti_log("Buffer Manager",tmpstring);
        
        myWorkBuffer->is_done = true;
        m_BufferManager[qid]->table_status[myWorkBuffer->table_alias].second = true;
        myWorkBuffer->cond.notify_all();
    }
}

int BufferManager::InitWork(int qid, int wid, string table_alias,
                            vector<string> table_column_, vector<int> return_datatype,
                            vector<int> table_offlen_, int total_block_cnt_){
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        InitQuery(qid);
    }else if(m_BufferManager[qid]->work_buffer_list.find(wid) 
                != m_BufferManager[qid]->work_buffer_list.end()){
        cout << "<error> Work ID Duplicate Error" << endl;
        return -1;            
    }   

    Work_Buffer* workBuffer = new Work_Buffer(table_alias, table_column_, return_datatype, 
                                                table_offlen_, total_block_cnt_);

    m_BufferManager[qid]->work_cnt++;
    m_BufferManager[qid]->work_buffer_list[wid]= workBuffer;
    m_BufferManager[qid]->table_status[table_alias] = make_pair(wid,false);

    return 1;
}


int BufferManager::EndQuery(int qid){
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        cout << "<error> There's No Query ID {" << qid << "}" << endl;
        return 0;
    }
    m_BufferManager.erase(qid);
    return 1;
}

void BufferManager::InitQuery(int qid){
    Query_Buffer* queryBuffer = new Query_Buffer(qid);
    m_BufferManager.insert(pair<int,Query_Buffer*>(qid,queryBuffer));
}

int BufferManager::CheckTableStatus(int qid, string tname){
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        return QueryIDError;
    }else if(m_BufferManager[qid]->table_status.find(tname) == m_BufferManager[qid]->table_status.end()){
        return NonInitTable;
    }else if(!m_BufferManager[qid]->table_status[tname].second){
        return NotFinished;
    }else{
        return WorkDone;
    }
}

TableInfo BufferManager::GetTableInfo(int qid, string tname){
    int status = CheckTableStatus(qid,tname);

    TableInfo tableInfo;

    if(status!=WorkDone){
        int wid = m_BufferManager[qid]->table_status[tname].first;
        Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
        unique_lock<mutex> lock(workBuffer->mu);
        workBuffer->cond.wait(lock);
    }

    int wid = m_BufferManager[qid]->table_status[tname].first;
    tableInfo.table_column = m_BufferManager[qid]->work_buffer_list[wid]->table_column;
    tableInfo.table_datatype = m_BufferManager[qid]->work_buffer_list[wid]->table_datatype;
    tableInfo.table_offlen = m_BufferManager[qid]->work_buffer_list[wid]->table_offlen;
    
    return tableInfo;
}

TableData BufferManager::GetTableData(int qid, string tname){
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
            // cout << "here55" << endl;
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


int BufferManager::SaveTableData(int qid, string tname, unordered_map<string,vector<vectortype>> table_data_){
    keti_log("Buffer Manager","Saved Table, Table Name : " + tname);
    cout << "123123 : " << table_data_.size() << endl;
    for(auto it = table_data_.begin(); it != table_data_.end(); it++){
        pair<string,vector<vectortype>> tmpp = *it;
        // for(int i = 0; i < tmpp.second.size())
        cout << tmpp.first << " " << tmpp.second.size() << endl;
    }
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

            for(auto i : workBuffer->table_data){
                i.second.clear();
            }
        }case WorkDone:{
            int wid = m_BufferManager[qid]->table_status[tname].first;
            Work_Buffer* workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
            unique_lock<mutex> lock(workBuffer->mu);

            for(auto i : workBuffer->table_data){
                i.second.clear();
            }
            return 0;
        }
    }

    return 1;
}

void getColOffset(const char* row_data, int* col_offset_list, vector<int> return_datatype, vector<int> table_offlen){
    int col_type = 0, col_len = 0, col_offset = 0, new_col_offset = 0, tune = 0;
    int col_count = return_datatype.size();

    for(int i=0; i<col_count; i++){
        col_type = return_datatype[i];
        col_len = table_offlen[i];
        new_col_offset = col_offset + tune;
        col_offset += col_len;
        if(col_type == MySQL_VARSTRING){
            if(col_len < 256){//0~255
                char var_len[1];
                var_len[0] = row_data[new_col_offset];
                uint8_t var_len_ = *((uint8_t *)var_len);
                tune += var_len_ + 1 - col_len;
            }else{//0~65535
                char var_len[2];
                var_len[0] = row_data[new_col_offset];
                int new_col_offset_ = new_col_offset + 1;
                var_len[1] = row_data[new_col_offset_];
                uint16_t var_len_ = *((uint16_t *)var_len);
                tune += var_len_ + 2 - col_len;
            }
        }

        col_offset_list[i] = new_col_offset;
    }
}