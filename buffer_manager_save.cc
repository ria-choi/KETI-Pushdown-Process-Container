#include "buffer_manager_save.h"

void sumtest::sum1(Block_Buffer bbuf){
    char testbuf[7];
    int brownum = 0;
    for(int i =0; i < bbuf.nrows; i++){
        brownum = bbuf.row_offset[i];
        char *iter = bbuf.data + brownum;
        memcpy(testbuf,iter+25,7);
        float a = decimalch(testbuf);
        memcpy(testbuf,iter+32,7);
        float b = decimalch(testbuf);
        //cout << " a : " << a << " b : " << b << endl;
        data = a * b + data;
        
    }
    // cout << "data is :" << endl;
    // printf("%f",data);mergeResult.merged_block_id_list[i].first
}

float sumtest::decimalch(char b[]){
    char num[4];
    int *tempbuf;
    int ab;
    float cd;
    float ret;
    memset(num,0,4);
    for(int i = 0; i < 4; i++){
        num[i] = b[5-i];
    }
    tempbuf = (int*)num;
    ret = tempbuf[0];
    //cout << ret << endl;
    memset(num,0,4);
    num[0] = b[6];
    tempbuf = (int*)num;
    ab = tempbuf[0];
    //cout << ab << endl;
    cd = (float)ab/100;
    //cout << cd << endl;
    return ret + cd;
}

int GetColumnValue(TableManager &tblManager, string col_name, string table_name, int &col_offset, int &col_length, char* rowdata){

    vector<TableManager::ColumnSchema> schema;
    int resp = tblManager.get_table_schema(table_name, schema);
    string col_name_;
    int col_type;

    int offset = 0;
    for(int j = 0; j < schema.size(); j++){
        if (schema[j].type == 15){
            //varchar일 경우 내 길이 구하기 + 뒤에값에 영향주기
            col_offset = schema[j].offset + 1;
            char varcharlenbuf[4];
            memset(varcharlenbuf,0,4);
            varcharlenbuf[3] = rowdata[col_offset];
            int varcharlen = *((int*)varcharlenbuf);
            offset += schema[j].length - varcharlen;
            col_length = varcharlen;
        }else{
            col_length = schema[j].length;
        }
        
        col_offset = schema[j].offset - offset;
        col_name_ = table_name + '.' + schema[j].column_name;

        if(col_name_ == col_name){
            col_type = schema[j].type;
            return col_type;
        }
    }
}

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

		char data[BUFF_SIZE];
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
		BufferQueue.push_work(BlockResult(json.c_str(), data));		
        
        close(client_fd);		
	}   
	close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler, TableManager &tblManager){
    while (1){
        BlockResult blockResult = BufferQueue.wait_and_pop();

        if(m_BufferManager.find(blockResult.query_id) == m_BufferManager.end()){
            cout << "<error> No Query ID, Result Query ID error! " << endl;
            continue;
        }else if(m_BufferManager[blockResult.query_id]->m_WorkIDManager.find(blockResult.work_id)
                                    == m_BufferManager[blockResult.query_id]->m_WorkIDManager.end()){
            cout << "<error> No Work ID, Result Work ID error! " << endl;
            continue;
        }

        MergeBlock(blockResult, scheduler, tblManager);
    }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler, TableManager &tblManager){
    string q = m_WorkIDManager[result.work_id];
    int w = result.work_id;

    Work_Buffer* myWorkBuffer = m_BufferManager[q]->work_buffer_list[w];

    //작업 종료된 id의 데이터인지 확인
    if(myWorkBuffer->is_done){
        return;
    }

    //병합 완료된 블록 확인
    vector<pair<int,list<int>>>::iterator iter1;
    for (iter1 = result.block_id_list.begin(); iter1 != result.block_id_list.end(); iter1++) {
        int is_full = (*iter1).first;
        if(is_full){
            list<int>::iterator iter2;
            for(iter2 = (*iter1).second.begin(); iter2 != (*iter1).second.end(); iter2++){
                if(*iter2 < result.last_valid_block_id){
                    myWorkBuffer->need_block_list.erase(*iter2);
                }else{
                    cout << "-------*iter2 > result.last_valid_block_id-----------" << endl;
                }
            }
        }
        // cout << endl;
        // cout << "size: " << myWorkBuffer->need_block_list.size() << endl;
    }

    cout << "(total/result/total-result)" << "(" << myWorkBuffer->left_block_count <<"/" << result.result_block_count << "/" << myWorkBuffer->left_block_count-result.result_block_count << ")" << endl;
    
    scheduler.csdworkdec(result.csd_name, result.result_block_count);
    myWorkBuffer->left_block_count -= result.result_block_count;

    //work_type에 따라 작업 수행
    switch (myWorkBuffer->work_type){
        case Buffer_Work_Type::JoinX:
        {    
            // cout << "#work_type : JoinX" << endl;

            // cout << "+++++ block save +++++" << endl;
            myWorkBuffer->merging_block_buffer.nrows = result.rows;
            myWorkBuffer->merging_block_buffer.length = result.length;
            memcpy(myWorkBuffer->merging_block_buffer.data,result.data,result.length);
            myWorkBuffer->merging_block_buffer.row_offset.assign(result.row_offset.begin(), result.row_offset.end()); 
            myWorkBuffer->row_all += result.rows;
            // cout << "row_all: " << myWorkBuffer->row_all << endl;
            myWorkBuffer->PushWork();

            break;
        }

        case Buffer_Work_Type::JoinO_HasMapX_MakeMapO :
        {
            // cout << "#work_type : JoinO_HasMapX_MakeMapO" << endl;

            // cout << "+++++ row make map +++++" << endl;
            //type int
            for(int i=0; i<result.rows; i++){
                int row_offset = result.row_offset[i];
                // make map
                for (int m = 0; m < myWorkBuffer->make_map_col.size(); m++) {
                    string my_col = myWorkBuffer->make_map_col[m];
                    int col_offset, col_length; // 받아올 변수
                    int col_type = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    //cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;

                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);

                    if (col_type == 3 || col_type == 14){ //int(little)
                        int my_value = *((int *)tempbuf);
                        m_BufferManager[q]->int_col_map[my_col].insert(my_value);
                    }else if(col_type == 8){ //bigint(little)
                        int64_t my_value = *((int64_t *)tempbuf);
                        m_BufferManager[q]->int_col_map[my_col].insert(my_value);
                    }else if(col_type == 254 || col_type == 15){ //big(string), big(varstring)
                        string my_value(tempbuf);
                        m_BufferManager[q]->string_col_map[my_col].insert(my_value);
                    }else if(col_type == 246){ //decimal
                        tempbuf[0] = 0x00; //tempbuf[80 00 00 00 00 00 00 01]->tempbuf[00 00 00 00 00 00 00 01]
                        char tempbuf_[col_length];
                        for(int i = 0; i < col_length; i++){
                            tempbuf_[i] = tempbuf[col_length-i-1]; //tempbuf[00 00 00 00 00 00 00 01]->tempbuf_[01 00 00 00 00 00 00 00]
                        }
                        int my_value = *((int *)tempbuf_);
                        m_BufferManager[q]->int_col_map[my_col].insert(my_value);
                    }else{ 
                        cout << "[Buff_M] New Type!!!! - " << col_type << endl;
                    }
        
                }   
            }

            // cout << "+++++ block save +++++" << endl;
            myWorkBuffer->merging_block_buffer.nrows = result.rows;
            myWorkBuffer->merging_block_buffer.length = result.length;
            memcpy(myWorkBuffer->merging_block_buffer.data,result.data,result.length);
            myWorkBuffer->merging_block_buffer.row_offset.assign(result.row_offset.begin(), result.row_offset.end());
                
            //map 출력
            //    for (int m = 0; m < myWorkBuffer->make_map_col.size(); m++) {
            //         string my_col = myWorkBuffer->make_map_col[m];
            //         unordered_map<int,int> map;
            //         map = m_BufferManager[q]->col_map[my_col];
            //         cout<< my_col << " map : [ ";
            //         for(auto const&i:map){
            //             cout << i.first << " "; 
            //         }
            //         cout << " ]" << endl;
            //    }
            myWorkBuffer->PushWork();

            break;
        }

        case Buffer_Work_Type::JoinO_HasMapO_MakeMapX :
        {
            // cout << "#work_type : JoinO_HasMapO_MakeMapX" << endl;

            int row_len = 0;
            vector<int> temp_offset;
            temp_offset.assign(result.row_offset.begin(), result.row_offset.end());
            temp_offset.push_back(result.length);

            // cout << "+++++ row join column +++++" << endl;
            for(int i=0; i<result.rows; i++){
                int row_offset = result.row_offset[i];
                bool passed = true;
                // column join
                for (int j = 0; j < myWorkBuffer->join_col.size(); j++) {
                    string my_col = myWorkBuffer->join_col[j].first;
                    string opp_col = myWorkBuffer->join_col[j].second;
                    int col_offset, col_length; // 받아올 변수
                    int col_type = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    // cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;
                    
                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);

                    if (col_type == 3 || col_type == 14){ //int(little)
                        int my_value = *((int *)tempbuf);
                        if(m_BufferManager[q]->int_col_map[opp_col].find(my_value) == m_BufferManager[q]->int_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else if(col_type == 8){ //bigint(little)
                        int my_value = *((int64_t *)tempbuf);
                        if(m_BufferManager[q]->int_col_map[opp_col].find(my_value) == m_BufferManager[q]->int_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else if(col_type == 254 || col_type == 15){ //big(string), big(varstring)
                        string my_value(tempbuf);
                        if(m_BufferManager[q]->string_col_map[opp_col].find(my_value) == m_BufferManager[q]->string_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else if(col_type == 246){ //decimal
                        tempbuf[0] = 0x00; //tempbuf[80 00 00 00 00 00 00 01]->tempbuf[00 00 00 00 00 00 00 01]
                        char tempbuf_[col_length];
                        for(int i = 0; i < col_length; i++){
                            tempbuf_[i] = tempbuf[col_length-i-1]; //tempbuf[00 00 00 00 00 00 00 01]->tempbuf_[01 00 00 00 00 00 00 00]
                        }
                        int my_value = *((int *)tempbuf_);
                        if(m_BufferManager[q]->int_col_map[opp_col].find(my_value) == m_BufferManager[q]->int_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else{ 
                        cout << "[Buff_M] New Type!!!! - " << col_type << endl;
                    }
                }   

                if(passed){
                    // cout << "+++++ row save +++++" << endl;
                    row_len = temp_offset[i+1] - temp_offset[i];
                    if(myWorkBuffer->merging_block_buffer.length + row_len > BUFF_SIZE){ //row추가시 데이터 크기 넘으면
                        myWorkBuffer->PushWork();
                    }
                    myWorkBuffer->merging_block_buffer.row_offset.push_back(myWorkBuffer->merging_block_buffer.length);
                    myWorkBuffer->merging_block_buffer.nrows += 1;
                    int data_offset = myWorkBuffer->merging_block_buffer.length;
                    memcpy(myWorkBuffer->merging_block_buffer.data + data_offset, result.data + temp_offset[i], row_len);
                    myWorkBuffer->merging_block_buffer.length += row_len;
                }

            }

            break;    
        }

        case Buffer_Work_Type::JoinO_HasMapO_MakeMapO :
        {
            // cout << "#work_type : JoinO_HasMapO_MakeMapO" << endl;

            int row_len = 0;
            vector<int> temp_offset;
            temp_offset.assign(result.row_offset.begin(), result.row_offset.end());
            temp_offset.push_back(result.length);

            // cout << "+++++ row make map / row join column +++++" << endl;
            for(int i=0; i<result.rows; i++){
                int row_offset = result.row_offset[i];
                bool passed = true;

                // column join
                for (int j = 0; j < myWorkBuffer->join_col.size(); j++) {
                    string my_col = myWorkBuffer->join_col[j].first;
                    string opp_col = myWorkBuffer->join_col[j].second;
                    int col_offset, col_length; // 받아올 변수
                    int col_type = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    // cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;
                    
                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);

                    if (col_type == 3 || col_type == 14){ //int(little)
                        int my_value = *((int *)tempbuf);
                        if(m_BufferManager[q]->int_col_map[opp_col].find(my_value) == m_BufferManager[q]->int_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else if(col_type == 8){ //bigint(little)
                        int my_value = *((int64_t *)tempbuf);
                        if(m_BufferManager[q]->int_col_map[opp_col].find(my_value) == m_BufferManager[q]->int_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else if(col_type == 254 || col_type == 15){ //big(string), big(varstring)
                        string my_value(tempbuf);
                        if(m_BufferManager[q]->string_col_map[opp_col].find(my_value) == m_BufferManager[q]->string_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else if(col_type == 246){ //decimal
                        tempbuf[0] = 0x00; //tempbuf[80 00 00 00 00 00 00 01]->tempbuf[00 00 00 00 00 00 00 01]
                        char tempbuf_[col_length];
                        for(int i = 0; i < col_length; i++){
                            tempbuf_[i] = tempbuf[col_length-i-1]; //tempbuf[00 00 00 00 00 00 00 01]->tempbuf_[01 00 00 00 00 00 00 00]
                        }
                        int my_value = *((int *)tempbuf_);
                        if(m_BufferManager[q]->int_col_map[opp_col].find(my_value) == m_BufferManager[q]->int_col_map[opp_col].end()){
                            passed = false;
                            break;
                        }
                    }else{ 
                        cout << "[Buff_M] New Type!!!! - " << col_type << endl;
                    }
                }   

                if(!passed){
                    continue;
                }
                
                // cout << "+++++ save row +++++" << endl;
                row_len = temp_offset[i+1] - temp_offset[i];
                if(myWorkBuffer->merging_block_buffer.length + row_len > BUFF_SIZE){ //row추가시 데이터 크기 넘으면
                    myWorkBuffer->PushWork();
                }
                myWorkBuffer->merging_block_buffer.row_offset.push_back(myWorkBuffer->merging_block_buffer.length);
                myWorkBuffer->merging_block_buffer.nrows += 1; 
                int data_offset = myWorkBuffer->merging_block_buffer.length;
                memcpy(myWorkBuffer->merging_block_buffer.data + data_offset, result.data + temp_offset[i], row_len);
                myWorkBuffer->merging_block_buffer.length += row_len;
                

                // make map
                for (int m = 0; m < myWorkBuffer->make_map_col.size(); m++) {
                    string my_col = myWorkBuffer->make_map_col[m];
                    int col_offset, col_length; // 받아올 변수
                    int col_type = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    //cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;

                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);

                    if (col_type == 3 || col_type == 14){ //int(little)
                        int my_value = *((int *)tempbuf);
                        m_BufferManager[q]->int_col_map[my_col].insert(my_value);
                    }else if(col_type == 8){ //bigint(little)
                        int64_t my_value = *((int64_t *)tempbuf);
                        m_BufferManager[q]->int64_col_map[my_col].insert(my_value);
                    }else if(col_type == 254 || col_type == 15){ //big(string), big(varstring)
                        string my_value(tempbuf);
                        m_BufferManager[q]->string_col_map[my_col].insert(my_value);
                    }else if(col_type == 246){ //decimal
                        tempbuf[0] = 0x00; //tempbuf[80 00 00 00 00 00 00 01]->tempbuf[00 00 00 00 00 00 00 01]
                        char tempbuf_[col_length];
                        for(int i = 0; i < col_length; i++){
                            tempbuf_[i] = tempbuf[col_length-i-1]; //tempbuf[00 00 00 00 00 00 00 01]->tempbuf_[01 00 00 00 00 00 00 00]
                        }
                        int64_t my_value = *((int64_t *)tempbuf_);
                        m_BufferManager[q]->int64_col_map[my_col].insert(my_value);
                    }else{ 
                        cout << "[Buff_M] New Type!!!! - " << col_type << endl;
                    }
        
                }   

            }

            break;
        }

    }

    // cout << "size: " << myWorkBuffer->need_block_list.size() << endl;

    //필요한 블록이 다 모였는지 확인
    if((myWorkBuffer->need_block_list.size() == 0) || (myWorkBuffer->block_cnt == 0)){
        cout << "FINISHED " << (myWorkBuffer->need_block_list.size() == 0) << "/" << (myWorkBuffer->block_cnt == 0) << endl;
        
        myWorkBuffer->merging_block_buffer.last_merging_buffer = true;
        myWorkBuffer->is_done = true;  
        
        if(myWorkBuffer->work_type == 2 || myWorkBuffer->work_type == 3){
            myWorkBuffer->PushWork();
        }
            
        cout << "Work [" << myWorkBuffer->work_id << "] Done!!" << endl;               
    }

}

void Work_Buffer::PushWork(){
    // if(work_type == 1 || work_type == 3){
    //     make_map_queue.push_work(merging_block_buffer);
    // }
    result_block_queue.push_work(merging_block_buffer);
    merging_block_buffer.InitBlockBuffer();
}

int Select_Buffer::MakeResult(TableManager &tblManager){
    //work(table)간 연산(dependency 없는 table간)
    //select 결과 구성 -> 컬럼 재구성,연산
    if(work_buffer_map.size() == 1){
        Work_Buffer* wbuffer = work_buffer_map.begin()->second;
        for(int i=0; i<wbuffer->result_block_queue.get_size(); i++){
            result_block_queue.push_work(wbuffer->result_block_queue.wait_and_pop());
        }
    }else{

    }
}

int Query_Buffer::MakeResult(TableManager &tblManager){//다 끝난 select 결과로 최종 query return result 생성 함수
    //select간 union, intersect, plus, minus, union all 수행
    if(select_list.size() == 1){
        int sid = select_list[0];
        Select_Buffer* sbuffer = select_buffer_map[sid];
        for(int i=0; i<sbuffer->result_block_queue.get_size(); i++){
            return_block_queue.push_work(sbuffer->result_block_queue.wait_and_pop());
        }
    }else{

    }
}

int BufferManager::GetData(Block_Buffer &dest){//최종 query buffer의 결과를 리턴하는 함수
    if(m_BufferManager.find(dest.query_id) == m_BufferManager.end()){
        cout << "<error> No Query ID, Get Query Data error! " << endl;
        return -1;
    }else if(m_BufferManager[dest.query_id]->is_done &&
        m_BufferManager[dest.query_id]->return_block_queue.is_empty()){
        cout << "Query ID ["<< dest.query_id << "] is done! (no data to return)" << endl;
        return -2;
    }
    else{
        Block_Buffer BBuf = m_BufferManager[dest.query_id]->return_block_queue.wait_and_pop();
        dest.length = BBuf.length;
        dest.nrows = BBuf.nrows;
        memcpy(dest.data, BBuf.data, BBuf.length);
        dest.row_offset.assign(BBuf.row_offset.begin(),BBuf.row_offset.end());

        // cout << "send to handler : " << endl;
        // for (int i =0; i < BBuf.length; i++){
        //     printf("%02X ",(u_char)BBuf.data[i]);
        // }
        // cout << "-------------------------------------------------------"<< endl;

    }
    return 1;
}

// void Work_Buffer::WorkBufferMakeMap(unordered_map<string, unordered_map<int,int>> col_map_, TableManager tblManager){

//     while(1){
//         Block_Buffer blockBuffer = make_map_queue.wait_and_pop();
                
//         int row_len = 0;
//         vector<int> temp_offset;
//         temp_offset.assign( blockBuffer.row_offset.begin(), blockBuffer.row_offset.end() );
//         temp_offset.push_back(blockBuffer.length);

//         for(int i=0; i<blockBuffer.nrows; i++){
//             for (int m = 0; m < make_map_col.size(); m++) {
//                 string my_col = make_map_col[m];
//                 int col_offset, col_length; // 받아올 변수
//                 GetColumnValue(tblManager, my_col, table_name, col_offset, col_length);
//                 int my_value = *((int*)blockBuffer.data[i]+col_offset);
//                 cout << "my_value : " << my_value << endl;
//                 col_map_[my_col].insert({my_value,1});
//             }
//         } 

//         if(blockBuffer.last_merging_buffer){
//             return;
//         }
//     }       
// }


int BufferManager::SetWork(int qid, int sid, int wid, vector<int> block_list, 
                string table_name_, vector<tuple<string,string,string>> join_){//wid의 block list
    cout << "#Set Work! [" << qid << "-" << sid << "-" << wid << "]" << endl;
   
    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        cout << "<error> No Query ID, Init Query first! " << endl;
        return -1;
    }else if(m_BufferManager[qid]->select_buffer_map.find(sid)==m_BufferManager[qid]->select_buffer_map.end()){
        cout << "<error> No Select ID, Init Select ID error " << endl;
        return -1;
    }else if(m_BufferManager[qid]->select_buffer_map[qid]->work_buffer_map.find(wid)
                ==m_BufferManager[qid]->select_buffer_map[qid]->work_buffer_map.end()){
        cout << "<error> No Work ID, Set Work ID error " << endl;
        return -1;            
    }   

    Work_Buffer* myWorkBuffer = m_BufferManager[qid]->select_buffer_map[qid]->work_buffer_map[wid];

    vector<string> make_map_col;
    vector<pair<string,string>> join_col;
    int work_type = Buffer_Work_Type::JoinX;

    //CHECK WORK_TYPE
    if(join_.size() != 0){
        vector<tuple<string,string,string>>::iterator join_iter;
        for (join_iter = join_.begin(); join_iter != join_.end(); join_iter++){
            string my_col = get<0>(*join_iter);
            string oper = get<1>(*join_iter);
            string opp_col = get<2>(*join_iter);

            cout << "{ " << my_col<< " " << oper << " " << opp_col  << "}" << endl;

            if(m_BufferManager[qid]->select_buffer_map[sid]->int_col_map.find(opp_col) 
                    == m_BufferManager[qid]->select_buffer_map[sid]->int_col_map.end()){ //Map X
                cout << "#Map X : " << opp_col << endl;
                if(work_type == Buffer_Work_Type::JoinO_HasMapO_MakeMapX 
                    || work_type == Buffer_Work_Type::JoinO_HasMapO_MakeMapO){
                    work_type = Buffer_Work_Type::JoinO_HasMapO_MakeMapO;
                }else{
                    work_type = Buffer_Work_Type::JoinO_HasMapX_MakeMapO;
                }
                make_map_col.push_back(my_col);
            }else{ //Map O
                cout << "#Map O : " << opp_col << endl;
                if(work_type == Buffer_Work_Type::JoinX 
                    || work_type == Buffer_Work_Type::JoinO_HasMapO_MakeMapX){
                    work_type = Buffer_Work_Type::JoinO_HasMapO_MakeMapX;
                }else{
                    work_type = Buffer_Work_Type::JoinO_HasMapO_MakeMapO;
                }            
                join_col.push_back({my_col,opp_col});
            }
        }
    }

    myWorkBuffer->InitWork(wid,table_name_,make_map_col,join_col,block_list,work_type);

    // //MAKE_MAP THREAD  
    // if(myWorkBuffer->work_type == 1 || myWorkBuffer->work_type == 3){
    //     thread Work_Buffer_Get_Col_Thread = thread([&](){myWorkBuffer->WorkBufferMakeMap(m_BufferManager[query_]->col_map,tblManager);});
    //     Work_Buffer_Get_Col_Thread.join();
    // }

    cout << "#my work type : " << work_type << endl;//check
    
    return 1;
}

int BufferManager::InitSelect(int qid, int sid, vector<int> work_list, vector<pair<int,vector<string>>> select_clause_info){//sid의 wid list
    cout << "#Init Select! [" << qid << "-" << sid << "]" << endl;

    if(m_BufferManager.find(qid) == m_BufferManager.end()){
        cout << "<error> No Query ID, Init Query first! " << endl;
        return -1;
    }else if(m_BufferManager[qid]->select_buffer_map.find(sid)==m_BufferManager[qid]->select_buffer_map.end()){
        cout << "<error> No Select ID, Init Select ID error " << endl;
        return -1;
    }

    vector<int>::iterator w;
    for (w = work_list.begin(); w != work_list.end(); ++w){
        m_BufferManager[qid]->m_WorkIDManager.insert(pair<int,int>(*w,sid));
        Work_Buffer* myWorkBuffer = new Work_Buffer(qid,*w);
        m_BufferManager[qid]->select_buffer_map[sid]->work_buffer_map.insert(pair<int,Work_Buffer*>(*w,myWorkBuffer));
    }

    m_BufferManager[qid]->select_buffer_map[sid]->work_cnt = work_list.size();

}

int BufferManager::InitQuery(int qid, vector<int> select_list){//qid의 sid list
    cout << "#Init Query! [" << qid << "]" << endl;

    if(!(m_BufferManager.find(qid)==m_BufferManager.end())){
        cout << "Query ID Duplicate Error" << endl;
        
        return -1;
    }
    else{
        Query_Buffer* queryBuffer = new Query_Buffer(qid,select_list);
        bool flag = true;

        vector<int>::iterator s;
        for (s = select_list.begin(); s != select_list.end(); ++s){
            if(flag){
                Select_Buffer* selectBuffer = new Select_Buffer(*s);
                queryBuffer->select_buffer_map.insert(pair<int,Select_Buffer*>(*s,selectBuffer));
                queryBuffer->select_cnt++;
                flag = !flag;
            }else{
                flag = !flag;
            }
        }

        m_BufferManager.insert(pair<int,Query_Buffer*>(qid,queryBuffer));

        return 1;
    }
}

