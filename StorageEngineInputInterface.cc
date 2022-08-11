#include "StorageEngineInputInterface.h"

int main(int argc, char const *argv[])
{
	//init table manager	
	tblManager.init_TableManager();
	//init WorkID
	WorkID=0;
	
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
	RunServer();
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
		char buffer[4096] = {0};

		if ((new_socket = accept(server_fd, (struct sockaddr *)&address, 
			(socklen_t*)&addrlen))<0)
		{
			perror("accept");
			exit(EXIT_FAILURE);
		}
		
		read( new_socket , buffer, 4096);
		std::cout << buffer << std::endl;
				
		//parse json	
		Document document;
		document.Parse(buffer);
        int type = 0;
		
		// Value &type = document["type"]; //이게 없어짐
		// string full_query;
		// if(document.HasMember("full_query")){
		// 	full_query = document["full_query"].GetString();
		// 	if(fullquerymap.find(full_query) == fullquerymap.end()){
		// 		bufma.InitQuery(full_query);
		// 		fullquerymap.insert(make_pair(full_query,1));
		// 	}
		// }
		boost::thread_group tg;

        //스니펫 받아서 타입 구분 하나 필요
        /*
        확인해야할것
        1. 버퍼매니저에 해당 테이블의 정보가 있는지
        2.1 -> 없다면, 2.2 -> 있다면
        2.1.1 인덱스 테이블이 있는지
        2.1.2 없다면 예측을 통해 csd로 내려가는지
        2.1.3 있다면 커버링 인덱스인지
        2.1.4 커버링 인덱스가 아니라면 range가 얼마나 줄어드는지
        2.2.1 있다면 무조건 위에서 처리
        */
       int type = GetSnippetType(document);

		// key = document["key"].GetInt();
		switch(type){
			case KETI_WORK_TYPE::SE_FULL_SCAN :{

			}
			case KETI_WORK_TYPE::SE_COVERING_INDEX :{

			}
			case KETI_WORK_TYPE::CSD_FULL_SCAN : {
				std::cout << "Do SCAN_N_FILTER Pushdown" << std::endl;

				int work_id = WorkID.load();
				WorkID++;
				
				std::string req_json;
				std::string res_json;

				Value &table_name = document["table_name"];
				//gen req_json
				
				tblManager.generate_req_json(table_name.GetString(),req_json);
				// std::cout << req_json << std::endl;
				// cout << table_name.GetString() << endl;
				
				//do LBA2PBA
				my_LBA2PBA(req_json,res_json);
				// std::cout << res_json << std::endl;
				Document reqdoc;
				reqdoc.Parse(req_json.c_str());
				vector<string> sstfilename;


				for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i ++){
					sstfilename.push_back(reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
				}
				

				Document blockdoc;
				blockdoc.Parse(res_json.c_str());

				
				//get table schema
				vector<TableManager::ColumnSchema> schema;
				tblManager.get_table_schema(table_name.GetString(),schema);
				vector<int> offset;
				vector<int> offlen;
				vector<int> datatype;
				vector<string> colname;
                vector<string> column_filtering;
                vector<string> Group_By;
                vector<string> Order_By;
                vector<string> Expr;
                vector<string> column_projection;
				string comma = ".";
				// tblManager.print_TableManager();
				vector<TableManager::ColumnSchema>::iterator itor = schema.begin();
				for(; itor != schema.end(); itor++){
					// std::cout << "column_name : " << (*itor).column_name << " " << (*itor).type << " " << (*itor).length << " " << (*itor).offset << std::endl;
					offset.push_back((*itor).offset);
					offlen.push_back((*itor).length);
					datatype.push_back((*itor).type);
					colname.push_back(table_name.GetString() + comma + (*itor).column_name);
					// cout << table_name.GetString() << endl;
				}

				Value &Blcokinfo = blockdoc["RES"]["Chunk List"];
				Value &filter = document["Snippet"]["table_filter"];
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
                for(int i = 0; i < document["Snippet"]["column_filtering"].Size(); i++){
                    column_filtering.push_back(document["Snippet"]["column_filtering"][i].GetString());
                }
                for(int i = 0; i < document["Snippet"]["expr"].Size(); i++){
                    Expr.push_back(document["Snippet"]["expr"][i].GetString());
                }
                for(int i = 0; i < document["Snippet"]["Group_by"].Size(); i++){
                    Group_By.push_back(document["Snippet"]["Group_by"][i].GetString());
                }
                for(int i = 0; i < document["Snippet"]["Order_by"].Size(); i++){
                    Order_By.push_back(document["Snippet"]["Order_by"][i].GetString());
                }
                for(int i = 0; i < document["Snippet"]["column_projection"].Size(); i++){
                    Order_By.push_back(document["Snippet"]["column_projection"][i].GetString());
                }
					// bufma.SetWork(work_id,csdscheduler.blockvec);
				
				csdscheduler.snippetdata.work_id = work_id;
				csdscheduler.snippetdata.table_offset = offset;
				csdscheduler.snippetdata.table_offlen = offlen;
				csdscheduler.snippetdata.table_filter = filter;
				csdscheduler.snippetdata.table_datatype = datatype;
				csdscheduler.snippetdata.sstfilelist = sstfilename;
				csdscheduler.snippetdata.table_col = colname;
				csdscheduler.snippetdata.block_info_list = Blcokinfo;
				csdscheduler.snippetdata.tablename = table_name.GetString();
                csdscheduler.snippetdata.Order_By = Order_By;
                csdscheduler.snippetdata.Group_By = Group_By;
                csdscheduler.snippetdata.Expr = Expr;
                csdscheduler.snippetdata.column_filtering = column_filtering;
                csdscheduler.snippetdata.column_projection = column_projection;

				for(int i = 0; i < sstfilename.size(); i++){
					tg.create_thread(boost::bind(&Scheduler::sched,&csdscheduler,i));
				}
				
				tg.join_all();
			
				csdscheduler.threadblocknum.clear();
				csdscheduler.blockvec.clear();
				// cout << table_name.GetString() << endl;
				//after sched
				send(new_socket,&work_id,sizeof(work_id),0);
				std::cout << "WorkID : " << work_id << std::endl;

				break;
			}
            case KETI_WORK_TYPE::CSD_INDEX_SEEK : {

            }
            case KETI_WORK_TYPE::SE_MERGE_BUFFER_MANAGER : {

            }
			default: {
				break;
			}
		}
		close(new_socket);
		
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



void RunServer() {
  std::string server_address("0.0.0.0:50051");
  SnippetSampleServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case, it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}