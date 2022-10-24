#include "StorageEngineInputInterface.h"
#include "WalManager.h"
#include "keti_util.h"

// Include C++ Header
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
// Include gRPC Header
#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using snippetsample::SnippetSample;
using snippetsample::Snippet;
using snippetsample::SnippetRequest;
using snippetsample::Result;
using snippetsample::Request;
using namespace std::chrono;

// Logic and data behind the server's behavior.
class SnippetSampleServiceImpl final : public SnippetSample::Service {
  Status SetSnippet(ServerContext* context,
                   ServerReaderWriter<Result, SnippetRequest>* stream) override {
    SnippetRequest snippetrequest;
    // Snippet snippet;
    queue<SnippetStruct> snippetqueue;
    st = time(0);
    keti_log("Storage Engine Instance","\t------------:: STEP 2 ::------------");
    while (stream->Read(&snippetrequest)) {      
      // gRPC -> custom struct
      {
        SnippetStruct snippetdata;

        snippetdata.snippetType = snippetrequest.type();
        auto snippet = snippetrequest.snippet();
        
        AppendTableFilter(snippetdata,snippet);
        
        if(snippetdata.snippetType == snippetsample::SnippetRequest::DEPENDENCY_OPER_SNIPPET){
          if(snippet.has_dependency()) {
            auto dependency = snippet.dependency();
            AppendDependencyProjection(snippetdata,dependency);
            AppendDependencyFilter(snippetdata,dependency);
          }
        }
        
        for(int i = 0; i < snippet.table_name_size() ; i++){
          snippetdata.tablename.push_back(snippet.table_name(i));
        }
        
        for(int i = 0; i < snippet.column_alias_size() ; i++){
          snippetdata.column_alias.push_back(snippet.column_alias(i));
        }
        
        for(int i = 0; i < snippet.column_filtering_size() ; i++){
          snippetdata.columnFiltering.push_back(snippet.column_filtering(i));
        }
        
        if(snippet.has_order_by()){
          auto order_by = snippet.order_by();
          for(int j = 0; j < order_by.ascending_size() ; j++){
            snippetdata.orderBy.push_back(order_by.column_name(j));
            snippetdata.orderType.push_back(order_by.ascending(j));
          }
        }
        
        for(int i = 0; i < snippet.group_by_size() ; i++){
          snippetdata.groupBy.push_back(snippet.group_by(i));
        }
        
        for(int i = 0; i < snippet.table_col_size() ; i++){
          snippetdata.table_col.push_back(snippet.table_col(i));
        }
        
        for(int i = 0; i < snippet.table_offset_size() ; i++){
          snippetdata.table_offset.push_back(snippet.table_offset(i));
        }
        
        for(int i = 0; i < snippet.table_offlen_size() ; i++){
          snippetdata.table_offlen.push_back(snippet.table_offlen(i));
        }
        
        for(int i = 0; i < snippet.table_datatype_size() ; i++){
          snippetdata.table_datatype.push_back(snippet.table_datatype(i));
        }
        
        snippetdata.tableAlias = snippet.table_alias();
        snippetdata.query_id = snippet.query_id();
        snippetdata.work_id = snippet.work_id();
        AppendProjection(snippetdata, snippet);
        snippetqueue.push(snippetdata);
        
        string wal_json;
      }
      string wal_json;
      
      // if(snippetrequest.type() == 0) {
      //   WalManager test(snippetrequest.snippet());
      //   test.run(wal_json);
      //   snippetdata.wal_json = wal_json;
      // }
    }
    LQNAME = snippetqueue.back().tableAlias;
    Queryid = snippetqueue.back().query_id;
    snippetmanager.NewQuery(snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
    
    unordered_map<string,vector<vectortype>> RetTable = bufma.GetTableData(Queryid,LQNAME).table_data;
    
    for(auto i = RetTable.begin(); i != RetTable.end(); i++){
      pair<string,vector<vectortype>> tmppair = *i;
    }
    return Status::OK;
  }
  Status Run(ServerContext* context, const Request* request, Result* result) override {        
    query_result = "\n";
    
    {      
      unordered_map<string,vector<vectortype>> RetTable = bufma.GetTableData(Queryid,LQNAME).table_data;
      query_result +=  "+--------------------------------+\n ";
      for(auto i = RetTable.begin(); i != RetTable.end(); i++){
        pair<string,vector<vectortype>> tmppair = *i;
        query_result += tmppair.first + "  ";
      }
      query_result += "\n";
      query_result +=  "+--------------------------------+\n";
      
      auto tmppair = *RetTable.begin();
        int ColumnCount = tmppair.second.size();
        for(int i = 0; i < ColumnCount; i++){
          string tmpstring = " ";
          for(auto j = RetTable.begin(); j != RetTable.end(); j++){
            pair<string,vector<vectortype>> tmppair = *j;
            if(tmppair.second[i].type == 1){
              if(tmppair.second[i].intvec == 103){
                tmpstring += to_string(tmppair.second[i].intvec) + "           ";
              }else{
                tmpstring += to_string(tmppair.second[i].intvec) + "            ";
              }
            }else if(tmppair.second[i].type == 2){
              tmpstring +=  to_string(tmppair.second[i].floatvec) + "    ";
            }else{
                tmpstring += tmppair.second[i].strvec + "            ";
            }
          }
          query_result += tmpstring + "\n";
          
        }
        query_result +=  "+--------------------------------+";
      fi = time(0);
    }

    result->set_value(query_result);

    query_result = "";
    bufma.EndQuery(Queryid);
    return Status::OK;
  }
  private:
    std::unordered_map<int, std::vector<std::string>> map;
    std::string query_result = "";
    time_t fi, st;
    string LQNAME;
    int Queryid;
};


void RunServer() {
  tblManager.init_TableManager();
  std::string server_address("0.0.0.0:50051");
  SnippetSampleServiceImpl service;

  ServerBuilder builder;

  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char const *argv[]) {
  if(argc > 1){
    testrun(argv[1]);
  }else{
    RunServer();
  }

  return 0;
}

void testrun(string dirname){
  // cout << "[Storage Engine Input Interface] Start gRPC Server" << endl;
  tblManager.init_TableManager();
  int64_t timestamp_st = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  time_t st = time(0);
  queue<SnippetStruct> snippetqueue;
  // cout << dirname << endl;
  string tmpstring;
  tmpstring = split(dirname,'h')[1];
  if(tmpstring.size() == 1){
    tmpstring = "0" + tmpstring;
  }
  // SnippetStructQueue tmpque;

  keti_log("DB Connector Instance","\t------------:: STEP 1 ::------------");
  keti_log("Plan Executer","Creating Snippet ...");
  keti_log("Plan Executer","Snippet Group WorkID : 0 ... done");
  keti_log("Plan Executer","Snippet Group WorkID : 1 ... done");
  keti_log("Plan Executer","Merge Query WorkID : 2 ... done");
  keti_log("Plan Executer","Merge Query WorkID : 3 ... done");


  keti_log("Storage Engine Interface","Send Snippet Group (WorkID : 0) to Storage Engine Instance");
  keti_log("Storage Engine Interface","Send Snippet Group (WorkID : 1) to Storage Engine Instance");
  keti_log("Storage Engine Interface","Send Merge Query (WorkID : 2) to Storage Engine Instance");
  keti_log("Storage Engine Interface","Send Merge Query (WorkID : 3) to Storage Engine Instance");

  // std::string line;
  // std::getline(std::cin, line);
  keti_log("Storage Engine Instance","\t------------:: STEP 2 ::------------");
  // cout << "-----------------------------------:: STEP 2 ::-----------------------------------" << endl;
  // for(int i = 0; i < 13; i++){
    // cout << dirname << endl;
    int filecount = 0;
  for(const auto & file : filesystem::directory_iterator(dirname)){
    filecount++;
  }
  for(int i = 0; i < filecount; i++){
    // cout << "[Storage Engine Input Interface] Snippet " << i << " Recived" << endl;
    // string filename = "newtestsnippet/tpch05-" + to_string(i) + ".json";
    string filename = dirname + "/tpch" + tmpstring + "-" +to_string(i) + ".json";
    // cout << filename << endl;
    // keti_log("Storage Engine Input Interface","Recived Snippet" + tmpstring + "-" + to_string(i));
    //cout << "[Storage Engine Input Interface] Recived Snippet" << tmpstring << "-" << i << endl;
    int json_fd;
	  string json = "";
    json_fd = open(filename.c_str(),O_RDONLY);
    int res;
    char buf;

    
    while(1){
      res = read(json_fd,&buf,1);
      if(res == 0){
        break;
      }
      json += buf;
    }
    close(json_fd);
    Document doc;
    // cout << 1 << endl;
    // cout << json << endl;
	  doc.Parse(json.c_str());
    // cout << 2 << endl;
    // bool isjoin = false;
    // if(doc["type"] == 2){
    //   isjoin = true;
    // }
    Value& document = doc["snippet"];
    // SnippetStruct snippetdata(document["tableFilter"], document["blockList"]);
    // SnippetStruct snippetdata(document["blockList"]);
    // cout << 2 << endl;
    SnippetStruct snippetdata;
    // cout << snippetdata.table_filter.Size() << endl;
    snippetdata.snippetType = doc["type"].GetInt();
    // cout << 2 << endl;
    // for(int i = 0; i < document["tableFilter"].Size(); i++){}
    AppendTableFilter(snippetdata,document["tableFilter"]);
    // cout << 2 << endl;

    if(snippetdata.snippetType == 6){
      if(document.HasMember("dependency")){
        if(document["dependency"].HasMember("dependencyProjection")){
          AppendDependencyProjection(snippetdata,document["dependency"]["dependencyProjection"]);
        }
        if(document["dependency"].HasMember("dependencyFilter")){
          AppendDependencyFilter(snippetdata,document["dependency"]["dependencyFilter"]);
        }
      }
    }
    // cout << 2 << endl;
    // snippetdata.table_filter = doc["tableFilter"];

    for(int i = 0; i < document["tableName"].Size(); i++){
		  snippetdata.tablename.push_back(document["tableName"][i].GetString());
	  }
    // cout << 2 << endl;
	  for(int i = 0; i < document["columnAlias"].Size(); i++){
		  snippetdata.column_alias.push_back(document["columnAlias"][i].GetString());
	  }
    // cout << 2 << endl;
	  for(int i = 0; i < document["columnFiltering"].Size(); i++){
		  snippetdata.columnFiltering.push_back(document["columnFiltering"][i].GetString());
	  }
    // cout << 2 << endl;
    // cout << document.HasMember("orderBy") << endl;
    // cout << document["orderBy"].GetType() << endl;
    // for(int i = 0; i < document["orderBy"].Size(); i++){
    if(document.HasMember("orderBy")){
      for(int j = 0; j < document["orderBy"]["columnName"].Size(); j++){
          snippetdata.orderBy.push_back(document["orderBy"]["columnName"][j].GetString());
          snippetdata.orderType.push_back(document["orderBy"]["ascending"][j].GetInt());
        }
    }
    // }
    // cout << 2 << endl;
	  for(int i = 0; i < document["groupBy"].Size(); i++){
		  snippetdata.groupBy.push_back(document["groupBy"][i].GetString());
	  }
    for(int i = 0; i < document["tableCol"].Size(); i++){
		  snippetdata.table_col.push_back(document["tableCol"][i].GetString());
	  }
    for(int i = 0; i < document["tableOffset"].Size(); i++){
		  snippetdata.table_offset.push_back(document["tableOffset"][i].GetInt());
	  }
    for(int i = 0; i < document["tableOfflen"].Size(); i++){
		  snippetdata.table_offlen.push_back(document["tableOfflen"][i].GetInt());
	  }
    for(int i = 0; i < document["tableDatatype"].Size(); i++){
		  snippetdata.table_datatype.push_back(document["tableDatatype"][i].GetInt());
	  }
    // cout << 1 << endl;
    snippetdata.tableAlias = document["tableAlias"].GetString();
    // cout << document.HasMember("queryID") << endl;
    snippetdata.query_id = document["queryID"].GetInt();
    // cout << 4 << endl;
    snippetdata.work_id = document["workID"].GetInt();
    // cout << 5 << endl;
    AppendProjection(snippetdata, document["columnProjection"]);
    // cout << 6 << endl;
    // cout << 2 << endl;
    snippetqueue.push(snippetdata);
    // cout << snippetqueue.front().table_filter.Size() << endl;
    // tmpque.enqueue(snippetdata);
  }
  string LQNAME = snippetqueue.back().tableAlias;
  int Queryid = snippetqueue.back().query_id;
  snippetmanager.NewQuery(snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
  // snippetmanager.NewQuery(tmpque,bufma,tblManager,csdscheduler,csdmanager);
  // thread t1 = thread(&SnippetManager::NewQuery,&snippetmanager,snippetqueue,bufma,tblManager,csdscheduler,csdmanager);
  // t1.detach();
  // cout << 1 << endl;
  // unordered_map<string,vector<vectortype>> rrr = bufma.GetTableData(4,"snippet4-12").table_data;
  // cout << 2 << endl;
  unordered_map<string,vector<vectortype>> RetTable = bufma.GetTableData(Queryid,LQNAME).table_data;
  // unordered_map<string,vector<vectortype>> RetTable = snippetmanager.ReturnResult(4);
  // std::string line;
  // std::getline(std::cin, line);
  keti_log("DB Connector Instance","\n\t------------:: STEP 4 ::------------");

  std::vector<int> len;
  cout << "+--------------------------------------+" << endl;
  cout << " ";
  for(auto i = RetTable.begin(); i != RetTable.end(); i++){
    pair<string,vector<vectortype>> tmppair = *i;
    cout << tmppair.first;
    len.push_back(tmppair.first.length());
    cout << " \t";
  }
  cout << endl;
  cout << "+--------------------------------------+" << endl;
    
    auto tmppair = *RetTable.begin();
      int ColumnCount = tmppair.second.size();
      for(int i = 0; i < ColumnCount; i++){
        string tmpstring = " ";
        int col_idx = 0;
        for(auto j = RetTable.begin(); j != RetTable.end(); j++){
          pair<string,vector<vectortype>> tmppair = *j;
          // cout << tmppair.second[j] << " ";
          if(tmppair.second[i].type == 1){
            if(tmppair.second[i].intvec == 103){
              tmpstring += to_string(tmppair.second[i].intvec);
            }else{
              tmpstring += to_string(tmppair.second[i].intvec);
            }
            // std::string space(len[col_idx] - to_string(tmppair.second[i].intvec).length(),' ');
            // tmpstring += space;
            tmpstring += " \t";
            // cout << tmppair.second[i].intvec << endl;
          }else if(tmppair.second[i].type == 2){
            // std::stringstream sstream;
            // sstream << tmppair.second[i].floatvec;
            tmpstring +=  to_string(tmppair.second[i].floatvec);
            // std::string space(len[col_idx] - to_string(tmppair.second[i].floatvec).length(),' ');
            // tmpstring += space;
            tmpstring += " \t";
            // cout << tmppair.second[i].floatvec << endl;
          }else{
            // if(tmppair.second[i].strvec == "103"){
            //   tmpstring += tmppair.second[i].strvec + "          ";
            // }else{
              tmpstring += tmppair.second[i].strvec;
            // std::string space(len[col_idx] - tmppair.second[i].strvec.length(),' ');
            // tmpstring += space;
            tmpstring += " \t";
            // }
            // cout << tmppair.second[i].strvec << endl;
          }
        }
        // cout << tmpstring << endl;
        cout << tmpstring << endl;;
        col_idx++;
      }
      cout << "+--------------------------------------+" << endl;;
      // cout << "+-------------------------+" <<endl;
    time_t fi = time(0);
    int64_t timestamp_fi = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  cout << "Total Time : " << fi-st << "s"<< endl;
  cout << timestamp_fi-timestamp_st << endl;
  // sleep(100);
}

// fill SnippetStruct TableFilter with json
void AppendTableFilter(SnippetStruct &snippetdata,Value &filterdata){
  for(int i = 0; i < filterdata.Size(); i++){
    filterstruct tmpfilterst;
    if(filterdata[i].HasMember("LV")){
      for(int j = 0; j < filterdata[i]["LV"]["type"].Size(); j++){
        lv tmplv;
        tmplv.type.push_back(filterdata[i]["LV"]["type"][j].GetInt());
        tmplv.value.push_back(filterdata[i]["LV"]["value"][j].GetString());
        tmpfilterst.LV = tmplv;
      }
    }
    tmpfilterst.filteroper = filterdata[i]["Operator"].GetInt();
    if(filterdata[i].HasMember("RV")){
      rv tmprv;
      for(int j = 0; j < filterdata[i]["RV"]["type"].Size(); j++){
        
        tmprv.type.push_back(filterdata[i]["RV"]["type"][j].GetInt());
        tmprv.value.push_back(filterdata[i]["RV"]["value"][j].GetString());
        
      }
      tmpfilterst.RV = tmprv;
    }
    snippetdata.table_filter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct TableFilter with gRPC message
void AppendTableFilter(SnippetStruct &snippetdata,snippetsample::Snippet &snippet){
  for(int i = 0; i < snippet.table_filter_size(); i++){
    auto filter = snippet.table_filter(i);
    filterstruct tmpfilterst;
    if(filter.has_lv()){
      lv tmplv;
      auto lv = filter.lv();
      for(int j = 0; j < lv.type_size(); j++){
        tmplv.type.push_back(lv.type(j));
        tmplv.value.push_back(lv.value(j));
      }
      tmpfilterst.LV = tmplv;
    }
    tmpfilterst.filteroper = filter.operator_();
    if(filter.has_rv()){
      rv tmprv;
      auto rv = filter.rv();
      for(int j = 0; j < rv.type_size(); j++){
        tmprv.type.push_back(rv.type(j));
        tmprv.value.push_back(rv.value(j));
      }
      tmpfilterst.RV = tmprv;
    }
    snippetdata.table_filter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct DependencyFilter with json
void AppendDependencyFilter(SnippetStruct &snippetdata,Value &filterdata){
  for(int i = 0; i < filterdata.Size(); i++){
    filterstruct tmpfilterst;
    if(filterdata[i].HasMember("LV")){
      for(int j = 0; j < filterdata[i]["LV"]["type"].Size(); j++){
        lv tmplv;
        tmplv.type.push_back(filterdata[i]["LV"]["type"][j].GetInt());
        tmplv.value.push_back(filterdata[i]["LV"]["value"][j].GetString());
        tmpfilterst.LV = tmplv;
      }
    }
    tmpfilterst.filteroper = filterdata[i]["Operator"].GetInt();
    if(filterdata[i].HasMember("RV")){
      for(int j = 0; j < filterdata[i]["RV"]["type"].Size(); j++){
        rv tmprv;
        tmprv.type.push_back(filterdata[i]["RV"]["type"][j].GetInt());
        tmprv.value.push_back(filterdata[i]["RV"]["value"][j].GetString());
        tmpfilterst.RV = tmprv;
      }
    }
    snippetdata.dependencyFilter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct DependencyFilter with gRPC message
void AppendDependencyFilter(SnippetStruct &snippetdata,snippetsample::Snippet_Dependency &dependency){  
  for(int i = 0; i < dependency.dependency_filter_size(); i++){
    auto filter = dependency.dependency_filter(i);
    filterstruct tmpfilterst;
    if(filter.has_lv()){
      lv tmplv;
      auto lv = filter.lv();
      for(int j = 0; j < lv.type_size(); j++){
        tmplv.type.push_back(lv.type(j));
        tmplv.value.push_back(lv.value(j));
      }
      tmpfilterst.LV = tmplv;
    }
    tmpfilterst.filteroper = filter.operator_();
    if(filter.has_rv()){
      rv tmprv;
      auto rv = filter.rv();
      for(int j = 0; j < rv.type_size(); j++){
        tmprv.type.push_back(rv.type(j));
        tmprv.value.push_back(rv.value(j));
      }
      tmpfilterst.RV = tmprv;
    }
    snippetdata.dependencyFilter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct Projection with json
void AppendProjection(SnippetStruct &snippetdata,Value &Projectiondata){
	for(int i = 0; i < Projectiondata.Size(); i++){
		vector<Projection> tmpVec;
    if(Projectiondata[i]["selectType"] == KETI_COUNTALL){
      Projection tmpPro;
      tmpPro.value = "4";
      tmpVec.push_back(tmpPro);
    }else{
      for(int j = 0; j < Projectiondata[i]["value"].Size(); j++){
        if(j == 0){
          Projection tmpPro;
          tmpPro.value = to_string(Projectiondata[i]["selectType"].GetInt());
          tmpVec.push_back(tmpPro);
        }
        
        Projection tmpPro;
        tmpPro.type = Projectiondata[i]["valueType"][j].GetInt();
        tmpPro.value = Projectiondata[i]["value"][j].GetString();
        tmpVec.push_back(tmpPro);
      }
    }
		snippetdata.columnProjection.push_back(tmpVec);
	}
}

// fill SnippetStruct Projection with gRPC message
void AppendProjection(SnippetStruct &snippetdata,snippetsample::Snippet &snippet){
	for(int i = 0; i < snippet.column_projection_size() ; i++){
    auto projection = snippet.column_projection(i);
		vector<Projection> tmpVec;
    if(projection.select_type() == snippetsample::Snippet_Projection::COUNTSTAR){
      Projection tmpPro;
      tmpPro.value = to_string(projection.select_type());;
      tmpVec.push_back(tmpPro);      
    } else {
      for(int j = 0; j < projection.value_type_size(); j++){        
        if(j==0){
          Projection tmpPro;
          tmpPro.value = to_string(projection.select_type());
          tmpVec.push_back(tmpPro);
        }
        Projection tmpPro;
        tmpPro.type = projection.value_type(j);
        tmpPro.value = projection.value(j);
        tmpVec.push_back(tmpPro);
      }
    }
		snippetdata.columnProjection.push_back(tmpVec);
	}
}

// fill SnippetStruct DependencyProjection with json
void AppendDependencyProjection(SnippetStruct &snippetdata,Value &Projectiondata){
	for(int i = 0; i < Projectiondata.Size(); i++){
		vector<Projection> tmpVec;
    if(Projectiondata[i]["selectType"] == KETI_COUNTALL){
      Projection tmpPro;
      tmpPro.value = "4";
      tmpVec.push_back(tmpPro);
    }else{
      for(int j = 0; j < Projectiondata[i]["value"].Size(); j++){
        if(j == 0){
          Projection tmpPro;
          tmpPro.value = to_string(Projectiondata[i]["selectType"].GetInt());
          tmpVec.push_back(tmpPro);
        }
        
        Projection tmpPro;
        tmpPro.type = Projectiondata[i]["valueType"][j].GetInt();
        tmpPro.value = Projectiondata[i]["value"][j].GetString();
        tmpVec.push_back(tmpPro);
      }
    }
		snippetdata.dependencyProjection.push_back(tmpVec);
	}
}

// fill SnippetStruct DependencyProjection with gRPC message
void AppendDependencyProjection(SnippetStruct &snippetdata,snippetsample::Snippet_Dependency &dependency){
	for(int i = 0; i < dependency.dependency_projection_size() ; i++){
    auto projection = dependency.dependency_projection(i);
    // cout << i << endl;
		vector<Projection> tmpVec;
    if(projection.select_type() == snippetsample::Snippet_Projection::COUNTSTAR){
      Projection tmpPro;
      tmpPro.value = to_string(projection.select_type());;
      tmpVec.push_back(tmpPro);      
    } else {
      for(int j = 0; j < projection.value_type_size(); j++){        
        if(j==0){
          Projection tmpPro;
          tmpPro.value = to_string(projection.select_type());
          tmpVec.push_back(tmpPro);
        }
        Projection tmpPro;
        tmpPro.type = projection.value_type(j);
        tmpPro.value = projection.value(j);
        tmpVec.push_back(tmpPro);
      }
    }
		snippetdata.columnProjection.push_back(tmpVec);
	}
}

// void testsetdata(){
//   vector<any> table1;
//   vector<any> table2;
//   for(int i = 0; i < 100; i++){
//     table1.push_back(i);
//     table2.push_back(i);
//   }

//   vector<any> table3;
//   vector<any> table4;
//   for(int i = 0; i < 100; i++){
//     table3.push_back(to_string(i) + "a");
//     table4.push_back(to_string(5 * i) + "a");
//   }


//   unordered_map<string,vector<any>> testtable;
//   testtable.insert(make_pair("testcolumn",table1));
//   testtable.insert(make_pair("testcolumn111",table3));
//   unordered_map<string,vector<any>> testtable2;
//   testtable2.insert(make_pair("testcolumn2",table2));
//   testtable2.insert(make_pair("testcolumn222",table4));

//   vector<string> tname;
//   vector<int> tlen;
//   bufma.InitWork(0,0,"testtable",tname,tlen,tlen,0);
  
//   // sleep(1);
//   bufma.SaveTableData(0,"testtable",testtable);
//   bufma.InitWork(0,1,"testtable2",tname,tlen,tlen,0);
//   bufma.SaveTableData(0,"testtable2",testtable2);
//   bufma.InitWork(0,2,"testtable3",tname,tlen,tlen,0);
// }

// void testjoin(){
//   StringBuffer snippetbuf;
//   snippetbuf.Clear();
//   Writer<StringBuffer> writer(snippetbuf);
//   writer.StartObject();
//   writer.Key("Snippet");
//   writer.StartArray();
//   writer.StartObject();
//   writer.Key("LV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn");
//   writer.EndArray();
//   writer.EndObject();
//   writer.Key("RV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn2");
//   writer.EndArray();
//   writer.EndObject();
//   writer.EndObject();

//   writer.StartObject();
//   writer.Key("LV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn111");
//   writer.EndArray();
//   writer.EndObject();
//   writer.Key("RV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn222");
//   writer.EndArray();
//   writer.EndObject();
//   writer.EndObject();

//   writer.EndArray();
//   writer.EndObject();
//   // a.table_filter = writer.;
//   string tmpsni = snippetbuf.GetString();

//   Document doc;
//   doc.Parse(tmpsni.c_str());
//   // a.table_filter = doc["Snippet"];
//   testsetdata();
//     cout << tmpsni << endl;
//   SnippetStruct a(doc["Snippet"],doc["Snippet"]);
//   vector<string> tablename;
//   tablename.push_back("testtable");
//   tablename.push_back("testtable2");
//   a.tablename = tablename;
//   a.query_id = 0;
//   a.tableAlias = "testtable3";

//   // auto ccc = bufma.GetTableData(0,"testtable");
//   // for(int i = 0; i < ccc.table_data.ss)
//   // cout << ccc.table_data.size() << endl;
//  cout << bufma.CheckTableStatus(0,"testtable") << endl;
//  cout << bufma.CheckTableStatus(0,"testtable2") << endl;


//   JoinTable(a,bufma);


//   unordered_map<string,vector<any>> savedTable;
//   savedTable = bufma.GetTableData(0,"testtable3").table_data;

//     for(auto i = savedTable.begin(); i != savedTable.end(); i++){
//       pair<string,vector<any>> tmppair = *i;
//       cout << tmppair.first << " ";
//     }
//     cout << endl;
//     auto tmppair = *savedTable.begin();
//     int ColumnCount = tmppair.second.size();
//     for(int i = 0; i < ColumnCount; i++){
//       for(auto j = savedTable.begin(); j != savedTable.end(); j++){
//         pair<string,vector<any>> tmppair = *j;
//         // cout << tmppair.second[j] << " ";
//         if(tmppair.second[i].type() == typeid(int&)){
//           cout << any_cast<int>(tmppair.second[i]) << " ";
//         }else if(tmppair.second[i].type() == typeid(float&)){
//           cout << any_cast<float>(tmppair.second[i]) << " ";
//         }else{
//           cout << any_cast<string>(tmppair.second[i]) << " ";
//         }
//       }
//       cout << endl;
//     }
// }


// void testaggregation(){
//   testsetdata();

//   StringBuffer snippetbuf;
//   snippetbuf.Clear();
//   Writer<StringBuffer> writer(snippetbuf);
//   writer.StartObject();
//   writer.Key("Snippet");
//   writer.StartArray();
//   writer.StartObject();
//   writer.Key("LV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn");
//   writer.EndArray();
//   writer.EndObject();
//   writer.Key("RV");
//   writer.StartObject();
//   writer.Key("value");
//   writer.StartArray();
//   writer.String("testcolumn2");
//   writer.EndArray();
//   writer.EndObject();
//   writer.EndObject();

//   writer.EndArray();
//   writer.EndObject();
//   // a.table_filter = writer.;
//   string tmpsni = snippetbuf.GetString();

//   Document doc;
//   doc.Parse(tmpsni.c_str());

//   SnippetStruct a(doc["Snippet"],doc["Snippet"]);
//   vector<string> tmpvec;
//   tmpvec.push_back("testtable");
//   a.tablename = tmpvec;
//   vector<Projection> tmpvep;
//   Projection tmppro;
//   tmppro.value = "1";
//   tmpvep.push_back(tmppro);
//   Projection tmppro1;
//   tmppro1.value = "testcolumn";
//   tmppro1.type = 3;
//   tmpvep.push_back(tmppro1);
//   vector<vector<Projection>> tmptmp;
//   tmptmp.push_back(tmpvep);
//   a.columnProjection = tmptmp;
//   a.tableAlias = "testtable3";
//   a.query_id = 0;
//   vector<string> tmpcolal;
//   tmpcolal.push_back("test0");
//   a.column_alias = tmpcolal;



//   Aggregation(a,bufma,1);

//   unordered_map<string,vector<any>> savedTable;
//   savedTable = bufma.GetTableData(0,"testtable3").table_data;

//     for(auto i = savedTable.begin(); i != savedTable.end(); i++){
//       pair<string,vector<any>> tmppair = *i;
//       cout << tmppair.first << " ";
//     }
//     cout << endl;
//     auto tmppair = *savedTable.begin();
//     int ColumnCount = tmppair.second.size();
//     for(int i = 0; i < ColumnCount; i++){
//       for(auto j = savedTable.begin(); j != savedTable.end(); j++){
//         pair<string,vector<any>> tmppair = *j;
//         // cout << tmppair.second[j] << " ";
//         cout << tmppair.second[i].type().name() << endl;
//         if(tmppair.second[i].type() == typeid(int&)){
//           cout << any_cast<int>(tmppair.second[i]) << " ";
//         }else if(tmppair.second[i].type() == typeid(float&)){
//           cout << any_cast<float>(tmppair.second[i]) << " ";
//         }else{
//           cout << any_cast<string>(tmppair.second[i]) << " ";
//         }
//       }
//       cout << endl;
//     }
// }