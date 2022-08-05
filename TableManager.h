#ifndef _TABLE_MANAGER_H_
#define _TABLE_MANAGER_H_

#include <vector>
#include <unordered_map>

using namespace std;

class TableManager{	
public:
	struct ColumnSchema {
		std::string column_name;
		int type;
		int length;
		int offset;
	};
	
	struct DataBlockHandle {
		off64_t Offset;
		off64_t Length;
	};
	
	struct SSTFile{
		std::string filename;
		vector<struct DataBlockHandle> BlockList;
	};
	
	struct Table {
		std::string tablename;
		int tablesize;
		vector<struct ColumnSchema> Schema;
		vector<struct SSTFile> SSTList;
		vector<vector<string>> IndexList;
		vector<string> PK;
	};

	int init_TableManager();
	void print_TableManager();
	int generate_req_json(std::string tablename,char *dst);
	int generate_req_json(std::string tablename,std::string &dst);
	int get_table_schema(std::string tablename,vector<struct ColumnSchema> &dst);
	vector<string> get_ordered_table_by_size(vector<string> tablenames);
	int get_IndexList(string tablename, vector<vector<string>> &dst);
	
	
private:
	unordered_map<std::string,struct Table> m_TableManager;
};
#endif