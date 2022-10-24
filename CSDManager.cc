#include "CSDManager.h"


void CSDManager::CSDManagerInit(){
    CSDInfo initinfo;
    initinfo.CSDIP = "10.0.5.120+10.1.1.2";
    initinfo.CSDReplica = "4";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000078.sst");
    initinfo.SSTList.push_back("000200.sst");
    initinfo.SSTList.push_back("000336.sst");
    initinfo.SSTList.push_back("000460.sst");
    initinfo.SSTList.push_back("000584.sst");
    initinfo.SSTList.push_back("000737.sst");
    initinfo.SSTList.push_back("000738.sst");
    initinfo.SSTList.push_back("000739.sst");
    // initinfo.SSTList.push_back("001533.sst");
    // initinfo.SSTList.push_back("1.sst");
    // initinfo.SSTList.push_back("9.sst");
    // initinfo.SSTList.push_back("17.sst");
    initinfo.CSDList.push_back("9");
    initinfo.CSDList.push_back("17");
    CSD_Map_.insert(make_pair("1",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.2.2";
    initinfo.CSDReplica = "5";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000080.sst");
    initinfo.SSTList.push_back("000216.sst");
    initinfo.SSTList.push_back("000352.sst");
    initinfo.SSTList.push_back("000476.sst");
    initinfo.SSTList.push_back("000600.sst");
    initinfo.SSTList.push_back("000755.sst");
    // initinfo.SSTList.push_back("001548.sst");
    // initinfo.SSTList.push_back("2.sst");
    // initinfo.SSTList.push_back("10.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("10");
    initinfo.CSDList.push_back("18");
    CSD_Map_.insert(make_pair("2",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.3.2";
    initinfo.CSDReplica = "5";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000095.sst");
    initinfo.SSTList.push_back("000231.sst");
    initinfo.SSTList.push_back("000367.sst");
    initinfo.SSTList.push_back("000491.sst");
    initinfo.SSTList.push_back("000615.sst");
    initinfo.SSTList.push_back("000770.sst");
    // initinfo.SSTList.push_back("001576.sst");
    // initinfo.SSTList.push_back("3.sst");
    // initinfo.SSTList.push_back("11.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("11");
    initinfo.CSDList.push_back("19");
    CSD_Map_.insert(make_pair("3",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.4.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000123.sst");
    initinfo.SSTList.push_back("000247.sst");
    initinfo.SSTList.push_back("000383.sst");
    initinfo.SSTList.push_back("000507.sst");
    initinfo.SSTList.push_back("000631.sst");
    initinfo.SSTList.push_back("000786.sst");
    // initinfo.SSTList.push_back("001591.sst");
    // initinfo.SSTList.push_back("4.sst");
    // initinfo.SSTList.push_back("12.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("12");
    initinfo.CSDList.push_back("20");
    CSD_Map_.insert(make_pair("4",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.5.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000138.sst");
    initinfo.SSTList.push_back("000262.sst");
    initinfo.SSTList.push_back("000398.sst");
    initinfo.SSTList.push_back("000522.sst");
    initinfo.SSTList.push_back("000646.sst");
    initinfo.SSTList.push_back("000801.sst");
    // initinfo.SSTList.push_back("001607.sst");
    // initinfo.SSTList.push_back("5.sst");
    // initinfo.SSTList.push_back("13.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("13");
    initinfo.CSDList.push_back("21");
    CSD_Map_.insert(make_pair("5",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.6.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000154.sst");
    initinfo.SSTList.push_back("000278.sst");
    initinfo.SSTList.push_back("000414.sst");
    initinfo.SSTList.push_back("000538.sst");
    initinfo.SSTList.push_back("000662.sst");
    initinfo.SSTList.push_back("000817.sst");
    // initinfo.SSTList.push_back("001622.sst");
    // initinfo.SSTList.push_back("6.sst");
    // initinfo.SSTList.push_back("14.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("14");
    initinfo.CSDList.push_back("22");
    CSD_Map_.insert(make_pair("6",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.7.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000169.sst");
    initinfo.SSTList.push_back("000293.sst");
    initinfo.SSTList.push_back("000429.sst");
    initinfo.SSTList.push_back("000553.sst");
    initinfo.SSTList.push_back("000677.sst");
    initinfo.SSTList.push_back("000832.sst");
    // initinfo.SSTList.push_back("001638.sst");
    // initinfo.SSTList.push_back("7.sst");
    // initinfo.SSTList.push_back("15.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("15");
    initinfo.CSDList.push_back("23");
    CSD_Map_.insert(make_pair("7",initinfo));
    initinfo.SSTList.clear();
    initinfo.CSDIP = "10.0.5.120+10.1.8.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    initinfo.SSTList.push_back("000185.sst");
    initinfo.SSTList.push_back("000309.sst");
    initinfo.SSTList.push_back("000459.sst");
    initinfo.SSTList.push_back("000583.sst");
    initinfo.SSTList.push_back("000736.sst");
    initinfo.SSTList.push_back("000834.sst");
    // initinfo.SSTList.push_back("001653.sst");
    // initinfo.SSTList.push_back("8.sst");
    // initinfo.SSTList.push_back("16.sst");
    initinfo.CSDList.clear();
    initinfo.CSDList.push_back("16");
    initinfo.CSDList.push_back("24");
    CSD_Map_.insert(make_pair("8",initinfo));
}

CSDInfo CSDManager::getCSDInfo(string CSDID){
    return CSD_Map_[CSDID];
}


void CSDManager::CSDBlockDesc(string id, int num){
    CSD_Map_[id].CSDWorkingBlock = CSD_Map_[id].CSDWorkingBlock - num;
}

vector<string> CSDManager::getCSDIDs(){
    vector<string> ret;
    for(auto i = CSD_Map_.begin(); i != CSD_Map_.end(); i++){
        pair<string,CSDInfo> tmppair;
        tmppair = *i;
        ret.push_back(tmppair.first);
    }
    return ret;
}


string CSDManager::getsstincsd(string sstname){
    for(auto it = CSD_Map_.begin(); it != CSD_Map_.end(); it++){
        pair<string,CSDInfo> tmpp = *it;
        for(int i = 0; i < tmpp.second.SSTList.size(); i++){
            if(sstname == tmpp.second.SSTList[i]){
                return tmpp.first;
            }
        }
    }
    return "";
}