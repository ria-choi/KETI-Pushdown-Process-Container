#include "ColumnEncoding.h"

void ColumnEncoding::init_data(){
    printf("Call Column Encoding::init_data method...\n");
}

void ColumnEncoding::Encode(){
    printf("Call Column Encoding::Encode method...\n");
}

int8_t KETI_INT8_TO_MySQL_INT8(string data){
    int8_t ret;
    ret = atoi(data.c_str());
    return ret;
}

int16_t KETI_INT16_TO_MySQL_INT16(string data){
    int16_t ret;
    ret = atoi(data.c_str());
    return ret;
}

int32_t KETI_INT32_TO_MySQL_INT32(string data){
    int32_t ret;
    ret = atoi(data.c_str());
    return ret;
}

int64_t KETI_INT64_TO_MySQL_INT64(string data){
    int64_t ret;
    ret = atoi(data.c_str());
    return ret;
}

float KETI_FLOAT32_TO_MySQL_FLOAT32(string data){
    float ret;
    ret = atof(data.c_str());
    return ret;
}

double KETI_FLOAT64_TO_MySQL_FLOAT64(string data){
    double ret;
    ret = stod(data.c_str());
    return ret;
}

string KETI_NUMERIC_TO_MySQL_NUMERIC(string data){
    string ret;
    ret = StringToDecimal();
    return ret;
}

int32_t KETI_DATE_TO_MySQL_DATE(string data){
    int32_t ret;
    ret = StringToDate(data);
    return ret;
}

int64_t KETI_TIMESTAMP_TO_MySQL_TIMESTAMP(string data){
    int64_t ret;
    tm timeinfo;
    int year;
    int mon;
    int mday;
    int hour;
    int min;
    int sec;
    std::vector<char> writable(data.begin(), data.end());
    writable.push_back('\0');
    char* ptr = &writable[0];
    char *tok = strtok(ptr,"-");
    year = atoi(tok);
    tok = strtok(NULL,"-");
    mon = atoi(tok);
    tok = strtok(NULL," ");
    mday = atoi(tok);
    tok = strtok(NULL,":");
    hour = atoi(tok);
    tok = strtok(NULL,":");
    min = atoi(tok);
    tok = strtok(NULL,"\0");
    sec = atoi(tok);
    // ret = atoi(data.c_str());
    timeinfo.tm_year = year - 1900;
    timeinfo.tm_mon = mon - 1;
    timeinfo.tm_mday = mday;
    timeinfo.tm_hour = hour;
    timeinfo.tm_min = min;
    timeinfo.tm_sec = sec;
    return ret;
}

string KETI_STRING_TO_MySQL_STRING(string data){
    return "+" + data;
}


string StringToDecimal(string data, int length1, int length2){
    int int_decimal = 0;
    int float_decimal = 0;
    string ret = "+80";
    std::vector<char> writable(data.begin(), data.end());
    writable.push_back('\0');
    char* ptr = &writable[0];
    char *tok = strtok(ptr,".");
    int_decimal = atoi(tok);
    tok = strtok(NULL,"\0");
    float_decimal = atoi(tok);
    std::stringstream ss;
    std::string s;

    ss << std::hex << int_decimal;
    
    s = ss.str();
    for(int i = 0; i < length1 * 2 - s.size(); i++){
        ret = ret + "0";
    }
    ret = ret + s;
    std::stringstream ss;
    std::string s;

    ss << std::hex << float_decimal;
    s = ss.str();
    for(int i = 0; i < length2 * 2 - s.size(); i++){
        ret = ret + "0";
    }
    ret = ret + s;
    return ret;
}

int StringToDate(string data){
    int int_date = 0;
    std::vector<char> writable(data.begin(), data.end());
    writable.push_back('\0');
    char* ptr = &writable[0];
    char *tok = strtok(ptr,"-");
    int_date += atoi(tok) * 32 * 16;
    tok = strtok(NULL,"-");
    int_date += atoi(tok) * 32;
    tok = strtok(NULL,"\0");
    int_date += atoi(tok);
    return int_date;
}