#include "StorageEngineInputInterface.h"

// using namespace std;

class ColumnEncoding{
    void init_data();
    void Encode();
};

string StringToDecimal(string data, int length1, int length2);
int StringToDate(string data);

int8_t KETI_INT8_TO_MySQL_INT8(string data);

int16_t KETI_INT16_TO_MySQL_INT16(string data);

int32_t KETI_INT32_TO_MySQL_INT32(string data);

int64_t KETI_INT64_TO_MySQL_INT64(string data);

float KETI_FLOAT32_TO_MySQL_FLOAT32(string data);

double KETI_FLOAT64_TO_MySQL_FLOAT64(string data);

string KETI_NUMERIC_TO_MySQL_NUMERIC(string data);

int32_t KETI_DATE_TO_MySQL_DATE(string data);

int64_t KETI_TIMESTAMP_TO_MySQL_TIMESTAMP(string data);

string KETI_STRING_TO_MySQL_STRING(string data);
