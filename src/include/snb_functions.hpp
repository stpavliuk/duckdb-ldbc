#pragma once

#include "duckdb.hpp"

namespace duckdb {

unique_ptr<FunctionData> SNBDatagenBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names);
void SNBDatagenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

unique_ptr<FunctionData> SNBQueriesBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names);
void SNBQueriesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

unique_ptr<FunctionData> SNBAnswersBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names);
void SNBAnswersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

unique_ptr<GlobalTableFunctionState> SNBInit(ClientContext &context, TableFunctionInitInput &input);

string PragmaSNBQuery(ClientContext &context, const FunctionParameters &parameters);
string PragmaSNBExecuteQuery(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb
