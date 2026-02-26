#define DUCKDB_EXTENSION_MAIN

#include "ldbc_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct SNBDatagenFunctionData : public TableFunctionData {
	bool finished = false;
	double sf = 0.003;
	bool overwrite = false;
	string catalog = INVALID_CATALOG;
	string schema = DEFAULT_SCHEMA;
	bool create_pg = false;
};

struct SNBListGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> SNBDatagenBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SNBDatagenFunctionData>();
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("Cannot use NULL as function argument");
		}
		if (kv.first == "sf") {
			result->sf = kv.second.GetValue<double>();
		} else if (kv.first == "overwrite") {
			result->overwrite = kv.second.GetValue<bool>();
		} else if (kv.first == "catalog") {
			result->catalog = StringValue::Get(kv.second);
		} else if (kv.first == "schema") {
			result->schema = StringValue::Get(kv.second);
		} else if (kv.first == "create_pg") {
			result->create_pg = kv.second.GetValue<bool>();
		}
	}
	if (input.binder) {
		auto &catalog = Catalog::GetCatalog(context, result->catalog);
		auto &properties = input.binder->GetStatementProperties();
		properties.RegisterDBModify(catalog, context);
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("success");
	return std::move(result);
}

static void SNBDatagenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<SNBDatagenFunctionData>();
	if (data.finished) {
		return;
	}
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	data.finished = true;
}

static unique_ptr<FunctionData> SNBQueriesBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("category");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query_pgq");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("query_sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parameters");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("default_params");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void SNBQueriesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<SNBListGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;
}

static unique_ptr<FunctionData> SNBAnswersBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("category");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("answer");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void SNBAnswersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<SNBListGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;
}

static unique_ptr<GlobalTableFunctionState> SNBInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SNBListGlobalState>();
}

static string PragmaSNBQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto category = StringValue::Get(parameters.values[0]);
	if (category != "IC" && category != "IS" && category != "BI") {
		throw InvalidInputException("Unsupported SNB category '%s'. Expected one of: IC, IS, BI", category);
	}
	auto query_nr = parameters.values[1].GetValue<int64_t>();
	if (query_nr <= 0) {
		throw InvalidInputException("SNB query number must be positive");
	}
	return "SELECT 'SNB query execution is not implemented yet' AS status";
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction datagen_func("snb_datagen", {}, SNBDatagenFunction, SNBDatagenBind);
	datagen_func.named_parameters["sf"] = LogicalType::DOUBLE;
	datagen_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	datagen_func.named_parameters["catalog"] = LogicalType::VARCHAR;
	datagen_func.named_parameters["schema"] = LogicalType::VARCHAR;
	datagen_func.named_parameters["create_pg"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(datagen_func);

	auto snb_pragma = PragmaFunction::PragmaCall("snb", PragmaSNBQuery, {LogicalType::VARCHAR, LogicalType::BIGINT});
	loader.RegisterFunction(snb_pragma);

	TableFunction queries_func("snb_queries", {}, SNBQueriesFunction, SNBQueriesBind, SNBInit);
	loader.RegisterFunction(queries_func);

	TableFunction answers_func("snb_answers", {}, SNBAnswersFunction, SNBAnswersBind, SNBInit);
	loader.RegisterFunction(answers_func);
}

void LdbcExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string LdbcExtension::Name() {
	return "ldbc";
}

std::string LdbcExtension::Version() const {
#ifdef EXT_VERSION_LDBC
	return EXT_VERSION_LDBC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ldbc, loader) {
	duckdb::LoadInternal(loader);
}
}
