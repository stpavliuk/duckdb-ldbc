#define DUCKDB_EXTENSION_MAIN

#include "ldbc_extension.hpp"
#include "snb_functions.hpp"

#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction datagen_func("snb_datagen", {}, SNBDatagenFunction, SNBDatagenBind);
	datagen_func.named_parameters["sf"] = LogicalType::DOUBLE;
	datagen_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	datagen_func.named_parameters["catalog"] = LogicalType::VARCHAR;
	datagen_func.named_parameters["schema"] = LogicalType::VARCHAR;
	datagen_func.named_parameters["create_pg"] = LogicalType::BOOLEAN;
	datagen_func.named_parameters["data_path"] = LogicalType::VARCHAR;
	datagen_func.named_parameters["download"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(datagen_func);

	auto snb_pragma = PragmaFunction::PragmaCall("snb", PragmaSNBQuery, {LogicalType::BIGINT});
	loader.RegisterFunction(snb_pragma);
	auto snb_execute_pragma = PragmaFunction::PragmaCall("snb_execute", PragmaSNBExecuteQuery, {LogicalType::BIGINT});
	loader.RegisterFunction(snb_execute_pragma);

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
