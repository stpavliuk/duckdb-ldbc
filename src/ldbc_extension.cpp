#define DUCKDB_EXTENSION_MAIN

#include "ldbc_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void LdbcScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Ldbc " + name.GetString() + " 🐥");
	});
}

inline void LdbcOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Ldbc " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto ldbc_scalar_function = ScalarFunction("ldbc", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LdbcScalarFun);
	loader.RegisterFunction(ldbc_scalar_function);

	// Register another scalar function
	auto ldbc_openssl_version_scalar_function = ScalarFunction("ldbc_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, LdbcOpenSSLVersionScalarFun);
	loader.RegisterFunction(ldbc_openssl_version_scalar_function);
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
