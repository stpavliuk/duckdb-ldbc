#include "snb_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <cmath>

namespace duckdb {

namespace {

constexpr double SF_0003 = 0.003;

struct SNBColumnSpec {
	string name;
	LogicalType type;
	bool not_null;
	bool primary_key;
};

struct SNBTableSpec {
	string table_name;
	string file_name;
	vector<SNBColumnSpec> columns;
};

struct SNBDatagenFunctionData : public TableFunctionData {
	bool finished = false;
	double sf = SF_0003;
	bool overwrite = false;
	string catalog = INVALID_CATALOG;
	string schema = DEFAULT_SCHEMA;
	bool create_pg = false;
	string data_path;
};

struct SNBListGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

SNBColumnSpec Column(string name, LogicalType type, bool not_null = false, bool primary_key = false) {
	return SNBColumnSpec {std::move(name), std::move(type), not_null, primary_key};
}

const vector<SNBTableSpec> &GetSNBTables() {
	static const vector<SNBTableSpec> tables = [] {
		const SNBTableSpec PERSON_WORKAT_ORGANISATION_TABLE = {
		    "Person_workAt_Organisation",
		    "person_workat_organisation.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("PersonId", LogicalType::BIGINT, true),
		        Column("OrganisationId", LogicalType::BIGINT, true),
		        Column("workFrom", LogicalType::INTEGER),
		        Column("classYear", LogicalType::INTEGER),
		    },
		};
		const SNBTableSpec MESSAGE_REPLYOF_MESSAGE_TABLE = {
		    "Message_replyOf_Message",
		    "message_replyof_message.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("messageId", LogicalType::BIGINT, true),
		        Column("parentMessageId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec MESSAGE_HASAUTHOR_PERSON_TABLE = {
		    "Message_hasAuthor_Person",
		    "message_hasauthor_person.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("messageId", LogicalType::BIGINT, true),
		        Column("personId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec MESSAGE_HASTAG_TAG_TABLE = {
		    "Message_hasTag_Tag",
		    "message_hastag_tag.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("id", LogicalType::BIGINT, true),
		        Column("TagId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec PERSON_LIKES_MESSAGE_TABLE = {
		    "Person_likes_Message",
		    "person_likes_message.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("PersonId", LogicalType::BIGINT, true),
		        Column("id", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec MESSAGE_TABLE = {
		    "Message",
		    "message.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("id", LogicalType::BIGINT),
		        Column("language", LogicalType::VARCHAR),
		        Column("content", LogicalType::VARCHAR),
		        Column("imageFile", LogicalType::VARCHAR),
		        Column("locationIP", LogicalType::VARCHAR, true),
		        Column("browserUsed", LogicalType::VARCHAR, true),
		        Column("length", LogicalType::INTEGER, true),
		        Column("CreatorPersonId", LogicalType::BIGINT, true),
		        Column("ContainerForumId", LogicalType::BIGINT),
		        Column("LocationCountryId", LogicalType::BIGINT, true),
		        Column("ParentMessageId", LogicalType::BIGINT),
		        Column("typeMask", LogicalType::BIGINT),
		    },
		};
		const SNBTableSpec PERSON_KNOWS_PERSON_TABLE = {
		    "Person_knows_Person",
		    "person_knows_person.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("Person1Id", LogicalType::BIGINT, true),
		        Column("Person2Id", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec PERSON_WORKAT_COMPANY_TABLE = {
		    "Person_workAt_Company",
		    "person_workat_company.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("PersonId", LogicalType::BIGINT, true),
		        Column("CompanyId", LogicalType::BIGINT, true),
		        Column("workFrom", LogicalType::INTEGER, true),
		    },
		};
		const SNBTableSpec PERSON_STUDYAT_UNIVERSITY_TABLE = {
		    "Person_studyAt_University",
		    "person_studyat_university.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("PersonId", LogicalType::BIGINT, true),
		        Column("UniversityId", LogicalType::BIGINT, true),
		        Column("classYear", LogicalType::INTEGER, true),
		    },
		};
		const SNBTableSpec PERSON_HASINTEREST_TAG_TABLE = {
		    "Person_hasInterest_Tag",
		    "person_hasinterest_tag.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("PersonId", LogicalType::BIGINT, true),
		        Column("TagId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec FORUM_HASTAG_TAG_TABLE = {
		    "Forum_hasTag_Tag",
		    "forum_hastag_tag.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("ForumId", LogicalType::BIGINT, true),
		        Column("TagId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec FORUM_HASMEMBER_PERSON_TABLE = {
		    "Forum_hasMember_Person",
		    "forum_hasmember_person.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("ForumId", LogicalType::BIGINT, true),
		        Column("PersonId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec PERSON_TABLE = {
		    "Person",
		    "person.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("id", LogicalType::BIGINT),
		        Column("firstName", LogicalType::VARCHAR, true),
		        Column("lastName", LogicalType::VARCHAR, true),
		        Column("gender", LogicalType::VARCHAR, true),
		        Column("birthday", LogicalType::DATE, true),
		        Column("locationIP", LogicalType::VARCHAR, true),
		        Column("browserUsed", LogicalType::VARCHAR, true),
		        Column("LocationCityId", LogicalType::BIGINT, true),
		        Column("speaks", LogicalType::VARCHAR, true),
		        Column("email", LogicalType::VARCHAR, true),
		    },
		};
		const SNBTableSpec FORUM_TABLE = {
		    "Forum",
		    "forum.csv",
		    {
		        Column("creationDate", LogicalType::TIMESTAMP_TZ, true),
		        Column("id", LogicalType::BIGINT),
		        Column("title", LogicalType::VARCHAR, true),
		        Column("ModeratorPersonId", LogicalType::BIGINT),
		    },
		};
		const SNBTableSpec UNIVERSITY_TABLE = {
		    "University",
		    "university.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("LocationPlaceId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec COMPANY_TABLE = {
		    "Company",
		    "company.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("LocationPlaceId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec CITY_TABLE = {
		    "City",
		    "city.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("PartOfCountryId", LogicalType::BIGINT),
		    },
		};
		const SNBTableSpec COUNTRY_TABLE = {
		    "Country",
		    "country.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("PartOfContinentId", LogicalType::BIGINT),
		    },
		};
		const SNBTableSpec TAGCLASS_TABLE = {
		    "TagClass",
		    "tagclass.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("SubclassOfTagClassId", LogicalType::BIGINT),
		    },
		};
		const SNBTableSpec TAG_TABLE = {
		    "Tag",
		    "tag.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("TypeTagClassId", LogicalType::BIGINT, true),
		    },
		};
		const SNBTableSpec PLACE_TABLE = {
		    "Place",
		    "place.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("type", LogicalType::VARCHAR, true),
		        Column("PartOfPlaceId", LogicalType::BIGINT),
		    },
		};
		const SNBTableSpec ORGANISATION_TABLE = {
		    "Organisation",
		    "organisation.csv",
		    {
		        Column("id", LogicalType::BIGINT, true, true),
		        Column("type", LogicalType::VARCHAR, true),
		        Column("name", LogicalType::VARCHAR, true),
		        Column("url", LogicalType::VARCHAR, true),
		        Column("LocationPlaceId", LogicalType::BIGINT, true),
		        Column("typeMask", LogicalType::BIGINT),
		    },
		};

		return vector<SNBTableSpec> {
		    PERSON_WORKAT_ORGANISATION_TABLE,
		    MESSAGE_REPLYOF_MESSAGE_TABLE,
		    MESSAGE_HASAUTHOR_PERSON_TABLE,
		    MESSAGE_HASTAG_TAG_TABLE,
		    PERSON_LIKES_MESSAGE_TABLE,
		    MESSAGE_TABLE,
		    PERSON_KNOWS_PERSON_TABLE,
		    PERSON_WORKAT_COMPANY_TABLE,
		    PERSON_STUDYAT_UNIVERSITY_TABLE,
		    PERSON_HASINTEREST_TAG_TABLE,
		    FORUM_HASTAG_TAG_TABLE,
		    FORUM_HASMEMBER_PERSON_TABLE,
		    PERSON_TABLE,
		    FORUM_TABLE,
		    UNIVERSITY_TABLE,
		    COMPANY_TABLE,
		    CITY_TABLE,
		    COUNTRY_TABLE,
		    TAGCLASS_TABLE,
		    TAG_TABLE,
		    PLACE_TABLE,
		    ORGANISATION_TABLE,
		};
	}();
	return tables;
}

string ResolveDataPath(ClientContext &context, const string &data_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!data_path.empty()) {
		if (!fs.DirectoryExists(data_path)) {
			throw InvalidInputException("SNB data_path does not exist: %s", data_path);
		}
		return data_path;
	}

	vector<string> candidates {"duckdb/data/SNB0.003", "../duckpgq-extension/duckdb/data/SNB0.003"};
	for (auto &candidate : candidates) {
		if (fs.DirectoryExists(candidate)) {
			return candidate;
		}
	}

	throw InvalidInputException(
	    "Unable to locate SNB SF0.003 data directory. Pass snb_datagen(data_path := '<path>') or place data at "
	    "duckdb/data/SNB0.003");
}

void CreateSNBTable(ClientContext &context, const string &catalog_name, const string &schema_name,
                    const SNBTableSpec &table) {
	auto info = make_uniq<CreateTableInfo>();
	info->catalog = catalog_name;
	info->schema = schema_name;
	info->table = table.table_name;

	for (idx_t i = 0; i < table.columns.size(); i++) {
		auto &column = table.columns[i];
		info->columns.AddColumn(ColumnDefinition(column.name, column.type));
		if (column.not_null || column.primary_key) {
			info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
		}
		if (column.primary_key) {
			info->constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(i), column.name, true));
		}
	}

	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	catalog.CreateTable(context, std::move(info));
}

void CreateSchemaIfNotExists(ClientContext &context, const string &catalog_name, const string &schema_name) {
	CreateSchemaInfo schema_info;
	schema_info.catalog = catalog_name;
	schema_info.schema = schema_name;
	schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	catalog.CreateSchema(context, schema_info);
}

void DropTableIfExists(ClientContext &context, const string &catalog_name, const string &schema_name,
                       const string &table_name) {
	DropInfo info;
	info.type = CatalogType::TABLE_ENTRY;
	info.catalog = catalog_name;
	info.schema = schema_name;
	info.name = table_name;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	info.cascade = false;

	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	catalog.DropEntry(context, info);
}

void LoadCSVIntoTable(Connection &con, const string &catalog_name, const string &schema_name, const SNBTableSpec &table,
                      const string &csv_file) {
	named_parameter_map_t options;
	options["header"] = Value::BOOLEAN(false);
	options["delim"] = Value(",");
	options["quote"] = Value("\"");
	auto csv_relation = con.ReadCSV(csv_file, std::move(options));
	csv_relation->Insert(catalog_name, schema_name, table.table_name);
}

} // namespace

unique_ptr<FunctionData> SNBDatagenBind(ClientContext &context, TableFunctionBindInput &input,
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
		} else if (kv.first == "data_path") {
			result->data_path = StringValue::Get(kv.second);
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

void SNBDatagenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<SNBDatagenFunctionData>();
	if (data.finished) {
		return;
	}

	if (std::fabs(data.sf - SF_0003) > 0.0000001) {
		throw NotImplementedException("snb_datagen currently supports only sf=0.003");
	}

	auto data_path = ResolveDataPath(context, data.data_path);
	auto &fs = FileSystem::GetFileSystem(context);
	auto &tables = GetSNBTables();
	for (auto &table : tables) {
		auto csv_file = fs.JoinPath(data_path, table.file_name);
		if (!fs.FileExists(csv_file)) {
			throw InvalidInputException("Missing SNB CSV file: %s", csv_file);
		}
	}

	DuckDB db(DatabaseInstance::GetDatabase(context));
	Connection con(db);
	con.BeginTransaction();
	auto &working_context = *con.context;
	auto &catalog = Catalog::GetCatalog(working_context, data.catalog);
	MetaTransaction::Get(working_context).ModifyDatabase(catalog.GetAttached());
	auto catalog_name = catalog.GetName();
	auto schema_name = data.schema;
	CreateSchemaIfNotExists(working_context, catalog_name, schema_name);

	if (data.overwrite) {
		for (idx_t i = tables.size(); i > 0; i--) {
			auto &table = tables[i - 1];
			DropTableIfExists(working_context, catalog_name, schema_name, table.table_name);
		}
	}

	for (auto &table : tables) {
		CreateSNBTable(working_context, catalog_name, schema_name, table);
	}

	for (auto &table : tables) {
		auto csv_file = fs.JoinPath(data_path, table.file_name);
		LoadCSVIntoTable(con, catalog_name, schema_name, table, csv_file);
	}
	con.Commit();

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	data.finished = true;
}

unique_ptr<FunctionData> SNBQueriesBind(ClientContext &context, TableFunctionBindInput &input,
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

void SNBQueriesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<SNBListGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;
}

unique_ptr<FunctionData> SNBAnswersBind(ClientContext &context, TableFunctionBindInput &input,
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

void SNBAnswersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<SNBListGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;
}

unique_ptr<GlobalTableFunctionState> SNBInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SNBListGlobalState>();
}

string PragmaSNBQuery(ClientContext &context, const FunctionParameters &parameters) {
	const auto category = StringValue::Get(parameters.values[0]);
	if (category != "IC" && category != "IS" && category != "BI") {
		throw InvalidInputException("Unsupported SNB category '%s'. Expected one of: IC, IS, BI", category);
	}
	const auto query_nr = parameters.values[1].GetValue<int64_t>();
	if (query_nr <= 0) {
		throw InvalidInputException("SNB query number must be positive");
	}
	return "SELECT 'SNB query execution is not implemented yet' AS status";
}

} // namespace duckdb
