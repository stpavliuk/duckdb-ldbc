#include "snb_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <exception>

namespace duckdb {

namespace {

struct SNBQuerySpec {
	string category;
	int32_t query_nr;
	string query_pgq;
	string query_sql;
	string parameters;
	string default_params;
};

struct SNBListGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

const vector<SNBQuerySpec> &GetSNBQueries() {
	static const vector<SNBQuerySpec> queries = [] {
		const SNBQuerySpec IC1_QUERY = {
		    "IC",
		    1,
		    R"pgq(SELECT g.p2_id, g.p2_lastname, g.p2_birthday, g.p2_creationdate, g.p2_gender, g.p2_browserused, g.p2_locationip, g.pl_name
FROM GRAPH_TABLE (graph
  MATCH
    (p1:person)-[:knows]-(p2:person)-[:person_islocatedin]->(pl:place)
  WHERE p1.p_personid = $personId
    AND p2.p_firstname = $firstName
  COLUMNS (
    p2.p_personid as p2_id,
    p2.p_lastname as p2_lastname,
    p2.p_birthday as p2_birthday,
    p2.p_creationdate as p2_creationdate,
    p2.p_gender as p2_gender,
    p2.p_browserused as p2_browserused,
    p2.p_locationip as p2_locationip,
    pl.pl_name as pl_name
  )
) g;)pgq",
		    "",
		    "personId,firstName",
		    R"json({"personId":933,"firstName":"Karl"})json",
		};
		const SNBQuerySpec IC2_QUERY = {
		    "IC",
		    2,
		    R"pgq(SELECT g.p2_id, g.p2_firstname, g.p2_lastname, g.c_messageid, g.c_content, g.c_creationdate
FROM GRAPH_TABLE (graph
  MATCH
    (p1:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(c:comment)
  WHERE p1.p_personid = $personId
    AND c.m_creationdate < $maxDate
  COLUMNS (
    p2.p_personid as p2_id,
    p2.p_firstname as p2_firstname,
    p2.p_lastname as p2_lastname,
    c.m_messageid as c_messageid,
    c.m_content as c_content,
    c.m_creationdate as c_creationdate
  )
) g;)pgq",
		    "",
		    "personId,maxDate",
		    R"json({"personId":933,"maxDate":"2012-01-01 00:00:00"})json",
		};
		const SNBQuerySpec IC3_QUERY = {
		    "IC",
		    3,
		    R"pgq(SELECT g.p2_id, g.p2_firstname, g.p2_lastname
FROM GRAPH_TABLE (graph
  MATCH
    (p1:person)-[k1:knows]-(p2:person)<-[:comment_hascreator]-(m1:comment)-[:comment_islocatedin]->(pl1:place),
    (p2)<-[:comment_hascreator]-(m2:comment)-[:comment_islocatedin]->(pl2:place)
  WHERE p1.p_personid = $personId
    AND m1.m_creationdate >= $startDate
    AND m1.m_creationdate < $durationDays
    AND m2.m_creationdate >= $startDate
    AND m2.m_creationdate < $durationDays
    AND pl1.pl_name = $countryXName
    AND pl2.pl_name = $countryYName
  COLUMNS (
    p2.p_personid as p2_id,
    p2.p_firstname as p2_firstname,
    p2.p_lastname as p2_lastname
  )
) g;)pgq",
		    "",
		    "personId,startDate,durationDays,countryXName,countryYName",
		    R"json({"personId":933,"startDate":"2012-01-01 00:00:00","durationDays":"2012-02-01 00:00:00","countryXName":"India","countryYName":"China"})json",
		};
		const SNBQuerySpec IC4_QUERY = {
		    "IC",
		    4,
		    R"pgq(SELECT g.t_name
FROM GRAPH_TABLE (graph
  MATCH
    (:person)-[k1:knows]-(p1:person)-[k2:knows]-(p2:person)<-[:post_hascreator]-(ps:post)-[:post_tag]->(t:tag)
  WHERE p1.p_personid = $personId
    AND ps.m_creationdate >= $startDate
    AND ps.m_creationdate < $durationDays
  COLUMNS (
    t.t_name as t_name
  )
) g;)pgq",
		    "",
		    "personId,startDate,durationDays",
		    R"json({"personId":933,"startDate":"2012-01-01 00:00:00","durationDays":"2012-02-01 00:00:00"})json",
		};
		const SNBQuerySpec IC5_QUERY = {
		    "IC",
		    5,
		    R"pgq(SELECT g.f_title
FROM GRAPH_TABLE (graph
  MATCH
  (p1:person)-[k1:knows]-(p2:person)-[fp:forum_person]->(f:forum)-[:containerof_post]->(p:post), (p)-[:post_hascreator]->(p2)
  WHERE p1.p_personid = $personId
    AND fp.fp_joindate >= $minDate
  COLUMNS (
    f.f_title AS f_title
  )
) g;)pgq",
		    "",
		    "personId,minDate",
		    R"json({"personId":933,"minDate":"2011-01-01 00:00:00"})json",
		};
		const SNBQuerySpec IC6_QUERY = {
		    "IC",
		    6,
		    R"pgq(SELECT g.t_name
FROM GRAPH_TABLE (graph
  MATCH
  (p1:person)-[:knows]-(p2:person)<-[:post_hascreator]-(m:post)-[:post_tag]->(t1:tag), (m)-[:post_tag]->(t2:tag)
  WHERE p1.p_personid = $personId
  AND t1.t_name = $tagName
  AND t2.t_name <> $tagName
  COLUMNS (
    t2.t_name as t_name
  )
) g;)pgq",
		    "",
		    "personId,tagName",
		    R"json({"personId":933,"tagName":"BasketballPlayer"})json",
		};
		const SNBQuerySpec IC7_QUERY = {
		    "IC",
		    7,
		    R"pgq(SELECT g.p_personid, g.p_firstname, g.p_lastname, g.m_content
FROM GRAPH_TABLE (graph
  MATCH
  (p2:person)-[:likes_comment]->(c:comment)-[:comment_hascreator]->(p1:person), (p1)-[:knows]-(p2)
  WHERE p1.p_personid = $personId
  COLUMNS (
    p2.p_personid AS p_personid,
    p2.p_firstname AS p_firstname,
    p2.p_lastname AS p_lastname,
    c.m_content AS m_content
  )
) g;)pgq",
		    "",
		    "personId",
		    R"json({"personId":933})json",
		};
		const SNBQuerySpec IC8_QUERY = {
		    "IC",
		    8,
		    R"pgq(SELECT g.p_personid, g.p_firstname, g.p_lastname, g.m_creationdate, g.m_messageid, g.m_content
FROM GRAPH_TABLE (graph
  MATCH
  (p2:person)<-[:comment_hascreator]-(c:comment)-[:comment_replyof_post]->(ps:post)-[:post_hascreator]->(p1:person)
  WHERE p1.p_personid = $personId
  COLUMNS (
    p2.p_personid AS p_personid,
    p2.p_firstname AS p_firstname,
    p2.p_lastname AS p_lastname,
    c.m_creationdate AS m_creationdate,
    c.m_messageid AS m_messageid,
    c.m_content AS m_content
  )
) g;)pgq",
		    "",
		    "personId",
		    R"json({"personId":933})json",
		};
		const SNBQuerySpec IC9_QUERY = {
		    "IC",
		    9,
		    R"pgq(SELECT g.p_firstname, g.p_lastname, g.m_creationdate
FROM GRAPH_TABLE (graph
  MATCH
  (p1:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(c:comment)
  WHERE p1.p_personid = $personId
  AND c.m_creationdate < $maxDate
  COLUMNS (
    p2.p_firstname AS p_firstname,
    p2.p_lastname AS p_lastname,
    c.m_creationdate AS m_creationdate
  )
) g;)pgq",
		    "",
		    "personId,maxDate",
		    R"json({"personId":933,"maxDate":"2012-01-01 00:00:00"})json",
		};
		const SNBQuerySpec IC11_QUERY = {
		    "IC",
		    11,
		    R"pgq(SELECT g.p_personid, g.p_firstname, g.p_lastname, g.o_name, g.pc_workfrom
FROM GRAPH_TABLE (graph
  MATCH
  (p1:person)-[:knows]-(p2:person)-[pc:person_company]->(o:organisation)-[:org_islocatedin]->(pl:place)
  WHERE p1.p_personid = $personId
  AND pc.pc_workfrom < $workFromYear
  AND pl.pl_name = $countryName
  COLUMNS (
    p2.p_personid AS p_personid,
    p2.p_firstname AS p_firstname,
    p2.p_lastname AS p_lastname,
    o.o_name AS o_name,
    pc.pc_workfrom AS pc_workfrom
  )
) g;)pgq",
		    "",
		    "personId,workFromYear,countryName",
		    R"json({"personId":933,"workFromYear":2010,"countryName":"China"})json",
		};
		const SNBQuerySpec IC12_QUERY = {
		    "IC",
		    12,
		    R"pgq(SELECT g.p_personid, g.p_firstname, g.p_lastname
FROM GRAPH_TABLE (graph
  MATCH
  (p1:person)-[:knows]-(f:person)<-[:comment_hascreator]-(c:comment)-[:comment_replyof_post]->(:post)-[:post_tag]->(t:tag)-[:tag_hastype]->(tc1:tagclass)-[:issubclassof]->(tc2:tagclass)
  WHERE p1.p_personid = $personId
  AND tc2.tc_name = $tagClassName
  COLUMNS (
    f.p_personid AS p_personid,
    f.p_firstname AS p_firstname,
    f.p_lastname AS p_lastname
  )
) g;)pgq",
		    "",
		    "personId,tagClassName",
		    R"json({"personId":933,"tagClassName":"Person"})json",
		};

		return vector<SNBQuerySpec> {
		    IC1_QUERY, IC2_QUERY, IC3_QUERY, IC4_QUERY,  IC5_QUERY,  IC6_QUERY,
		    IC7_QUERY, IC8_QUERY, IC9_QUERY, IC11_QUERY, IC12_QUERY,
		};
	}();

	return queries;
}

string EscapeSQLLiteral(const string &value) {
	return StringUtil::Replace(value, "'", "''");
}

const SNBQuerySpec &GetSNBQueryOrThrow(const int64_t query_nr) {
	if (query_nr <= 0) {
		throw InvalidInputException("SNB query number must be positive");
	}

	auto &queries = GetSNBQueries();
	for (auto &query : queries) {
		if (query.query_nr == query_nr) {
			return query;
		}
	}
	throw InvalidInputException("Unsupported SNB query %lld. Available query numbers are listed by snb_queries()",
	                            query_nr);
}

string RenderSNBPGQQueryWithDefaults(string query) {
	const vector<pair<string, string>> replacements = {
	    {"$countryXName", "'India'"},
	    {"$countryYName", "'China'"},
	    {"$countryName", "'China'"},
	    {"$durationDays", "TIMESTAMP '2012-02-01 00:00:00'"},
	    {"$startDate", "TIMESTAMP '2012-01-01 00:00:00'"},
	    {"$tagClassName", "'Person'"},
	    {"$workFromYear", "2010"},
	    {"$firstName", "'Karl'"},
	    {"$personId", "933"},
	    {"$minDate", "TIMESTAMP '2011-01-01 00:00:00'"},
	    {"$maxDate", "TIMESTAMP '2012-01-01 00:00:00'"},
	    {"$tagName", "'BasketballPlayer'"},
	};

	for (auto &replacement : replacements) {
		query = StringUtil::Replace(query, replacement.first, replacement.second);
	}

	return query;
}

string BuildSNBQueryPreviewSQL(const SNBQuerySpec &query) {
	const auto rendered = RenderSNBPGQQueryWithDefaults(query.query_pgq);

	return StringUtil::Format("SELECT '%s' AS category, %d AS query_nr, '%s' AS query_pgq, '%s' AS query_pgq_default",
	                          EscapeSQLLiteral(query.category), query.query_nr, EscapeSQLLiteral(query.query_pgq),
	                          EscapeSQLLiteral(rendered));
}

void ExecuteOrThrow(Connection &con, const string &sql) {
	const auto result = con.Query(sql);

	if (result->HasError()) {
		throw InvalidInputException("%s\nWhile executing SQL:\n%s", result->GetError(), sql);
	}
}

void EnsureExtensionLoaded(Connection &con, const string &extension_name) {
	auto load_result = con.Query(StringUtil::Format("LOAD %s", extension_name));
	if (!load_result->HasError()) {
		return;
	}

	ExecuteOrThrow(con, StringUtil::Format("INSTALL %s", extension_name));
	ExecuteOrThrow(con, StringUtil::Format("LOAD %s", extension_name));
}

} // namespace

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
	auto &queries = GetSNBQueries();

	if (state.offset >= queries.size()) {
		return;
	}

	idx_t chunk_count = 0;
	while (state.offset < queries.size() && chunk_count < STANDARD_VECTOR_SIZE) {
		auto &query = queries[state.offset];
		output.SetValue(0, chunk_count, Value(query.category));
		output.SetValue(1, chunk_count, Value::INTEGER(query.query_nr));
		output.SetValue(2, chunk_count, Value(query.query_pgq));
		output.SetValue(3, chunk_count, Value(query.query_sql));
		output.SetValue(4, chunk_count, Value(query.parameters));
		output.SetValue(5, chunk_count, Value(query.default_params));
		state.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
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
	if (state.offset > 0) {
		return;
	}

	state.offset = 1;
}

unique_ptr<GlobalTableFunctionState> SNBInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SNBListGlobalState>();
}

string PragmaSNBQuery(ClientContext &context, const FunctionParameters &parameters) {
	const auto query_nr = parameters.values[0].GetValue<int64_t>();
	const auto &query = GetSNBQueryOrThrow(query_nr);
	return BuildSNBQueryPreviewSQL(query);
}

string PragmaSNBExecuteQuery(ClientContext &context, const FunctionParameters &parameters) {
	const auto query_nr = parameters.values[0].GetValue<int64_t>();
	const auto &query = GetSNBQueryOrThrow(query_nr);
	const auto rendered_query = RenderSNBPGQQueryWithDefaults(query.query_pgq);

	DuckDB db(DatabaseInstance::GetDatabase(context));
	Connection con(db);

	try {
		EnsureExtensionLoaded(con, "duckpgq");
	} catch (std::exception &ex) {
		throw InvalidInputException("Unable to load duckpgq extension required for PGQ execution: %s", ex.what());
	}

	return rendered_query;
}

} // namespace duckdb
