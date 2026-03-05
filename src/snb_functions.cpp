#include "snb_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
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
	bool download = true;
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

bool ScaleFactorEquals(double left, double right) {
	return std::fabs(left - right) <= 0.0000001;
}

string QuoteIdentifier(const string &identifier) {
	return "\"" + StringUtil::Replace(identifier, "\"", "\"\"") + "\"";
}

void ExecuteOrThrow(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("%s\nWhile executing SQL:\n%s", result->GetError(), sql);
	}
}

void LoadSelectIntoTable(Connection &con, const string &catalog_name, const string &schema_name,
                         const string &table_name, const string &select_sql) {
	auto relation = con.RelationFromQuery(select_sql, "snb_load_relation");
	relation->Insert(catalog_name, schema_name, table_name);
}

void EnsureExtensionLoaded(Connection &con, const string &extension_name) {
	auto load_result = con.Query(StringUtil::Format("LOAD %s", extension_name));
	if (!load_result->HasError()) {
		return;
	}
	ExecuteOrThrow(con, StringUtil::Format("INSTALL %s", extension_name));
	ExecuteOrThrow(con, StringUtil::Format("LOAD %s", extension_name));
}

void EnsureDuckLakeLoaded(Connection &con) {
	EnsureExtensionLoaded(con, "httpfs");
	EnsureExtensionLoaded(con, "ducklake");
}

string GetDuckLakeURLForScaleFactor(double sf) {
	if (ScaleFactorEquals(sf, 0.1)) {
		return "ducklake:https://datasets.ldbcouncil.org/snb-interactive-v1-ducklake/sf0.1.ducklake";
	}
	if (ScaleFactorEquals(sf, 0.3)) {
		return "ducklake:https://datasets.ldbcouncil.org/snb-interactive-v1-ducklake/sf0.3.ducklake";
	}
	if (ScaleFactorEquals(sf, 1.0)) {
		return "ducklake:https://datasets.ldbcouncil.org/snb-interactive-v1-ducklake/sf1.ducklake";
	}
	if (ScaleFactorEquals(sf, 3.0)) {
		return "ducklake:https://datasets.ldbcouncil.org/snb-interactive-v1-ducklake/sf3.ducklake";
	}
	if (ScaleFactorEquals(sf, 10.0)) {
		return "ducklake:https://datasets.ldbcouncil.org/snb-interactive-v1-ducklake/sf10.ducklake";
	}
	throw NotImplementedException(
	    "Unsupported LDBC direct scale factor sf=%g. Supported values are: 0.1, 0.3, 1, 3, 10. "
	    "Use local sf=0.003 for the embedded dataset.",
	    sf);
}

void LoadFromLDBCDuckLake(Connection &con, const string &catalog_name, const string &schema_name, double sf) {
	const string source_alias = "__snb_source";
	const string source_alias_quoted = QuoteIdentifier(source_alias);
	const string source_url = GetDuckLakeURLForScaleFactor(sf);
	auto source_table = [&](const string &table_name) {
		return source_alias_quoted + ".main." + QuoteIdentifier(table_name);
	};

	ExecuteOrThrow(
	    con, StringUtil::Format("ATTACH '%s' AS %s", StringUtil::Replace(source_url, "'", "''"), source_alias_quoted));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person",
	                    StringUtil::Format(R"sql(
SELECT p_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, p_personid AS id, p_firstname AS firstName,
       p_lastname AS lastName, p_gender AS gender, p_birthday AS birthday, p_locationip AS locationIP,
       p_browserused AS browserUsed, p_placeid AS LocationCityId, p_languages AS speaks, p_emails AS email
FROM %s
)sql",
	                                       source_table("person")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Forum",
	                    StringUtil::Format(R"sql(
SELECT f_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, f_forumid AS id, f_title AS title,
       f_moderatorid AS ModeratorPersonId
FROM %s
)sql",
	                                       source_table("forum")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Place",
	                    StringUtil::Format(R"sql(
SELECT pl_placeid AS id, pl_name AS name, pl_url AS url, pl_type AS type, pl_containerplaceid AS PartOfPlaceId
FROM %s
)sql",
	                                       source_table("place")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "TagClass",
	                    StringUtil::Format(R"sql(
SELECT tc_tagclassid AS id, tc_name AS name, tc_url AS url, tc_subclassoftagclassid AS SubclassOfTagClassId
FROM %s
)sql",
	                                       source_table("tagclass")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Tag",
	                    StringUtil::Format(R"sql(
SELECT t_tagid AS id, t_name AS name, t_url AS url, t_tagclassid AS TypeTagClassId
FROM %s
)sql",
	                                       source_table("tag")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Organisation",
	                    StringUtil::Format(R"sql(
SELECT o_organisationid AS id, o_type AS type, o_name AS name, o_url AS url, o_placeid AS LocationPlaceId,
       CASE lower(o_type) WHEN 'company' THEN 1 WHEN 'university' THEN 2 ELSE NULL END AS typeMask
FROM %s
)sql",
	                                       source_table("organisation")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "University",
	                    StringUtil::Format(R"sql(
SELECT o_organisationid AS id, o_name AS name, o_url AS url, o_placeid AS LocationPlaceId
FROM %s
WHERE lower(o_type) = 'university'
)sql",
	                                       source_table("organisation")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Company",
	                    StringUtil::Format(R"sql(
SELECT o_organisationid AS id, o_name AS name, o_url AS url, o_placeid AS LocationPlaceId
FROM %s
WHERE lower(o_type) = 'company'
)sql",
	                                       source_table("organisation")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "City",
	                    StringUtil::Format(R"sql(
SELECT pl_placeid AS id, pl_name AS name, pl_url AS url, pl_containerplaceid AS PartOfCountryId
FROM %s
WHERE lower(pl_type) = 'city'
)sql",
	                                       source_table("place")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Country",
	                    StringUtil::Format(R"sql(
SELECT pl_placeid AS id, pl_name AS name, pl_url AS url, pl_containerplaceid AS PartOfContinentId
FROM %s
WHERE lower(pl_type) = 'country'
)sql",
	                                       source_table("place")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Forum_hasMember_Person",
	                    StringUtil::Format(R"sql(
SELECT fp_joindate::TIMESTAMP WITH TIME ZONE AS creationDate, fp_forumid AS ForumId, fp_personid AS PersonId
FROM %s
)sql",
	                                       source_table("forum_person")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Forum_hasTag_Tag",
	                    StringUtil::Format(R"sql(
SELECT f.f_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, ft.ft_forumid AS ForumId, ft.ft_tagid AS TagId
FROM %s ft
INNER JOIN %s f ON f.f_forumid = ft.ft_forumid
)sql",
	                                       source_table("forum_tag"), source_table("forum")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person_hasInterest_Tag",
	                    StringUtil::Format(R"sql(
SELECT p.p_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, pt.pt_personid AS PersonId, pt.pt_tagid AS TagId
FROM %s pt
INNER JOIN %s p ON p.p_personid = pt.pt_personid
)sql",
	                                       source_table("person_tag"), source_table("person")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person_workAt_Company",
	                    StringUtil::Format(R"sql(
SELECT p.p_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, pc.pc_personid AS PersonId,
       pc.pc_organisationid AS CompanyId, pc.pc_workfrom AS workFrom
FROM %s pc
INNER JOIN %s p ON p.p_personid = pc.pc_personid
)sql",
	                                       source_table("person_company"), source_table("person")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person_studyAt_University",
	                    StringUtil::Format(R"sql(
SELECT p.p_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, pu.pu_personid AS PersonId,
       pu.pu_organisationid AS UniversityId, pu.pu_classyear AS classYear
FROM %s pu
INNER JOIN %s p ON p.p_personid = pu.pu_personid
)sql",
	                                       source_table("person_university"), source_table("person")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person_workAt_Organisation",
	                    StringUtil::Format(R"sql(
SELECT p.p_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, pc.pc_personid AS PersonId,
       pc.pc_organisationid AS OrganisationId, pc.pc_workfrom AS workFrom, NULL::INTEGER AS classYear
FROM %s pc
INNER JOIN %s p ON p.p_personid = pc.pc_personid
UNION ALL
SELECT p.p_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, pu.pu_personid AS PersonId,
       pu.pu_organisationid AS OrganisationId, NULL::INTEGER AS workFrom, pu.pu_classyear AS classYear
FROM %s pu
INNER JOIN %s p ON p.p_personid = pu.pu_personid
)sql",
	                                       source_table("person_company"), source_table("person"),
	                                       source_table("person_university"), source_table("person")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person_knows_Person",
	                    StringUtil::Format(R"sql(
SELECT k_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, k_person1id AS Person1Id, k_person2id AS Person2Id
FROM %s
UNION ALL
SELECT k_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, k_person2id AS Person1Id, k_person1id AS Person2Id
FROM %s
)sql",
	                                       source_table("knows"), source_table("knows")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Person_likes_Message",
	                    StringUtil::Format(R"sql(
SELECT l_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, l_personid AS PersonId, l_messageid AS id
FROM %s
)sql",
	                                       source_table("likes")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Message_hasAuthor_Person",
	                    StringUtil::Format(R"sql(
SELECT m_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, m_messageid AS messageId, m_creatorid AS personId
FROM %s
UNION ALL
SELECT m_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, m_messageid AS messageId, m_creatorid AS personId
FROM %s
)sql",
	                                       source_table("post"), source_table("comment")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Message_replyOf_Message",
	                    StringUtil::Format(R"sql(
SELECT m_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, m_messageid AS messageId,
       coalesce(m_replyof_post, m_replyof_comment) AS parentMessageId
FROM %s
WHERE coalesce(m_replyof_post, m_replyof_comment) IS NOT NULL
)sql",
	                                       source_table("comment")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Message_hasTag_Tag",
	                    StringUtil::Format(R"sql(
WITH message_time AS (
	SELECT m_messageid, m_creationdate FROM %s
	UNION ALL
	SELECT m_messageid, m_creationdate FROM %s
), message_tag AS (
	SELECT mt_messageid, mt_tagid FROM %s
	UNION ALL
	SELECT mt_messageid, mt_tagid FROM %s
)
SELECT mt.m_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, tg.mt_messageid AS id, tg.mt_tagid AS TagId
FROM message_tag tg
INNER JOIN message_time mt ON mt.m_messageid = tg.mt_messageid
)sql",
	                                       source_table("post"), source_table("comment"), source_table("post_tag"),
	                                       source_table("comment_tag")));

	LoadSelectIntoTable(con, catalog_name, schema_name, "Message",
	                    StringUtil::Format(R"sql(
SELECT p.m_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, p.m_messageid AS id, p.m_ps_language AS language,
       p.m_content AS content, p.m_ps_imagefile AS imageFile, p.m_locationip AS locationIP, p.m_browserused AS browserUsed,
       p.m_length AS length, p.m_creatorid AS CreatorPersonId, p.m_ps_forumid AS ContainerForumId,
       coalesce(CASE lower(pl.pl_type)
		WHEN 'country' THEN pl.pl_placeid
		WHEN 'city' THEN pl.pl_containerplaceid
		ELSE NULL
	END, pl.pl_placeid) AS LocationCountryId, NULL::BIGINT AS ParentMessageId, 1::BIGINT AS typeMask
FROM %s p
LEFT JOIN %s pl ON pl.pl_placeid = p.m_locationid
UNION ALL
SELECT c.m_creationdate::TIMESTAMP WITH TIME ZONE AS creationDate, c.m_messageid AS id, NULL::VARCHAR AS language,
       c.m_content AS content, NULL::VARCHAR AS imageFile, c.m_locationip AS locationIP, c.m_browserused AS browserUsed,
       c.m_length AS length, c.m_creatorid AS CreatorPersonId, NULL::BIGINT AS ContainerForumId,
       coalesce(CASE lower(pl.pl_type)
		WHEN 'country' THEN pl.pl_placeid
		WHEN 'city' THEN pl.pl_containerplaceid
		ELSE NULL
	END, pl.pl_placeid) AS LocationCountryId, coalesce(c.m_replyof_post, c.m_replyof_comment) AS ParentMessageId,
       2::BIGINT AS typeMask
FROM %s c
LEFT JOIN %s pl ON pl.pl_placeid = c.m_locationid
)sql",
	                                       source_table("post"), source_table("place"), source_table("comment"),
	                                       source_table("place")));

	ExecuteOrThrow(con, StringUtil::Format("DETACH %s", source_alias_quoted));
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
		} else if (kv.first == "download") {
			result->download = kv.second.GetValue<bool>();
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

	DuckDB db(DatabaseInstance::GetDatabase(context));
	Connection con(db);
	if (data.download) {
		if (!data.data_path.empty()) {
			throw InvalidInputException("download=true does not use data_path");
		}

		EnsureDuckLakeLoaded(con);
	}

	string data_path;
	auto &tables = GetSNBTables();
	if (!data.download) {
		if (!ScaleFactorEquals(data.sf, SF_0003)) {
			throw NotImplementedException("download=false currently supports only sf=0.003");
		}

		data_path = ResolveDataPath(context, data.data_path);
		auto &fs = FileSystem::GetFileSystem(context);

		for (auto &table : tables) {
			auto csv_file = fs.JoinPath(data_path, table.file_name);
			if (!fs.FileExists(csv_file)) {
				throw InvalidInputException("Missing SNB CSV file: %s", csv_file);
			}
		}
	}

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

	if (data.download) {
		LoadFromLDBCDuckLake(con, catalog_name, schema_name, data.sf);
	} else {
		auto &fs = FileSystem::GetFileSystem(context);
		for (auto &table : tables) {
			auto csv_file = fs.JoinPath(data_path, table.file_name);
			LoadCSVIntoTable(con, catalog_name, schema_name, table, csv_file);
		}
	}

	con.Commit();

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	data.finished = true;
}

} // namespace duckdb
