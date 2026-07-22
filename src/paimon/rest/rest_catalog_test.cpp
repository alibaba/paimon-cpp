/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/rest/rest_catalog.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/table.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/rest/mock_rest_server.h"
#include "paimon/rest/rest_api.h"
#include "paimon/schema/schema.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

constexpr const char kToken[] = "test-token";
constexpr const char kPrefix[] = "paimon";
constexpr const char kWarehouse[] = "wh1";

// The in-memory catalog state behind the mock rest server.
struct MockCatalogState {
    struct TableData {
        std::string schema_json;
        int64_t schema_id = 0;
        std::string path;
    };
    std::map<std::string, std::map<std::string, TableData>> databases;
    // headers of the last request, with lower-cased names
    std::map<std::string, std::string> last_headers;
    // when set, every request except "/v1/config" fails with this http code
    std::optional<int32_t> force_error_code;
    // guards all fields above: the handler runs on the server's accept thread while
    // tests seed and inspect the state
    std::mutex mutex;
};

MockRestServer::Response JsonResponse(int32_t code, const std::string& body) {
    MockRestServer::Response response;
    response.code = code;
    response.body = body;
    return response;
}

MockRestServer::Response MockError(int32_t code, const std::string& resource_type,
                                   const std::string& resource_name, const std::string& message) {
    ErrorResponse error(resource_type, resource_name, message, code);
    return JsonResponse(code, error.ToJsonString().value());
}

std::string SnapshotJson(int64_t id) {
    return fmt::format(
        R"({{"version":3,"id":{},"schemaId":0,"baseManifestList":"bml","deltaManifestList":"dml",)"
        R"("commitUser":"user1","commitIdentifier":1,"commitKind":"APPEND","timeMillis":100,)"
        R"("totalRecordCount":10,"deltaRecordCount":1}})",
        id);
}

std::string TableResponseJson(const std::string& name, const MockCatalogState::TableData& table) {
    return fmt::format(
        R"({{"id":"1","name":"{}","path":"{}","isExternal":false,"schemaId":{},"schema":{},)"
        R"("owner":"owner1","updatedAt":123}})",
        name, table.path, table.schema_id, table.schema_json);
}

// Serves `names` one item per page to exercise the pagination loop of the client.
std::pair<std::vector<std::string>, std::optional<std::string>> PageOf(
    const std::vector<std::string>& names, const MockRestServer::Request& request) {
    size_t index = 0;
    auto token_iter = request.query_params.find(RestApi::kQueryParamPageToken);
    if (token_iter != request.query_params.end()) {
        // A malformed token must not throw: an exception on the accept thread would
        // terminate the test binary.
        index = static_cast<size_t>(
            StringUtils::StringToValue<uint64_t>(token_iter->second).value_or(0));
    }
    std::vector<std::string> page;
    std::optional<std::string> next_page_token;
    if (index < names.size()) {
        page.push_back(names[index]);
        if (index + 1 < names.size()) {
            next_page_token = std::to_string(index + 1);
        }
    }
    return {page, next_page_token};
}

// Implements the subset of the rest catalog protocol used by `RestCatalog` on top of
// `MockCatalogState`.
MockRestServer::Response HandleCatalogRequest(MockCatalogState* state,
                                              const MockRestServer::Request& request) {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_headers = request.headers;
    auto auth_iter = request.headers.find("authorization");
    if (auth_iter == request.headers.end() ||
        auth_iter->second != std::string("Bearer ") + kToken) {
        return MockError(401, "", "", "invalid token");
    }
    if (request.path == "/v1/config") {
        auto warehouse_iter = request.query_params.find("warehouse");
        if (warehouse_iter == request.query_params.end() || warehouse_iter->second != kWarehouse) {
            return MockError(400, "", "", "unexpected warehouse");
        }
        ConfigResponse config(
            {{RestApi::kOptionUrlPrefix, kPrefix},
             {"header.x-server-header", "from-config"},
             {"table-default.write-only", "true"},
             {"table-default.bucket", "8"}},
            {{"server-override", "from-server"}, {"header.x-shared-header", "from-config"}});
        return JsonResponse(200, config.ToJsonString().value());
    }
    if (state->force_error_code) {
        return MockError(state->force_error_code.value(), "", "", "injected failure");
    }
    const std::string base = std::string("/v1/") + kPrefix;
    if (request.path.rfind(base, 0) != 0) {
        return MockError(404, "", "", "unknown path " + request.path);
    }
    std::string rest = request.path.substr(base.size());

    if (rest == "/databases") {
        if (request.method == "GET") {
            std::vector<std::string> names;
            for (const auto& [name, tables] : state->databases) {
                names.push_back(name);
            }
            auto [page, next_page_token] = PageOf(names, request);
            ListDatabasesResponse response(page, next_page_token);
            return JsonResponse(200, response.ToJsonString().value());
        }
        if (request.method == "POST") {
            CreateDatabaseRequest create_request("", {});
            if (!RapidJsonUtil::FromJsonString(request.body, &create_request).ok()) {
                return MockError(400, "", "", "bad create database request");
            }
            if (state->databases.count(create_request.GetName()) > 0) {
                return MockError(409, ErrorResponse::kResourceTypeDatabase,
                                 create_request.GetName(), "database already exists");
            }
            state->databases[create_request.GetName()] = {};
            return JsonResponse(200, "");
        }
        return MockError(400, "", "", "unsupported method");
    }

    if (rest == "/tables/rename" && request.method == "POST") {
        RenameTableRequest rename_request("", "", "", "");
        if (!RapidJsonUtil::FromJsonString(request.body, &rename_request).ok()) {
            return MockError(400, "", "", "bad rename table request");
        }
        auto db_iter = state->databases.find(rename_request.GetSourceDatabase());
        if (db_iter == state->databases.end() ||
            db_iter->second.count(rename_request.GetSourceTable()) == 0) {
            return MockError(404, ErrorResponse::kResourceTypeTable,
                             rename_request.GetSourceTable(), "table not found");
        }
        auto& dest_tables = state->databases[rename_request.GetDestinationDatabase()];
        if (dest_tables.count(rename_request.GetDestinationTable()) > 0) {
            return MockError(409, ErrorResponse::kResourceTypeTable,
                             rename_request.GetDestinationTable(), "table already exists");
        }
        dest_tables[rename_request.GetDestinationTable()] =
            db_iter->second[rename_request.GetSourceTable()];
        db_iter->second.erase(rename_request.GetSourceTable());
        return JsonResponse(200, "");
    }

    const std::string databases_prefix = "/databases/";
    if (rest.rfind(databases_prefix, 0) != 0) {
        return MockError(404, "", "", "unknown path " + request.path);
    }
    std::string remainder = rest.substr(databases_prefix.size());
    size_t tables_pos = remainder.find("/tables");

    if (tables_pos == std::string::npos) {
        const std::string& db_name = remainder;
        auto db_iter = state->databases.find(db_name);
        if (request.method == "GET") {
            if (db_iter == state->databases.end()) {
                return MockError(404, ErrorResponse::kResourceTypeDatabase, db_name,
                                 "database not found");
            }
            std::string body = fmt::format(
                R"({{"id":"1","name":"{}","location":"{}/{}.db","options":{{"dbk":"dbv"}}}})",
                db_name, kWarehouse, db_name);
            return JsonResponse(200, body);
        }
        if (request.method == "DELETE") {
            if (db_iter == state->databases.end()) {
                return MockError(404, ErrorResponse::kResourceTypeDatabase, db_name,
                                 "database not found");
            }
            state->databases.erase(db_iter);
            return JsonResponse(200, "");
        }
        return MockError(400, "", "", "unsupported method");
    }

    std::string db_name = remainder.substr(0, tables_pos);
    auto db_iter = state->databases.find(db_name);
    if (db_iter == state->databases.end()) {
        return MockError(404, ErrorResponse::kResourceTypeDatabase, db_name, "database not found");
    }
    auto& tables = db_iter->second;
    std::string table_part = remainder.substr(tables_pos + std::strlen("/tables"));

    if (table_part.empty()) {
        if (request.method == "GET") {
            std::vector<std::string> names;
            for (const auto& [name, table] : tables) {
                names.push_back(name);
            }
            auto [page, next_page_token] = PageOf(names, request);
            ListTablesResponse response(page, next_page_token);
            return JsonResponse(200, response.ToJsonString().value());
        }
        if (request.method == "POST") {
            CreateTableRequest create_request("", "", "");
            if (!RapidJsonUtil::FromJsonString(request.body, &create_request).ok()) {
                return MockError(400, "", "", "bad create table request");
            }
            if (tables.count(create_request.GetTable()) > 0) {
                return MockError(409, ErrorResponse::kResourceTypeTable, create_request.GetTable(),
                                 "table already exists");
            }
            MockCatalogState::TableData table;
            table.schema_json = create_request.GetSchemaJson();
            table.schema_id = 0;
            table.path = fmt::format("{}/{}.db/{}", kWarehouse, db_name, create_request.GetTable());
            tables[create_request.GetTable()] = table;
            return JsonResponse(200, "");
        }
        return MockError(400, "", "", "unsupported method");
    }

    // "/{table}" or "/{table}/snapshots"
    std::string table_name = table_part.substr(1);
    bool list_snapshots = false;
    const std::string snapshots_suffix = "/snapshots";
    if (table_name.size() > snapshots_suffix.size() &&
        table_name.compare(table_name.size() - snapshots_suffix.size(), snapshots_suffix.size(),
                           snapshots_suffix) == 0) {
        table_name = table_name.substr(0, table_name.size() - snapshots_suffix.size());
        list_snapshots = true;
    }
    auto table_iter = tables.find(table_name);
    if (table_iter == tables.end()) {
        return MockError(404, ErrorResponse::kResourceTypeTable, table_name, "table not found");
    }
    if (list_snapshots) {
        // two pages, out of order to exercise pagination and sorting
        auto token_iter = request.query_params.find(RestApi::kQueryParamPageToken);
        if (token_iter == request.query_params.end()) {
            return JsonResponse(
                200, fmt::format(R"({{"snapshots":[{}],"nextPageToken":"1"}})", SnapshotJson(2)));
        }
        return JsonResponse(200, fmt::format(R"({{"snapshots":[{}]}})", SnapshotJson(1)));
    }
    if (request.method == "GET") {
        return JsonResponse(200, TableResponseJson(table_name, table_iter->second));
    }
    if (request.method == "DELETE") {
        tables.erase(table_iter);
        return JsonResponse(200, "");
    }
    return MockError(400, "", "", "unsupported method");
}

}  // namespace

class RestCatalogTest : public ::testing::Test {
 protected:
    void SetUp() override {
        state_ = std::make_shared<MockCatalogState>();
        ASSERT_OK_AND_ASSIGN(
            server_, MockRestServer::Start([state = state_](const MockRestServer::Request& req) {
                return HandleCatalogRequest(state.get(), req);
            }));
        options_ = {
            {CatalogOptions::METASTORE, "rest"},
            {CatalogOptions::URI, server_->GetBaseUri()},
            {CatalogOptions::TOKEN_PROVIDER, "bear"},
            {CatalogOptions::TOKEN, kToken},
            {Options::FILE_SYSTEM, "local"},
            // mock_format is linked statically into the test binary, so its factory is
            // registered in the binary's own registry even when the real format plugin
            // dylibs register into a different one (macOS two-level namespace)
            {Options::FILE_FORMAT, "mock_format"},
            {Options::MANIFEST_FORMAT, "mock_format"},
            {"header.x-client-header", "from-client"},
            {"header.x-shared-header", "from-client"},
        };
    }

    void TearDown() override {
        if (server_) {
            server_->Stop();
        }
    }

    Result<std::unique_ptr<RestCatalog>> CreateRestCatalog() {
        return RestCatalog::Create(kWarehouse, options_, nullptr);
    }

    Status CreateSampleTable(Catalog* catalog, const Identifier& identifier,
                             bool ignore_if_exists = false) {
        std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("f0", arrow::int32(), /*nullable=*/false),
                           arrow::field("f1", arrow::utf8())});
        struct ArrowSchema c_schema;
        if (!arrow::ExportSchema(*schema, &c_schema).ok()) {
            return Status::Invalid("failed to export arrow schema");
        }
        Status status =
            catalog->CreateTable(identifier, &c_schema, /*partition_keys=*/{},
                                 /*primary_keys=*/{"f0"}, {{"bucket", "2"}}, ignore_if_exists);
        // CreateTable takes ownership of the exported schema only once it reaches
        // arrow::ImportSchema, which an identifier rejected by its checks never does
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
        return status;
    }

    // Seeds `schema_json` as table `table_name` of "db1" behind the mock server and
    // expects loading the table to fail with an Invalid status carrying
    // `expected_message`.
    void ExpectBrokenSchemaRejected(Catalog* catalog, const std::string& table_name,
                                    const std::string& schema_json,
                                    const std::string& expected_message) {
        MockCatalogState::TableData table_data;
        table_data.schema_json = schema_json;
        table_data.path = "wh1/db1.db/" + table_name;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->databases["db1"][table_name] = table_data;
        }
        Status status = catalog->GetTable(Identifier("db1", table_name)).status();
        ASSERT_TRUE(status.IsInvalid()) << status.ToString();
        ASSERT_NOK_WITH_MSG(status, expected_message);
    }

    std::shared_ptr<MockCatalogState> state_;
    std::unique_ptr<MockRestServer> server_;
    std::map<std::string, std::string> options_;
};

TEST_F(RestCatalogTest, CreateMergesServerConfig) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    const std::map<std::string, std::string>& merged = catalog->GetOptions();
    ASSERT_EQ(kPrefix, merged.at(RestApi::kOptionUrlPrefix));
    ASSERT_EQ("from-server", merged.at("server-override"));
    ASSERT_EQ(kWarehouse, catalog->GetRootPath());
    ASSERT_NE(nullptr, catalog->GetFileSystem());
}

TEST_F(RestCatalogTest, CatalogFactoryMetastoreDispatch) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog, Catalog::Create(kWarehouse, options_));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, catalog->ListDatabases());
    ASSERT_TRUE(databases.empty());
    options_[CatalogOptions::METASTORE] = "something-else";
    Status status = Catalog::Create(kWarehouse, options_).status();
    ASSERT_TRUE(status.IsInvalid()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "unsupported metastore");
}

TEST_F(RestCatalogTest, CreateWithWrongTokenFails) {
    options_[CatalogOptions::TOKEN] = "wrong-token";
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "not authorized");
}

TEST_F(RestCatalogTest, CreateRejectsInvalidOptions) {
    // all rejected by client side validation, before any request reaches the server
    const std::map<std::string, std::string> valid_options = options_;

    options_.erase(CatalogOptions::URI);
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "'uri' must be configured");

    options_ = valid_options;
    options_.erase(CatalogOptions::TOKEN_PROVIDER);
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "'token.provider' must be configured");

    options_ = valid_options;
    options_[CatalogOptions::TOKEN_PROVIDER] = "dlf";
    Status unsupported_provider = CreateRestCatalog().status();
    ASSERT_TRUE(unsupported_provider.IsNotImplemented()) << unsupported_provider.ToString();
    ASSERT_NOK_WITH_MSG(unsupported_provider, "unsupported token provider");

    options_ = valid_options;
    options_.erase(CatalogOptions::TOKEN);
    ASSERT_NOK_WITH_MSG(CreateRestCatalog().status(), "bear token provider");
}

TEST_F(RestCatalogTest, DatabaseOperations) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());

    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateDatabase("db2", {}, /*ignore_if_exists=*/false));
    Status duplicated = catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false);
    ASSERT_TRUE(duplicated.IsExist()) << duplicated.ToString();
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/true));

    // the mock server returns one database per page
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, catalog->ListDatabases());
    ASSERT_EQ((std::vector<std::string>{"db1", "db2"}), databases);

    ASSERT_OK_AND_ASSIGN(bool exists, catalog->DatabaseExists("db1"));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->DatabaseExists("db3"));
    ASSERT_FALSE(exists);

    ASSERT_EQ("wh1/db1.db", catalog->GetDatabaseLocation("db1"));
    ASSERT_EQ("", catalog->GetDatabaseLocation("db3"));

    ASSERT_OK(catalog->DropDatabase("db2", /*ignore_if_not_exists=*/false, /*cascade=*/false));
    ASSERT_OK(catalog->DropDatabase("db2", /*ignore_if_not_exists=*/true, /*cascade=*/false));
    Status missing = catalog->DropDatabase("db2", /*ignore_if_not_exists=*/false,
                                           /*cascade=*/false);
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();

    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));
    ASSERT_NOK_WITH_MSG(
        catalog->DropDatabase("db1", /*ignore_if_not_exists=*/false, /*cascade=*/false),
        "non-empty database");
    // cascade drop skips the emptiness check
    ASSERT_OK(catalog->DropDatabase("db1", /*ignore_if_not_exists=*/false, /*cascade=*/true));
    ASSERT_OK_AND_ASSIGN(exists, catalog->DatabaseExists("db1"));
    ASSERT_FALSE(exists);
}

TEST_F(RestCatalogTest, TableOperations) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    Identifier identifier("db1", "t1");

    Status missing_db = CreateSampleTable(catalog.get(), Identifier("db_missing", "t1"));
    ASSERT_TRUE(missing_db.IsNotExist()) << missing_db.ToString();

    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));
    Status duplicated = CreateSampleTable(catalog.get(), identifier);
    ASSERT_TRUE(duplicated.IsExist()) << duplicated.ToString();
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier, /*ignore_if_exists=*/true));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> tables, catalog->ListTables("db1"));
    ASSERT_EQ((std::vector<std::string>{"t1"}), tables);
    Status list_missing = catalog->ListTables("db_missing").status();
    ASSERT_TRUE(list_missing.IsNotExist()) << list_missing.ToString();

    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(identifier));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t2")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::string location, catalog->GetTableLocation(identifier));
    ASSERT_EQ("wh1/db1.db/t1", location);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(identifier));
    ASSERT_EQ("t1", table->Name());
    std::shared_ptr<DataSchema> schema =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), schema->FieldNames());
    ASSERT_EQ((std::vector<std::string>{"f0"}), schema->PrimaryKeys());
    ASSERT_EQ(0, schema->Id());
    // options are enriched with the table path and audit info from the server
    ASSERT_EQ("wh1/db1.db/t1", schema->Options().at("path"));
    ASSERT_EQ("owner1", schema->Options().at("owner"));

    // "table-default." options of the merged config apply only where the caller left the
    // option unset: "write-only" is taken from the config, "bucket" keeps the value passed
    // to CreateTable instead of the configured "table-default.bucket" of 8
    ASSERT_EQ("true", schema->Options().at("write-only"));
    ASSERT_EQ("2", schema->Options().at("bucket"));

    // timeMillis is backed by the server's audit "updatedAt" instead of the current
    // time, keeping the conversion deterministic
    std::shared_ptr<TableSchema> table_schema =
        std::dynamic_pointer_cast<TableSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, table_schema);
    ASSERT_EQ(123, table_schema->TimeMillis());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> loaded_schema,
                         catalog->LoadTableSchema(identifier));
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), loaded_schema->FieldNames());
    Status schema_missing = catalog->LoadTableSchema(Identifier("db1", "t2")).status();
    ASSERT_TRUE(schema_missing.IsNotExist()) << schema_missing.ToString();

    // a schema comment of the server response is carried into the table schema
    MockCatalogState::TableData commented;
    commented.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                            R"( "partitionKeys": [], "primaryKeys": [], "options": {},)"
                            R"( "comment": "a table comment"})";
    commented.path = "wh1/db1.db/commented";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["commented"] = commented;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> commented_table,
                         catalog->GetTable(Identifier("db1", "commented")));
    ASSERT_EQ("a table comment", commented_table->LatestSchema()->Comment().value_or(""));

    ASSERT_OK(catalog->RenameTable(identifier, Identifier("db1", "t2"),
                                   /*ignore_if_not_exists=*/false));
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t2")));
    ASSERT_TRUE(exists);
    ASSERT_OK(catalog->RenameTable(identifier, Identifier("db1", "t3"),
                                   /*ignore_if_not_exists=*/true));
    Status rename_missing = catalog->RenameTable(identifier, Identifier("db1", "t3"),
                                                 /*ignore_if_not_exists=*/false);
    ASSERT_TRUE(rename_missing.IsNotExist()) << rename_missing.ToString();

    ASSERT_OK(catalog->DropTable(Identifier("db1", "t2"), /*ignore_if_not_exists=*/false));
    ASSERT_OK(catalog->DropTable(Identifier("db1", "t2"), /*ignore_if_not_exists=*/true));
    Status drop_missing = catalog->DropTable(Identifier("db1", "t2"),
                                             /*ignore_if_not_exists=*/false);
    ASSERT_TRUE(drop_missing.IsNotExist()) << drop_missing.ToString();
}

TEST_F(RestCatalogTest, ClientAndServerHeadersAreSent) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, catalog->ListDatabases());
    // "header." options from both the client and the merged server config are sent as
    // http headers on every request
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("from-client", state_->last_headers.at("x-client-header"));
        ASSERT_EQ("from-config", state_->last_headers.at("x-server-header"));
        // when the client and the server config set the same "header." option, the
        // merged config wins (overrides > client options > defaults)
        ASSERT_EQ("from-config", state_->last_headers.at("x-shared-header"));
        ASSERT_EQ(std::string("Bearer ") + kToken, state_->last_headers.at("authorization"));
    }
    // a request carrying a body declares the json content type (set before the auth
    // headers are merged, so a signing auth provider covers it)
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        ASSERT_EQ("application/json", state_->last_headers.at("content-type"));
    }
}

TEST_F(RestCatalogTest, SystemTableSchema) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));

    // the "options" system table has a static schema and needs no file system access
    Identifier system_identifier("db1", "t1$options");
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(system_identifier));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t9$options")));
    ASSERT_FALSE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t1$unsupported")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> schema,
                         catalog->LoadTableSchema(system_identifier));
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), schema->FieldNames());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(system_identifier));
    ASSERT_EQ("t1$options", table->Name());
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), table->LatestSchema()->FieldNames());

    Status unsupported = catalog->LoadTableSchema(Identifier("db1", "t1$unsupported")).status();
    ASSERT_TRUE(unsupported.IsNotExist()) << unsupported.ToString();
}

TEST_F(RestCatalogTest, BranchTableLoadsBranchSchemaFromServer) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));

    // the identifier sent to the server keeps the branch, so the server resolves it and
    // returns the branch's own schema. The path it reports is the data table root, not
    // the branch subdirectory: readers derive "<path>/branch/branch-<name>" from the
    // branch option, so a branch path here would be applied twice
    MockCatalogState::TableData branch_data;
    branch_data.schema_json = R"({"fields": [{"id": 0, "name": "b0", "type": "INT NOT NULL"}],)"
                              R"( "partitionKeys": [], "primaryKeys": [], "options": {}})";
    branch_data.schema_id = 3;
    branch_data.path = "wh1/db1.db/t1";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1$branch_b1"] = branch_data;
    }

    Identifier branch_identifier("db1", "t1$branch_b1");
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->TableExists(branch_identifier));
    ASSERT_TRUE(exists);
    // a branch the server does not know is missing instead of silently falling back to
    // the main table
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("db1", "t1$branch_missing")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::string location, catalog->GetTableLocation(branch_identifier));
    ASSERT_EQ("wh1/db1.db/t1", location);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(branch_identifier));
    ASSERT_EQ("t1$branch_b1", table->Name());
    std::shared_ptr<DataSchema> schema =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, schema);
    // the branch's own schema and schema id, not the main table's
    ASSERT_EQ((std::vector<std::string>{"b0"}), schema->FieldNames());
    ASSERT_EQ(3, schema->Id());
    ASSERT_EQ("b1", schema->Options().at(Options::BRANCH));

    // the default branch is addressed as the bare table: "t1$branch_main" resolves to
    // "t1" and carries no branch option
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main_table,
                         catalog->GetTable(Identifier("db1", "t1$branch_main")));
    std::shared_ptr<DataSchema> main_schema =
        std::dynamic_pointer_cast<DataSchema>(main_table->LatestSchema());
    ASSERT_NE(nullptr, main_schema);
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), main_schema->FieldNames());
    ASSERT_EQ(0, main_schema->Options().count(Options::BRANCH));

    // the default branch is matched ignoring case, as in the Java client, so
    // "t1$branch_MAIN" addresses the bare table too
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main_case_table,
                         catalog->GetTable(Identifier("db1", "t1$branch_MAIN")));
    std::shared_ptr<DataSchema> main_case_schema =
        std::dynamic_pointer_cast<DataSchema>(main_case_table->LatestSchema());
    ASSERT_NE(nullptr, main_case_schema);
    ASSERT_EQ((std::vector<std::string>{"f0", "f1"}), main_case_schema->FieldNames());
    ASSERT_EQ(0, main_case_schema->Options().count(Options::BRANCH));

    // a system table on a branch resolves against the branch's data table: the system
    // suffix is stripped while the branch stays in the identifier sent to the server
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> options_schema,
                         catalog->LoadTableSchema(Identifier("db1", "t1$branch_b1$options")));
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), options_schema->FieldNames());
    // a missing branch fails through the system table path too instead of silently
    // resolving against the main table
    Status missing_branch =
        catalog->LoadTableSchema(Identifier("db1", "t1$branch_missing$options")).status();
    ASSERT_TRUE(missing_branch.IsNotExist()) << missing_branch.ToString();

    ASSERT_NOK_WITH_MSG(catalog->DropTable(branch_identifier, /*ignore_if_not_exists=*/false),
                        "branch table");
    ASSERT_NOK_WITH_MSG(catalog->RenameTable(branch_identifier, Identifier("db1", "t2"),
                                             /*ignore_if_not_exists=*/false),
                        "branch table");
    ASSERT_NOK_WITH_MSG(CreateSampleTable(catalog.get(), Identifier("db1", "t2$branch_b1")),
                        "branch table");
}

TEST_F(RestCatalogTest, NestedSchemaHighestFieldId) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    MockCatalogState::TableData table_data;
    table_data.schema_json = R"({
        "fields": [
            {"id": 0, "name": "f0", "type": "INT NOT NULL"},
            {"id": 1, "name": "s", "type": {"type": "ROW",
                "fields": [{"id": 3, "name": "inner", "type": "INT"}]}},
            {"id": 2, "name": "arr", "type": {"type": "ARRAY",
                "element": {"type": "ROW",
                            "fields": [{"id": 7, "name": "deep", "type": "BIGINT"}]}}},
            {"id": 4, "name": "m", "type": {"type": "MAP",
                "key": {"type": "ROW NOT NULL",
                        "fields": [{"id": 8, "name": "k", "type": "INT NOT NULL"}]},
                "value": {"type": "ROW",
                          "fields": [{"id": 9, "name": "v", "type": "INT"}]}}}
        ],
        "partitionKeys": [],
        "primaryKeys": [],
        "options": {}
    })";
    table_data.schema_id = 5;
    table_data.path = "wh1/db1.db/nested";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["nested"] = table_data;
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         catalog->GetTable(Identifier("db1", "nested")));
    std::shared_ptr<DataSchema> schema =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ(5, schema->Id());
    // 9 lives inside the value row of the map: ROW, ARRAY element and MAP key/value
    // must all be traversed
    ASSERT_EQ(9, schema->HighestFieldId());
    ASSERT_EQ((std::vector<std::string>{"f0", "s", "arr", "m"}), schema->FieldNames());
}

TEST_F(RestCatalogTest, ListTablesPaged) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t1")));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t2")));
    ASSERT_OK(CreateSampleTable(catalog.get(), Identifier("db1", "t3")));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> tables, catalog->ListTables("db1"));
    ASSERT_EQ((std::vector<std::string>{"t1", "t2", "t3"}), tables);
}

TEST_F(RestCatalogTest, BrokenSchemaRejected) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    ExpectBrokenSchemaRejected(catalog.get(), "duplicate",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
                               R"( {"id": 0, "name": "f1", "type": "STRING"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "duplicated");
    // an id inside a nested row colliding with an outer field id is a duplicate too
    ExpectBrokenSchemaRejected(catalog.get(), "duplicate_nested",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"},)"
                               R"( {"id": 1, "name": "s", "type": {"type": "ROW",)"
                               R"( "fields": [{"id": 0, "name": "inner", "type": "INT"}]}}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "duplicated");
    // a field without an integer id must fail the conversion instead of being
    // silently skipped when computing highestFieldId
    ExpectBrokenSchemaRejected(catalog.get(), "no_id",
                               R"({"fields": [{"name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "integer id");
    ExpectBrokenSchemaRejected(catalog.get(), "string_id",
                               R"({"fields": [{"id": "0", "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "integer id");
    // a field that is not an object fails too instead of being silently skipped
    ExpectBrokenSchemaRejected(catalog.get(), "non_object",
                               R"({"fields": [1],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": {}})",
                               "must be an object");
    // a missing or wrong-typed member is a visible failure instead of silently
    // defaulting to an empty value (e.g. loading a partitioned table as
    // unpartitioned)
    ExpectBrokenSchemaRejected(catalog.get(), "missing_partition_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "primaryKeys": [], "options": {}})",
                               "missing 'partitionKeys'");
    ExpectBrokenSchemaRejected(catalog.get(), "missing_primary_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "options": {}})",
                               "missing 'primaryKeys'");
    ExpectBrokenSchemaRejected(catalog.get(), "missing_options",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": []})",
                               "missing 'options'");
    ExpectBrokenSchemaRejected(catalog.get(), "wrong_typed_partition_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": {}, "primaryKeys": [], "options": {}})",
                               "'partitionKeys' is not an array");
    ExpectBrokenSchemaRejected(catalog.get(), "wrong_typed_primary_keys",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": "f0", "options": {}})",
                               "'primaryKeys' is not an array");
    ExpectBrokenSchemaRejected(catalog.get(), "wrong_typed_options",
                               R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                               R"( "partitionKeys": [], "primaryKeys": [], "options": []})",
                               "'options' is not an object");
}

TEST_F(RestCatalogTest, PartitionKeysRoundTrip) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("f0", arrow::int32(), /*nullable=*/false),
                       arrow::field("f1", arrow::utf8(), /*nullable=*/false)});
    struct ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(catalog->CreateTable(Identifier("db1", "pt"), &c_schema,
                                   /*partition_keys=*/{"f1"}, /*primary_keys=*/{}, {},
                                   /*ignore_if_exists=*/false));
    // the partition keys survive both the create request and the load response conversion
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(Identifier("db1", "pt")));
    std::shared_ptr<DataSchema> loaded =
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema());
    ASSERT_NE(nullptr, loaded);
    ASSERT_EQ((std::vector<std::string>{"f1"}), loaded->PartitionKeys());
}

TEST_F(RestCatalogTest, ServerErrorIsPropagatedNotMappedToAbsent) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->force_error_code = 500;
    }
    // a server failure surfaces as an error instead of "does not exist"
    Status db_status = catalog->DatabaseExists("db1").status();
    ASSERT_NOK_WITH_MSG(db_status, "server error");
    Status table_status = catalog->TableExists(Identifier("db1", "t1")).status();
    ASSERT_NOK_WITH_MSG(table_status, "server error");
}

TEST_F(RestCatalogTest, SystemTableChecks) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_NOK_WITH_MSG(catalog->CreateDatabase("sys", {}, false), "system database");
    ASSERT_NOK_WITH_MSG(catalog->DropDatabase("sys", false, false), "system database");
    ASSERT_NOK_WITH_MSG(catalog->DropTable(Identifier("sys", "t"), false), "system table");
    ASSERT_NOK_WITH_MSG(
        catalog->RenameTable(Identifier("db1", "t1$snapshots"), Identifier("db1", "t2"), false),
        "system table");
}

TEST_F(RestCatalogTest, SystemDatabase) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    // the "sys" database and its global system tables are resolved locally without
    // contacting the server, like in FileSystemCatalog
    ASSERT_OK_AND_ASSIGN(bool exists, catalog->DatabaseExists("sys"));
    ASSERT_TRUE(exists);

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> sys_tables, catalog->ListTables("sys"));
    ASSERT_TRUE(std::find(sys_tables.begin(), sys_tables.end(), "tables") != sys_tables.end());

    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("sys", "tables")));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog->TableExists(Identifier("sys", "unsupported")));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> schema,
                         catalog->LoadTableSchema(Identifier("sys", "tables")));
    ASSERT_FALSE(schema->FieldNames().empty());
    Status missing = catalog->LoadTableSchema(Identifier("sys", "unsupported")).status();
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();
}

TEST_F(RestCatalogTest, ListSnapshots) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCatalog> catalog, CreateRestCatalog());
    ASSERT_OK(catalog->CreateDatabase("db1", {}, /*ignore_if_exists=*/false));
    Identifier identifier("db1", "t1");
    ASSERT_OK(CreateSampleTable(catalog.get(), identifier));

    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> snapshots,
                         catalog->ListSnapshots(identifier, ""));
    ASSERT_EQ(2, snapshots.size());
    // fetched via two pages and sorted by snapshot id
    ASSERT_EQ(1, snapshots[0].snapshot_id);
    ASSERT_EQ(2, snapshots[1].snapshot_id);
    ASSERT_EQ("user1", snapshots[0].commit_user);
    ASSERT_EQ(SnapshotInfo::CommitKind::APPEND, snapshots[0].commit_kind);

    // the default branch is addressed as the bare table, so passing it explicitly
    // equals the branch-less call
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> main_snapshots,
                         catalog->ListSnapshots(identifier, "main"));
    ASSERT_EQ(2, main_snapshots.size());
    // the match ignores case, as in the Java client, so "MAIN" is the default branch too
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> main_case_snapshots,
                         catalog->ListSnapshots(identifier, "MAIN"));
    ASSERT_EQ(2, main_case_snapshots.size());

    Status missing = catalog->ListSnapshots(Identifier("db1", "t9"), "").status();
    ASSERT_TRUE(missing.IsNotExist()) << missing.ToString();

    // a non-main branch is sent under its branch object name, so the server resolves
    // the branch and lists its own snapshots
    MockCatalogState::TableData branch_data;
    branch_data.schema_json = R"({"fields": [{"id": 0, "name": "f0", "type": "INT NOT NULL"}],)"
                              R"( "partitionKeys": [], "primaryKeys": [], "options": {}})";
    branch_data.path = "wh1/db1.db/t1";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->databases["db1"]["t1$branch_b1"] = branch_data;
    }
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> branch_snapshots,
                         catalog->ListSnapshots(identifier, "b1"));
    ASSERT_EQ(2, branch_snapshots.size());
    // a branch the server does not know is missing instead of silently falling back
    // to the main table
    Status missing_branch = catalog->ListSnapshots(identifier, "b_missing").status();
    ASSERT_TRUE(missing_branch.IsNotExist()) << missing_branch.ToString();

    // a branch must be passed as the branch argument, not encoded in the identifier
    ASSERT_NOK_WITH_MSG(catalog->ListSnapshots(Identifier("db1", "t1$branch_b1"), "").status(),
                        "branch table");
}

TEST(RestApiErrorTest, ErrorToStatus) {
    RestHttpClient::Response response;
    response.code = 404;
    response.body = R"({"message": "no table", "resourceType": "TABLE", "resourceName": "t1"})";
    response.headers["x-request-id"] = "req-123";
    Status status = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(status.IsNotExist());
    ASSERT_TRUE(status.ToString().find("requestId:req-123") != std::string::npos)
        << status.ToString();

    response.code = 409;
    Status exist_status = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(exist_status.IsExist());

    response.code = 501;
    response.body = "";
    ASSERT_TRUE(RestApi::ErrorToStatus(response).IsNotImplemented());

    // a body that is not an error object at all is reported as such, since the body
    // itself is never echoed
    response.code = 500;
    response.body = "not-a-json";
    Status unparsable = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(unparsable, "server error");
    ASSERT_NOK_WITH_MSG(unparsable, "unparsable error response body (http status 500)");
    ASSERT_EQ(std::string::npos, unparsable.ToString().find("not-a-json")) << unparsable.ToString();

    // the code of the error body wins over the http status when they disagree (e.g. a
    // gateway rewriting the status)
    response.code = 500;
    response.body = R"({"message": "gone", "code": 404})";
    ASSERT_TRUE(RestApi::ErrorToStatus(response).IsNotExist());

    // an error object without a message is told apart from an unparsable body, and the
    // resource info is kept
    response.code = 404;
    response.body = R"({"resourceType": "TABLE", "resourceName": "t1"})";
    Status empty_message = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(empty_message, "empty error message (http status 404)");
    ASSERT_TRUE(empty_message.ToString().find("resource name: t1") != std::string::npos)
        << empty_message.ToString();

    // server messages that may embed secrets are redacted as a whole
    response.code = 400;
    response.body = R"({"message": "bad option password=abc123", "code": 400})";
    Status redacted = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(redacted.IsInvalid()) << redacted.ToString();
    ASSERT_TRUE(redacted.ToString().find("abc123") == std::string::npos) << redacted.ToString();
    ASSERT_TRUE(redacted.ToString().find("******") != std::string::npos) << redacted.ToString();

    // any header carrying a request id is used when x-request-id is absent
    response.code = 404;
    response.body = "";
    response.headers.clear();
    response.headers["x-amz-request-id"] = "amz-1";
    Status fallback = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(fallback.ToString().find("requestId:amz-1") != std::string::npos)
        << fallback.ToString();

    // the "unknown" placeholder is not a real request id
    response.headers.clear();
    response.headers["x-request-id"] = "unknown";
    Status unknown_id = RestApi::ErrorToStatus(response);
    ASSERT_TRUE(unknown_id.ToString().find("requestId") == std::string::npos)
        << unknown_id.ToString();

    // 401/403 map to IOError; the mapped code is carried as a status detail so
    // callers can distinguish them
    response.headers.clear();
    response.code = 401;
    Status not_authorized = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(not_authorized, "not authorized");
    ASSERT_NE(nullptr, not_authorized.detail());
    ASSERT_EQ(std::string(RestErrorDetail::kTypeId), not_authorized.detail()->type_id());
    ASSERT_EQ(401, std::static_pointer_cast<RestErrorDetail>(not_authorized.detail())->GetCode());
    response.code = 403;
    Status forbidden = RestApi::ErrorToStatus(response);
    ASSERT_NOK_WITH_MSG(forbidden, "forbidden");
    ASSERT_EQ(403, std::static_pointer_cast<RestErrorDetail>(forbidden.detail())->GetCode());

    // 503 and the codes without an own mapping (e.g. 429) become IOError with a
    // message naming the code
    response.code = 503;
    ASSERT_NOK_WITH_MSG(RestApi::ErrorToStatus(response), "service unavailable");
    response.code = 429;
    ASSERT_NOK_WITH_MSG(RestApi::ErrorToStatus(response), "rest request failed with code 429");
    response.code = 418;
    ASSERT_NOK_WITH_MSG(RestApi::ErrorToStatus(response), "rest request failed with code 418");
}

TEST(RestApiErrorTest, MalformedSuccessBodyFails) {
    // a 200 response whose body is not the expected json must fail, not crash or
    // return partial data
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             response.body = "not-a-json";
                             return response;
                         }));
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, server->GetBaseUri()},
        {CatalogOptions::TOKEN_PROVIDER, "bear"},
        {CatalogOptions::TOKEN, kToken},
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> api,
                         RestApi::Create(options, "", /*config_required=*/false));
    Status list_status = api->ListDatabases().status();
    ASSERT_NOK(list_status);
    // the body is not echoed into the error: a successful response may contain
    // credentials
    ASSERT_EQ(std::string::npos, list_status.ToString().find("not-a-json"))
        << list_status.ToString();
    ASSERT_NOK(api->GetTable(Identifier("db1", "t1")).status());

    Status config_status = RestApi::Create(options, "", /*config_required=*/true).status();
    ASSERT_NOK(config_status);
    ASSERT_EQ(std::string::npos, config_status.ToString().find("not-a-json"))
        << config_status.ToString();
}

TEST(RestApiErrorTest, PagedListingStopsOnEmptyPageWithToken) {
    // a server that keeps returning a page token with no data must not loop forever
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.body = R"({"databases":[],"nextPageToken":"more"})";
                             return response;
                         }));
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, server->GetBaseUri()},
        {CatalogOptions::TOKEN_PROVIDER, "bear"},
        {CatalogOptions::TOKEN, kToken},
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> api,
                         RestApi::Create(options, "", /*config_required=*/false));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> databases, api->ListDatabases());
    ASSERT_TRUE(databases.empty());
    ASSERT_EQ(1, request_count.load());
}

}  // namespace paimon::test
