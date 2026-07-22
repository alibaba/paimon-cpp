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

#include "paimon/rest/rest_messages.h"

#include <map>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/document.h"

namespace paimon::test {

TEST(RestMessagesTest, ErrorResponseRoundTrip) {
    ErrorResponse response(ErrorResponse::kResourceTypeDatabase, "db1", "database db1 not found",
                           404);
    ASSERT_OK_AND_ASSIGN(std::string json, response.ToJsonString());
    ASSERT_OK_AND_ASSIGN(ErrorResponse parsed, ErrorResponse::FromJsonString(json));
    ASSERT_EQ("DATABASE", parsed.GetResourceType());
    ASSERT_EQ("db1", parsed.GetResourceName());
    ASSERT_EQ("database db1 not found", parsed.GetMessage());
    ASSERT_EQ(404, parsed.GetCode());
}

TEST(RestMessagesTest, ErrorResponseLenientParse) {
    ASSERT_OK_AND_ASSIGN(ErrorResponse parsed, ErrorResponse::FromJsonString("{}"));
    ASSERT_EQ("", parsed.GetMessage());
    ASSERT_EQ(0, parsed.GetCode());
}

TEST(RestMessagesTest, ConfigResponseMerge) {
    // null values may be sent by the server: a null default carries nothing to merge,
    // a null override unsets the key
    std::string json = R"({
        "defaults": {"prefix": "server-prefix", "a": "default-a", "b": "default-b",
                     "nullable": null, "unset-default": "default-value"},
        "overrides": {"c": "override-c", "a": "override-a",
                      "unset-client": null, "unset-default": null, "unset-absent": null}
    })";
    ASSERT_OK_AND_ASSIGN(ConfigResponse config, ConfigResponse::FromJsonString(json));
    std::map<std::string, std::string> client = {
        {"a", "client-a"}, {"b", "client-b"}, {"d", "client-d"}, {"unset-client", "client-value"}};
    std::map<std::string, std::string> merged = config.Merge(client);
    // overrides > client options > defaults
    ASSERT_EQ("override-a", merged["a"]);
    ASSERT_EQ("client-b", merged["b"]);
    ASSERT_EQ("override-c", merged["c"]);
    ASSERT_EQ("client-d", merged["d"]);
    ASSERT_EQ("server-prefix", merged["prefix"]);
    ASSERT_EQ(0, merged.count("nullable"));
    // a null override beats the client option and the default; on an unknown key it is
    // a no-op
    ASSERT_EQ(0, merged.count("unset-client"));
    ASSERT_EQ(0, merged.count("unset-default"));
    ASSERT_EQ(0, merged.count("unset-absent"));
}

TEST(RestMessagesTest, ListResponsesParse) {
    ASSERT_OK_AND_ASSIGN(ListDatabasesResponse databases,
                         ListDatabasesResponse::FromJsonString(
                             R"({"databases": ["db1", "db2"], "nextPageToken": "token1"})"));
    ASSERT_EQ((std::vector<std::string>{"db1", "db2"}), databases.Data());
    ASSERT_EQ("token1", databases.NextPageToken().value());

    ASSERT_OK_AND_ASSIGN(ListTablesResponse tables,
                         ListTablesResponse::FromJsonString(R"({"tables": ["t1"]})"));
    ASSERT_EQ((std::vector<std::string>{"t1"}), tables.Data());
    ASSERT_FALSE(tables.NextPageToken().has_value());

    // nextPageToken may be serialized as explicit null by the server
    ASSERT_OK_AND_ASSIGN(
        ListTablesResponse null_token,
        ListTablesResponse::FromJsonString(R"({"tables": [], "nextPageToken": null})"));
    ASSERT_FALSE(null_token.NextPageToken().has_value());
}

TEST(RestMessagesTest, ListSnapshotsResponseParse) {
    std::string json = R"({
        "snapshots": [{
            "version": 3,
            "id": 7,
            "schemaId": 2,
            "baseManifestList": "manifest-list-1",
            "deltaManifestList": "manifest-list-2",
            "commitUser": "user1",
            "commitIdentifier": 9,
            "commitKind": "APPEND",
            "timeMillis": 1234567,
            "totalRecordCount": 100,
            "deltaRecordCount": 10,
            "watermark": 42
        }, {
            "version": 3,
            "id": 8,
            "schemaId": 2,
            "baseManifestList": "manifest-list-3",
            "deltaManifestList": "manifest-list-4",
            "commitUser": "user1",
            "commitIdentifier": 10,
            "commitKind": "COMPACT",
            "timeMillis": 1234568,
            "totalRecordCount": 100,
            "deltaRecordCount": 0
        }],
        "nextPageToken": null
    })";
    ASSERT_OK_AND_ASSIGN(ListSnapshotsResponse response,
                         ListSnapshotsResponse::FromJsonString(json));
    ASSERT_EQ(2, response.Data().size());
    const Snapshot& snapshot = response.Data()[0];
    ASSERT_EQ(7, snapshot.Id());
    ASSERT_EQ(2, snapshot.SchemaId());
    ASSERT_EQ("user1", snapshot.CommitUser());
    ASSERT_EQ(1234567, snapshot.TimeMillis());
    SnapshotInfo info = snapshot.ToSnapshotInfo();
    ASSERT_EQ(SnapshotInfo::CommitKind::APPEND, info.commit_kind);
    ASSERT_EQ(100, info.total_record_count.value());
    ASSERT_EQ(42, info.watermark.value());
    SnapshotInfo compact_info = response.Data()[1].ToSnapshotInfo();
    ASSERT_EQ(SnapshotInfo::CommitKind::COMPACT, compact_info.commit_kind);
    ASSERT_FALSE(compact_info.watermark.has_value());
}

TEST(RestMessagesTest, GetDatabaseResponseParse) {
    std::string json = R"({
        "id": "10",
        "name": "db1",
        "location": "/warehouse/db1.db",
        "options": {"k1": "v1"},
        "owner": "owner1",
        "createdAt": 100,
        "createdBy": "creator",
        "updatedAt": 200,
        "updatedBy": "updater"
    })";
    ASSERT_OK_AND_ASSIGN(GetDatabaseResponse response, GetDatabaseResponse::FromJsonString(json));
    ASSERT_EQ("db1", response.GetName());
    ASSERT_EQ("/warehouse/db1.db", response.GetLocation());
    ASSERT_EQ("v1", response.GetOptions().at("k1"));
    std::map<std::string, std::string> options;
    response.GetAuditFields().PutAuditOptionsTo(&options);
    ASSERT_EQ("owner1", options.at("owner"));
    ASSERT_EQ("100", options.at("createdAt"));
    ASSERT_EQ("updater", options.at("updatedBy"));
}

TEST(RestMessagesTest, GetTableResponseParse) {
    std::string json = R"({
        "id": "42",
        "database": "db1",
        "name": "t1",
        "path": "/warehouse/db1.db/t1",
        "isExternal": false,
        "schemaId": 3,
        "schema": {
            "fields": [
                {"id": 0, "name": "f0", "type": "INT NOT NULL"},
                {"id": 1, "name": "f1", "type": "STRING"}
            ],
            "partitionKeys": [],
            "primaryKeys": ["f0"],
            "options": {"bucket": "2"},
            "comment": "a table"
        },
        "updatedAt": 300
    })";
    ASSERT_OK_AND_ASSIGN(GetTableResponse response, GetTableResponse::FromJsonString(json));
    ASSERT_EQ("42", response.GetId());
    ASSERT_EQ("db1", response.GetDatabase());
    ASSERT_EQ("t1", response.GetName());
    ASSERT_EQ("/warehouse/db1.db/t1", response.GetPath());
    ASSERT_FALSE(response.IsExternal());
    ASSERT_EQ(3, response.GetSchemaId());
    ASSERT_EQ(300, response.GetAuditFields().updated_at.value());
    rapidjson::Document schema;
    schema.Parse(response.GetSchemaJson().c_str());
    ASSERT_FALSE(schema.HasParseError());
    ASSERT_EQ(2u, schema["fields"].Size());
    ASSERT_STREQ("f0", schema["fields"][0]["name"].GetString());
}

TEST(RestMessagesTest, UnknownFieldsAreIgnored) {
    // forward compatibility: fields a newer server adds must be ignored
    std::string json = R"({
        "id": "42", "name": "t1", "path": "p", "schemaId": 3,
        "schema": {"fields": []},
        "futureField": {"nested": true}, "anotherUnknown": [1, 2]
    })";
    ASSERT_OK_AND_ASSIGN(GetTableResponse response, GetTableResponse::FromJsonString(json));
    ASSERT_EQ("t1", response.GetName());
    ASSERT_EQ(3, response.GetSchemaId());
    // the endpoints addressing a table by database and name report no "database"
    ASSERT_EQ("", response.GetDatabase());
}

TEST(RestMessagesTest, MissingRequiredFieldsFail) {
    // "path", "schemaId" and "schema" are required
    ASSERT_NOK(GetTableResponse::FromJsonString(R"({"id": "42", "name": "t1"})").status());
    // a non-object identifier is rejected
    CreateTableRequest bad_identifier("", "", "");
    ASSERT_NOK(RapidJsonUtil::FromJsonString(R"({"identifier": "not-an-object", "schema": {}})",
                                             &bad_identifier));
}

TEST(RestMessagesTest, CreateTableRequestSerialize) {
    CreateTableRequest request(
        "db1", "t1", R"({"fields": [], "partitionKeys": [], "primaryKeys": [], "options": {}})");
    ASSERT_OK_AND_ASSIGN(std::string json, request.ToJsonString());
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    ASSERT_FALSE(doc.HasParseError());
    ASSERT_STREQ("db1", doc["identifier"]["database"].GetString());
    ASSERT_STREQ("t1", doc["identifier"]["object"].GetString());
    ASSERT_TRUE(doc["schema"].IsObject());
    ASSERT_TRUE(doc["schema"]["fields"].IsArray());

    ASSERT_OK_AND_ASSIGN(CreateTableRequest parsed, CreateTableRequest::FromJsonString(json));
    ASSERT_EQ("db1", parsed.GetDatabase());
    ASSERT_EQ("t1", parsed.GetTable());
}

TEST(RestMessagesTest, InvalidSchemaJsonErrorOmitsPayload) {
    CreateTableRequest request("db1", "t1", R"({"fields": [], "options": {"token": "top-secret")");
    Status status = request.ToJsonString().status();
    ASSERT_NOK_WITH_MSG(status, "invalid json");
    // the payload may carry credentials, so it must not be echoed back in the error
    ASSERT_EQ(std::string::npos, status.ToString().find("top-secret")) << status.ToString();
}

TEST(RestMessagesTest, RenameTableRequestSerialize) {
    RenameTableRequest request("db1", "t1", "db1", "t2");
    ASSERT_OK_AND_ASSIGN(std::string json, request.ToJsonString());
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    ASSERT_FALSE(doc.HasParseError());
    ASSERT_STREQ("t1", doc["source"]["object"].GetString());
    ASSERT_STREQ("t2", doc["destination"]["object"].GetString());

    ASSERT_OK_AND_ASSIGN(RenameTableRequest parsed, RenameTableRequest::FromJsonString(json));
    ASSERT_EQ("db1", parsed.GetSourceDatabase());
    ASSERT_EQ("t2", parsed.GetDestinationTable());
}

TEST(RestMessagesTest, CreateDatabaseRequestRoundTrip) {
    CreateDatabaseRequest request("db1", {{"k1", "v1"}});
    ASSERT_OK_AND_ASSIGN(std::string json, request.ToJsonString());
    ASSERT_OK_AND_ASSIGN(CreateDatabaseRequest parsed, CreateDatabaseRequest::FromJsonString(json));
    ASSERT_EQ("db1", parsed.GetName());
    ASSERT_EQ("v1", parsed.GetOptions().at("k1"));
}

}  // namespace paimon::test
