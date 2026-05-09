/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/core/catalog/file_system_catalog.h"

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/system_table_schema.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/snapshot/snapshot_info.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileSystemCatalogTest, TestDatabaseExists) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());

    ASSERT_OK_AND_ASSIGN(auto exist, catalog.DatabaseExists("db1"));
    ASSERT_FALSE(exist);

    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/false));
    ASSERT_NOK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));

    ASSERT_OK_AND_ASSIGN(exist, catalog.DatabaseExists("db1"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> db_names, catalog.ListDatabases());
    ASSERT_EQ(1, db_names.size());
    ASSERT_EQ(db_names[0], "db1");
    ASSERT_EQ(catalog.GetDatabaseLocation("db1"), PathUtil::JoinPath(dir->Str(), "db1.db"));
}

TEST(FileSystemCatalogTest, TestInvalidCreateDatabase) {
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_SYSTEM] = "local";
        options[Options::FILE_FORMAT] = "orc";
        options[Catalog::DB_LOCATION_PROP] = "loc";
        ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());

        ASSERT_NOK_WITH_MSG(
            catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true),
            "Cannot specify location for a database when using fileSystem catalog.");
    }
}

TEST(FileSystemCatalogTest, TestCreateSystemDatabaseAndTable) {
    // do not support create system database
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_SYSTEM] = "local";
        options[Options::FILE_FORMAT] = "orc";
        ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
        ASSERT_NOK_WITH_MSG(catalog.CreateDatabase(Catalog::SYSTEM_DATABASE_NAME, options,
                                                   /*ignore_if_exists=*/true),
                            "Cannot create database for system database");
    }
    // do not support create system table
    {
        std::map<std::string, std::string> options;
        options[Options::FILE_SYSTEM] = "local";
        options[Options::FILE_FORMAT] = "orc";
        ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
        ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()),
            arrow::field("f1", arrow::int8()),
            arrow::field("f2", arrow::int8()),
            arrow::field("f3", arrow::int16()),
        };
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        ASSERT_NOK_WITH_MSG(
            catalog.CreateTable(Identifier("db1", "ta$ble"), &schema, {"f1"}, {}, options, false),
            "Cannot create table for system table");
        ArrowSchemaRelease(&schema);
    }
}

TEST(FileSystemCatalogTest, TestCreateTable) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("f8", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    Identifier identifier("db1", "tbl1");
    ASSERT_OK_AND_ASSIGN(auto exist, catalog.TableExists(identifier));
    ASSERT_FALSE(exist);

    ASSERT_OK(catalog.CreateTable(identifier, &schema,
                                  /*partition_keys=*/{"f1", "f2"}, /*primary_keys=*/{"f3"}, options,
                                  false));

    ASSERT_OK_AND_ASSIGN(exist, catalog.TableExists(identifier));
    ASSERT_TRUE(exist);
    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestOptionsSystemTableCatalog) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    options["custom.option"] = "custom-value";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));

    auto typed_schema = arrow::schema({arrow::field("f0", arrow::int32())});
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(*typed_schema, &schema).ok());
    ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema,
                                  /*partition_keys=*/{}, /*primary_keys=*/{}, options,
                                  /*ignore_if_exists=*/false));
    ArrowSchemaRelease(&schema);

    Identifier options_identifier("db1", "tbl1$options");
    ASSERT_OK_AND_ASSIGN(bool exists, catalog.TableExists(options_identifier));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog.TableExists(Identifier("db1", "tbl1$unknown")));
    ASSERT_FALSE(exists);
    ASSERT_OK_AND_ASSIGN(exists, catalog.TableExists(Identifier("db1", "missing$options")));
    ASSERT_FALSE(exists);
    ASSERT_EQ(catalog.GetTableLocation(options_identifier),
              PathUtil::JoinPath(PathUtil::JoinPath(dir->Str(), "db1.db"), "tbl1$options"));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> system_schema,
                         catalog.LoadTableSchema(options_identifier));
    ASSERT_TRUE(std::dynamic_pointer_cast<SystemSchema>(system_schema) != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<SystemTableSchema>(system_schema) != nullptr);
    ASSERT_OK_AND_ASSIGN(auto c_schema, system_schema->GetArrowSchema());
    auto loaded_schema_result = arrow::ImportSchema(c_schema.get());
    ASSERT_TRUE(loaded_schema_result.ok()) << loaded_schema_result.status().ToString();
    auto loaded_schema = loaded_schema_result.ValueUnsafe();
    ASSERT_EQ(loaded_schema->field_names(), (std::vector<std::string>{"key", "value"}));
    ASSERT_EQ(loaded_schema->field(0)->type()->id(), arrow::Type::STRING);
    ASSERT_EQ(loaded_schema->field(1)->type()->id(), arrow::Type::STRING);
    ASSERT_FALSE(loaded_schema->field(0)->nullable());
    ASSERT_FALSE(loaded_schema->field(1)->nullable());

    ASSERT_OK_AND_ASSIGN(auto system_table, catalog.GetTable(options_identifier));
    ASSERT_EQ(system_table->Name(), "tbl1$options");
    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db1", "tbl1$unknown")), "not exist");
    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db1", "missing$options")), "not exist");

    ::ArrowSchema system_create_schema;
    ASSERT_TRUE(arrow::ExportSchema(*typed_schema, &system_create_schema).ok());
    ASSERT_NOK_WITH_MSG(
        catalog.CreateTable(options_identifier, &system_create_schema, {}, {}, options, false),
        "Cannot create table for system table");
    ArrowSchemaRelease(&system_create_schema);
    ASSERT_NOK_WITH_MSG(catalog.DropTable(options_identifier, false), "Cannot drop system table");
    ASSERT_NOK_WITH_MSG(catalog.RenameTable(options_identifier, Identifier("db1", "tbl2"), false),
                        "Cannot rename system table");
}

TEST(FileSystemCatalogTest, TestCreateTableWithBlob) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    options[Options::DATA_EVOLUTION_ENABLED] = "true";
    options[Options::ROW_TRACKING_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));

    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean()),
                                 arrow::field("f1", arrow::int8()),
                                 arrow::field("f2", arrow::int8()),
                                 arrow::field("f3", arrow::int16()),
                                 arrow::field("f4", arrow::int16()),
                                 arrow::field("f5", arrow::int32()),
                                 arrow::field("f6", arrow::int32()),
                                 arrow::field("f7", arrow::int64()),
                                 arrow::field("f8", arrow::int64()),
                                 arrow::field("f9", arrow::float32()),
                                 arrow::field("f10", arrow::float64()),
                                 arrow::field("f11", arrow::utf8()),
                                 arrow::field("f12", arrow::binary()),
                                 arrow::field("non-partition-field", arrow::int32()),
                                 BlobUtils::ToArrowField("f13", /*nullable=*/false)};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema,
                                  /*partition_keys=*/{"f1", "f2"}, /*primary_keys=*/{}, options,
                                  false));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> table_names, catalog.ListTables("db1"));
    ASSERT_EQ(1, table_names.size());
    ASSERT_EQ(table_names[0], "tbl1");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> table_schema,
                         catalog.LoadTableSchema(Identifier("db1", "tbl1")));
    ASSERT_TRUE(std::dynamic_pointer_cast<DataSchema>(table_schema) != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<TableSchema>(table_schema) != nullptr);
    ASSERT_OK_AND_ASSIGN(auto arrow_schema, table_schema->GetArrowSchema());
    auto loaded_schema = arrow::ImportSchema(arrow_schema.get()).ValueOrDie();
    ASSERT_TRUE(typed_schema.Equals(loaded_schema));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog.GetTable(Identifier("db1", "tbl1")));
    ASSERT_OK_AND_ASSIGN(auto arrow_schema_from_get_table, table->LatestSchema()->GetArrowSchema());
    auto schema_from_get_table =
        arrow::ImportSchema(arrow_schema_from_get_table.get()).ValueOrDie();
    ASSERT_TRUE(typed_schema.Equals(schema_from_get_table));
    ASSERT_EQ(table->FullName(), "db1.tbl1");

    ASSERT_NOK_WITH_MSG(catalog.GetTable(Identifier("db1", "table_xaxa")),
                        "Identifier{database='db1', table='table_xaxa'} not exist");

    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestInvalidCreateTable) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::struct_({arrow::field("s1", arrow::int8()),
                                           arrow::field("s1", arrow::int16())}))};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_NOK_WITH_MSG(
        catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f0"}, {"f1"}, options, false),
        "validate schema failed: read schema has duplicate field s1");
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> table_names, catalog.ListTables("db1"));
    ASSERT_EQ(0, table_names.size());
    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestCreateTableWhileDbNotExist) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),
        arrow::field("f3", arrow::int16()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_NOK_WITH_MSG(
        catalog.CreateTable(Identifier("db1", "table"), &schema, {"f1"}, {}, options, false),
        "database db1 is not exist");
    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestCreateTableWhileTableExist) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    {
        ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()),
            arrow::field("f1", arrow::int8()),
            arrow::field("f2", arrow::int8()),
            arrow::field("f3", arrow::int16()),
        };
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
                                      /*ignore_if_exists=*/false));
        ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
                                      /*ignore_if_exists=*/true));
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> table_names, catalog.ListTables("db1"));
        ASSERT_EQ(1, table_names.size());
        ASSERT_EQ(table_names[0], "tbl1");
    }
    {
        ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()),
            arrow::field("f1", arrow::int8()),
            arrow::field("f2", arrow::int8()),
            arrow::field("f3", arrow::int16()),
        };
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
                                      /*ignore_if_exists=*/true));
        ASSERT_NOK_WITH_MSG(
            catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
                                /*ignore_if_exists=*/false),
            "already exist");
        ArrowSchemaRelease(&schema);
    }
    {
        ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()),
            arrow::field("f1", arrow::int8()),
            arrow::field("f2", arrow::int8()),
            arrow::field("f3", arrow::int16()),
        };
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        Identifier identifier("db1", "tbl1");
        ASSERT_OK(catalog.CreateTable(identifier, &schema, {"f1"}, {}, options,
                                      /*ignore_if_exists=*/true));
        ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", dir->Str(), {}));
        ASSERT_OK(fs->Delete(
            PathUtil::JoinPath(catalog.GetTableLocation(identifier), "schema/schema-0")));
        ASSERT_OK(catalog.CreateTable(identifier, &schema, {"f1"}, {}, options,
                                      /*ignore_if_exists=*/false));
    }
}

TEST(FileSystemCatalogTest, TestInvalidList) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_NOK_WITH_MSG(catalog.ListTables("sys"),
                        "do not support listing tables for system database.");
}

TEST(FileSystemCatalogTest, TestValidateTableSchema) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::float64()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    Identifier identifier("db1", "tbl1");
    ASSERT_OK(catalog.CreateTable(identifier, &schema, {"f1"}, {}, options,
                                  /*ignore_if_exists=*/false));

    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db0", "tbl0")),
                        "Identifier{database=\'db0\', table=\'tbl0\'} not exist");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> table_schema, catalog.LoadTableSchema(identifier));
    auto data_schema = std::dynamic_pointer_cast<DataSchema>(table_schema);
    ASSERT_TRUE(data_schema != nullptr);
    ASSERT_EQ(0, data_schema->Id());
    ASSERT_EQ(3, data_schema->HighestFieldId());
    ASSERT_EQ(1, data_schema->PartitionKeys().size());
    ASSERT_EQ(0, data_schema->PrimaryKeys().size());
    ASSERT_EQ(-1, data_schema->NumBuckets());
    ASSERT_FALSE(table_schema->Comment().has_value());
    std::vector<std::string> field_names = table_schema->FieldNames();
    std::vector<std::string> expected_field_names = {"f0", "f1", "f2", "f3"};
    ASSERT_EQ(field_names, expected_field_names);

    FieldType type;
    ASSERT_OK_AND_ASSIGN(type, table_schema->GetFieldType("f0"));
    ASSERT_EQ(type, FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(type, table_schema->GetFieldType("f1"));
    ASSERT_EQ(type, FieldType::INT);
    ASSERT_OK_AND_ASSIGN(type, table_schema->GetFieldType("f2"));
    ASSERT_EQ(type, FieldType::INT);
    ASSERT_OK_AND_ASSIGN(type, table_schema->GetFieldType("f3"));
    ASSERT_EQ(type, FieldType::DOUBLE);
    ASSERT_NOK(table_schema->GetFieldType("f4"));

    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", dir->Str(), {}));
    std::string schema_path =
        PathUtil::JoinPath(catalog.GetTableLocation(identifier), "schema/schema-0");
    std::string expected_json_schema;
    ASSERT_OK(fs->ReadFile(schema_path, &expected_json_schema));

    ASSERT_OK_AND_ASSIGN(auto json_schema, table_schema->GetJsonSchema());
    ASSERT_EQ(expected_json_schema, json_schema);

    ASSERT_OK_AND_ASSIGN(auto arrow_schema, table_schema->GetArrowSchema());
    auto loaded_schema = arrow::ImportSchema(arrow_schema.get()).ValueOrDie();
    ASSERT_TRUE(typed_schema.Equals(loaded_schema));

    ASSERT_OK(fs->Delete(schema_path));
    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(identifier),
                        "Identifier{database=\'db1\', table=\'tbl1\'} not exist");

    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db1", "tbl$11")), "not exist");
    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestDropDatabase) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());

    // Test 1: Drop non-existent database with ignore_if_not_exists=true
    ASSERT_OK(catalog.DropDatabase("non_existent_db", /*ignore_if_not_exists=*/true,
                                   /*cascade=*/false));

    // Test 2: Drop non-existent database with ignore_if_not_exists=false
    ASSERT_NOK_WITH_MSG(catalog.DropDatabase("non_existent_db", /*ignore_if_not_exists=*/false,
                                             /*cascade=*/false),
                        "database non_existent_db does not exist");

    // Test 3: Drop empty database
    ASSERT_OK(catalog.CreateDatabase("test_db", options, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog.DropDatabase("test_db", /*ignore_if_not_exists=*/false,
                                   /*cascade=*/false));
    ASSERT_OK_AND_ASSIGN(bool exist, catalog.DatabaseExists("test_db"));
    ASSERT_FALSE(exist);

    // Test 4: Drop non-empty database without cascade
    ASSERT_OK(catalog.CreateDatabase("test_db2", options, /*ignore_if_exists=*/false));
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK(catalog.CreateTable(Identifier("test_db2", "tbl1"), &schema, {}, {}, options, false));
    ASSERT_NOK_WITH_MSG(catalog.DropDatabase("test_db2", /*ignore_if_not_exists=*/false,
                                             /*cascade=*/false),
                        "Cannot drop non-empty database test_db2. Use cascade=true to force.");

    // Test 5: Drop non-empty database with cascade
    ASSERT_OK(catalog.DropDatabase("test_db2", /*ignore_if_not_exists=*/false,
                                   /*cascade=*/true));
    ASSERT_OK_AND_ASSIGN(exist, catalog.DatabaseExists("test_db2"));
    ASSERT_FALSE(exist);
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> tables, catalog.ListTables("test_db2"));
    ASSERT_TRUE(tables.empty());

    // Test 6: Drop system database
    ASSERT_NOK_WITH_MSG(catalog.DropDatabase(Catalog::SYSTEM_DATABASE_NAME,
                                             /*ignore_if_not_exists=*/false,
                                             /*cascade=*/false),
                        "Cannot drop system database sys.");

    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestDropTable) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("test_db", options, /*ignore_if_exists=*/false));

    // Test 1: Drop non-existent table with ignore_if_not_exists=true
    ASSERT_OK(catalog.DropTable(Identifier("test_db", "non_existent_tbl"),
                                /*ignore_if_not_exists=*/true));

    // Test 2: Drop non-existent table with ignore_if_not_exists=false
    ASSERT_NOK_WITH_MSG(
        catalog.DropTable(Identifier("test_db", "non_existent_tbl"),
                          /*ignore_if_not_exists=*/false),
        "table Identifier{database='test_db', table='non_existent_tbl'} does not exist");

    // Test 3: Drop existing table
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK(catalog.CreateTable(Identifier("test_db", "tbl1"), &schema, {}, {}, options, false));
    ASSERT_OK(catalog.DropTable(Identifier("test_db", "tbl1"), /*ignore_if_not_exists=*/false));
    ASSERT_OK_AND_ASSIGN(bool exist, catalog.TableExists(Identifier("test_db", "tbl1")));
    ASSERT_FALSE(exist);

    // Test 4: Drop system table
    ASSERT_NOK_WITH_MSG(
        catalog.DropTable(Identifier("test_db", "tbl$system"),
                          /*ignore_if_not_exists=*/false),
        "Cannot drop system table Identifier{database='test_db', table='tbl$system'}.");

    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestRenameTable) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("test_db", options, /*ignore_if_exists=*/false));

    // Test 1: Rename non-existent table with ignore_if_not_exists=true
    ASSERT_OK(catalog.RenameTable(Identifier("test_db", "non_existent_tbl"),
                                  Identifier("test_db", "new_tbl"),
                                  /*ignore_if_not_exists=*/true));

    // Test 2: Rename non-existent table with ignore_if_not_exists=false
    ASSERT_NOK_WITH_MSG(
        catalog.RenameTable(Identifier("test_db", "non_existent_tbl"),
                            Identifier("test_db", "new_tbl"),
                            /*ignore_if_not_exists=*/false),
        "source table Identifier{database='test_db', table='non_existent_tbl'} does not exist");

    // Test 3: Normal rename
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema1;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema1).ok());
    ASSERT_OK(
        catalog.CreateTable(Identifier("test_db", "old_tbl"), &schema1, {}, {}, options, false));
    ASSERT_OK(catalog.RenameTable(Identifier("test_db", "old_tbl"),
                                  Identifier("test_db", "new_tbl"),
                                  /*ignore_if_not_exists=*/false));
    ASSERT_OK_AND_ASSIGN(bool old_exist, catalog.TableExists(Identifier("test_db", "old_tbl")));
    ASSERT_FALSE(old_exist);
    ASSERT_OK_AND_ASSIGN(bool new_exist, catalog.TableExists(Identifier("test_db", "new_tbl")));
    ASSERT_TRUE(new_exist);

    // Test 4: Rename to existing table
    ::ArrowSchema schema2;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema2).ok());
    ASSERT_OK(catalog.CreateTable(Identifier("test_db", "tbl2"), &schema2, {}, {}, options, false));
    ASSERT_NOK_WITH_MSG(
        catalog.RenameTable(Identifier("test_db", "new_tbl"), Identifier("test_db", "tbl2"),
                            /*ignore_if_not_exists=*/false),
        "target table Identifier{database='test_db', table='tbl2'} already exists");

    // Test 5: Cross-database rename
    ASSERT_OK(catalog.CreateDatabase("test_db2", options, /*ignore_if_exists=*/false));
    ASSERT_NOK_WITH_MSG(
        catalog.RenameTable(Identifier("test_db", "new_tbl"),
                            Identifier("test_db2", "cross_db_tbl"),
                            /*ignore_if_not_exists=*/false),
        "Cannot rename table across databases. Cross-database rename is not supported.");

    // Test 6: Rename system table
    ASSERT_NOK_WITH_MSG(catalog.RenameTable(Identifier("test_db", "tbl$system"),
                                            Identifier("test_db", "new_system_tbl"),
                                            /*ignore_if_not_exists=*/false),
                        "Cannot rename system table");

    ArrowSchemaRelease(&schema1);
    ArrowSchemaRelease(&schema2);
}

TEST(FileSystemCatalogTest, TestDropTableWithExternalPath) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("test_db", options, /*ignore_if_exists=*/false));

    // Create external path directory
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_path = external_dir->Str();

    // Create a file in external path to simulate external data
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", dir->Str(), {}));
    std::string external_data_file = PathUtil::JoinPath(external_path, "data-file.parquet");
    ASSERT_OK(fs->WriteFile(external_data_file, "test data", /*overwrite=*/true));

    // Verify external data file exists
    ASSERT_OK_AND_ASSIGN(bool external_file_exists, fs->Exists(external_data_file));
    ASSERT_TRUE(external_file_exists);

    // Create table with external path
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    std::map<std::string, std::string> table_options = options;
    table_options[Options::DATA_FILE_EXTERNAL_PATHS] = external_path;

    ASSERT_OK(catalog.CreateTable(Identifier("test_db", "tbl_with_external"), &schema, {}, {},
                                  table_options, false));

    // Verify table exists
    ASSERT_OK_AND_ASSIGN(bool table_exists,
                         catalog.TableExists(Identifier("test_db", "tbl_with_external")));
    ASSERT_TRUE(table_exists);

    // Drop the table
    ASSERT_OK(catalog.DropTable(Identifier("test_db", "tbl_with_external"),
                                /*ignore_if_not_exists=*/false));

    // Verify table is dropped
    ASSERT_OK_AND_ASSIGN(table_exists,
                         catalog.TableExists(Identifier("test_db", "tbl_with_external")));
    ASSERT_FALSE(table_exists);

    // Verify external path is also cleaned up
    ASSERT_OK_AND_ASSIGN(external_file_exists, fs->Exists(external_path));
    ASSERT_FALSE(external_file_exists);

    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestDropTableWithMultipleExternalPaths) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());
    ASSERT_OK(catalog.CreateDatabase("test_db", options, /*ignore_if_exists=*/false));

    // Create multiple external path directories
    auto external_dir1 = UniqueTestDirectory::Create();
    auto external_dir2 = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir1);
    ASSERT_TRUE(external_dir2);

    std::string external_path1 = external_dir1->Str();
    std::string external_path2 = external_dir2->Str();

    // Create files in external paths
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", dir->Str(), {}));
    std::string external_data_file1 = PathUtil::JoinPath(external_path1, "data-1.parquet");
    std::string external_data_file2 = PathUtil::JoinPath(external_path2, "data-2.parquet");
    ASSERT_OK(fs->WriteFile(external_data_file1, "test data 1", /*overwrite=*/true));
    ASSERT_OK(fs->WriteFile(external_data_file2, "test data 2", /*overwrite=*/true));

    // Create table with multiple external paths
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
    };
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    std::map<std::string, std::string> table_options = options;
    table_options[Options::DATA_FILE_EXTERNAL_PATHS] = external_path1 + "," + external_path2;

    ASSERT_OK(catalog.CreateTable(Identifier("test_db", "tbl_multi_external"), &schema, {}, {},
                                  table_options, false));

    // Drop the table
    ASSERT_OK(catalog.DropTable(Identifier("test_db", "tbl_multi_external"),
                                /*ignore_if_not_exists=*/false));

    // Verify both external paths are cleaned up
    ASSERT_OK_AND_ASSIGN(bool exists1, fs->Exists(external_path1));
    ASSERT_OK_AND_ASSIGN(bool exists2, fs->Exists(external_path2));
    ASSERT_FALSE(exists1);
    ASSERT_FALSE(exists2);

    ArrowSchemaRelease(&schema);
}

TEST(FileSystemCatalogTest, TestListSnapshots) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::string test_data_path = GetDataDir() + "/append_table_with_multiple_file_format.db";
    std::string db_path = dir->Str() + "/test_db.db";
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, db_path));

    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());

    Identifier id("test_db", "append_table_with_multiple_file_format");
    ASSERT_OK_AND_ASSIGN(std::vector<SnapshotInfo> snapshots, catalog.ListSnapshots(id, ""));

    ASSERT_EQ(snapshots.size(), 2);

    ASSERT_EQ(snapshots[0].snapshot_id, 1);
    ASSERT_EQ(snapshots[0].schema_id, 0);
    ASSERT_EQ(snapshots[0].commit_kind, SnapshotInfo::CommitKind::APPEND);
    ASSERT_EQ(snapshots[0].time_millis, 1755671728191);
    ASSERT_EQ(snapshots[0].total_record_count, 5);
    ASSERT_EQ(snapshots[0].delta_record_count, 5);
    ASSERT_FALSE(snapshots[0].watermark.has_value());

    ASSERT_EQ(snapshots[1].snapshot_id, 2);
    ASSERT_EQ(snapshots[1].schema_id, 1);
    ASSERT_EQ(snapshots[1].commit_kind, SnapshotInfo::CommitKind::APPEND);
    ASSERT_EQ(snapshots[1].time_millis, 1755671956423);
    ASSERT_EQ(snapshots[1].total_record_count, 7);
    ASSERT_EQ(snapshots[1].delta_record_count, 2);
    ASSERT_FALSE(snapshots[1].watermark.has_value());

    // Verify ascending order by snapshot_id
    ASSERT_LT(snapshots[0].snapshot_id, snapshots[1].snapshot_id);
}

TEST(FileSystemCatalogTest, TestListSnapshotsTableNotExist) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());

    ASSERT_NOK_WITH_MSG(
        catalog.ListSnapshots(Identifier("non_existent_db", "non_existent_table"), ""),
        "does not exist");
}

}  // namespace paimon::test
