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
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileSystemCatalogTest, TestDataBaseExists) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto core_options, CoreOptions::FromMap(options));
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    FileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str());

    ASSERT_OK_AND_ASSIGN(auto exist, catalog.DataBaseExists("db1"));
    ASSERT_FALSE(exist);

    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/false));
    ASSERT_NOK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));

    ASSERT_OK_AND_ASSIGN(exist, catalog.DataBaseExists("db1"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> db_names, catalog.ListDatabases());
    ASSERT_EQ(1, db_names.size());
    ASSERT_EQ(db_names[0], "db1");
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
    ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema,
                                  /*partition_keys=*/{"f1", "f2"}, /*primary_keys=*/{"f3"}, options,
                                  false));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> table_names, catalog.ListTables("db1"));
    ASSERT_EQ(1, table_names.size());
    ASSERT_EQ(table_names[0], "tbl1");
    ArrowSchemaRelease(&schema);
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
    ASSERT_OK_AND_ASSIGN(auto arrow_schema, table_schema->GetArrowSchema());
    auto loaded_schema = arrow::ImportSchema(arrow_schema.get()).ValueOrDie();
    ASSERT_TRUE(typed_schema.Equals(loaded_schema));
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
        ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
                                      /*ignore_if_exists=*/true));
        ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", dir->Str(), {}));
        ASSERT_OK(fs->Delete(PathUtil::JoinPath(dir->Str(), "db1.db/tbl1/schema/schema-0")));
        ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
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
    ASSERT_OK(catalog.CreateTable(Identifier("db1", "tbl1"), &schema, {"f1"}, {}, options,
                                  /*ignore_if_exists=*/false));

    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db0", "tbl0")),
                        "Identifier{database=\'db0\', table=\'tbl0\'} not exist");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Schema> table_schema,
                         catalog.LoadTableSchema(Identifier("db1", "tbl1")));
    ASSERT_EQ(0, table_schema->Id());
    ASSERT_EQ(3, table_schema->HighestFieldId());
    ASSERT_EQ(1, table_schema->PartitionKeys().size());
    ASSERT_EQ(0, table_schema->PrimaryKeys().size());
    ASSERT_EQ(-1, table_schema->NumBuckets());
    ASSERT_FALSE(table_schema->Comment().has_value());
    std::vector<std::string> field_names = table_schema->FieldNames();
    std::vector<std::string> expected_field_names = {"f0", "f1", "f2", "f3"};
    ASSERT_EQ(field_names, expected_field_names);

    ASSERT_OK_AND_ASSIGN(auto arrow_schema, table_schema->GetArrowSchema());
    auto loaded_schema = arrow::ImportSchema(arrow_schema.get()).ValueOrDie();
    ASSERT_TRUE(typed_schema.Equals(loaded_schema));

    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", dir->Str(), {}));
    ASSERT_OK(fs->Delete(PathUtil::JoinPath(dir->Str(), "db1.db/tbl1/schema/schema-0")));

    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db1", "tbl1")),
                        "Identifier{database=\'db1\', table=\'tbl1\'} not exist");

    ASSERT_NOK_WITH_MSG(catalog.LoadTableSchema(Identifier("db1", "tbl$11")),
                        "do not support checking TableSchemaExists for system table.");
    ArrowSchemaRelease(&schema);
}

}  // namespace paimon::test
