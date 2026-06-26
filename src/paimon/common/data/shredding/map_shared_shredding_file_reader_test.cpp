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

#include "paimon/common/data/shredding/map_shared_shredding_file_reader.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/core/append/append_only_writer.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_file_format_factory.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class MapSharedShreddingFileReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    static MapSharedShreddingFieldMeta TagsMeta() {
        MapSharedShreddingFieldMeta meta;
        meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}, {"d", 3}, {"e", 4}};
        meta.field_to_columns = {{0, {0, 1}}, {1, {1}}, {2, {0}}, {3, {0}}, {4, {1}}};
        meta.overflow_field_set = {0, 2};
        meta.num_columns = 2;
        meta.max_row_width = 4;
        return meta;
    }

    std::shared_ptr<arrow::Schema> PhysicalSchemaWithMetadata() const {
        return PhysicalSchemaWithMetadata(TagsMeta());
    }

    std::shared_ptr<arrow::Schema> PhysicalSchemaWithMetadata(
        const MapSharedShreddingFieldMeta& meta) const {
        std::map<std::string, int32_t> field_to_num_columns = {{"tags", 2}};
        auto physical_schema =
            MapSharedShreddingUtils::LogicalToPhysicalSchema(logical_schema_, field_to_num_columns);
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        EXPECT_OK(MapSharedShreddingUtils::SerializeMetadata(
            meta, MapSharedShreddingDefine::kDefaultDictCompression, metadata.get()));

        arrow::FieldVector fields = physical_schema->fields();
        fields[1] = fields[1]->WithMetadata(metadata);
        return arrow::schema(std::move(fields));
    }

    std::shared_ptr<arrow::Array> PhysicalArray() const {
        std::shared_ptr<arrow::Schema> physical_schema = PhysicalSchemaWithMetadata();
        std::string json = R"([
            [1, [[0, 1], 10, 20, null]],
            [2, [[2, 0], 30, 40, null]],
            [3, null],
            [4, [[3, 4], 60, 70, [[0, 80], [2, null]]]]
        ])";
        return arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(physical_schema->fields()),
                                                         json)
            .ValueOrDie();
    }

    std::unique_ptr<MapSharedShreddingFileReader> WrapReader(
        std::unique_ptr<FileBatchReader>&& reader,
        const std::optional<std::string>& selected_keys_str = std::nullopt) const {
        EXPECT_OK_AND_ASSIGN(auto c_file_schema, reader->GetFileSchema());
        auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
        std::map<std::string, MapSharedShreddingFileReader::SharedShreddingContext>
            shared_shredding_name_to_context;
        for (const auto& field : file_schema->fields()) {
            auto metadata = std::const_pointer_cast<arrow::KeyValueMetadata>(field->metadata());
            if (!MapSharedShreddingUtils::HasShreddingMetadata(metadata)) {
                continue;
            }
            EXPECT_OK_AND_ASSIGN(auto meta,
                                 MapSharedShreddingUtils::DeserializeMetadata(
                                     metadata, MapSharedShreddingDefine::kDefaultDictCompression));
            auto physical_type =
                arrow::internal::checked_pointer_cast<arrow::StructType>(field->type());
            std::shared_ptr<arrow::Field> item_field;
            for (const auto& child : physical_type->fields()) {
                if (child->name() != MapSharedShreddingDefine::kFieldMapping &&
                    child->name() != MapSharedShreddingDefine::kOverflow) {
                    item_field = child;
                    break;
                }
            }
            EXPECT_TRUE(item_field);
            auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(arrow::map(
                arrow::utf8(), arrow::field("value", item_field->type(), item_field->nullable())));
            std::vector<std::string> selected_keys;
            if (selected_keys_str.has_value()) {
                selected_keys = StringUtils::Split(selected_keys_str.value(), ",",
                                                   /*ignore_empty=*/false);
            } else {
                selected_keys.reserve(meta.name_to_id.size());
                for (const auto& [key_name, _] : meta.name_to_id) {
                    selected_keys.push_back(key_name);
                }
            }
            shared_shredding_name_to_context.emplace(
                field->name(), MapSharedShreddingFileReader::SharedShreddingContext(
                                   meta, selected_keys, map_type));
        }
        return std::make_unique<MapSharedShreddingFileReader>(
            std::move(reader), std::move(shared_shredding_name_to_context), pool_);
    }

    std::unique_ptr<MapSharedShreddingFileReader> CreateReader(
        std::shared_ptr<arrow::Array> physical_array = nullptr,
        std::shared_ptr<arrow::Schema> physical_schema = nullptr,
        const std::optional<std::string>& selected_keys = std::nullopt) const {
        if (!physical_schema) {
            physical_schema = PhysicalSchemaWithMetadata();
        }
        if (!physical_array) {
            physical_array = PhysicalArray();
        }
        auto mock_reader = std::make_unique<MockFileBatchReader>(
            physical_array, arrow::struct_(physical_schema->fields()), /*read_batch_size=*/10);
        mock_reader->EnableRandomizeBatchSize(false);
        return WrapReader(std::move(mock_reader), selected_keys);
    }

    std::shared_ptr<arrow::Schema> ReadSchema(
        const std::optional<std::string>& selected_keys) const {
        arrow::FieldVector fields = logical_schema_->fields();
        if (selected_keys) {
            auto metadata = std::make_shared<arrow::KeyValueMetadata>();
            metadata->Append("paimon.map.selected-keys", *selected_keys);
            fields[1] = fields[1]->WithMetadata(metadata);
        }
        return arrow::schema(std::move(fields));
    }

    std::unique_ptr<ArrowSchema> ExportSchema(const std::shared_ptr<arrow::Schema>& schema) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    std::unique_ptr<RecordBatch> CreateBatch(const std::shared_ptr<arrow::Schema>& schema,
                                             const std::string& json) const {
        auto array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
                .ValueOrDie();
        ::ArrowArray arrow_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        EXPECT_OK_AND_ASSIGN(auto batch, batch_builder.Finish());
        return batch;
    }

    void AssertChunkedArrayEquals(const std::shared_ptr<arrow::ChunkedArray>& expected,
                                  const std::shared_ptr<arrow::ChunkedArray>& actual) const {
        ASSERT_TRUE(expected->Equals(actual)) << "Expected:\n"
                                              << expected->ToString() << "\nActual:\n"
                                              << actual->ToString();
    }

    std::shared_ptr<DataFilePathFactory> CreatePathFactory(const std::string& dir,
                                                           const std::string& format,
                                                           const CoreOptions& options) const {
        auto path_factory = std::make_shared<DataFilePathFactory>();
        EXPECT_OK(path_factory->Init(dir, format, options.DataFilePrefix(), nullptr));
        return path_factory;
    }

    std::unique_ptr<FileBatchReader> OpenFormatReader(
        const std::string& file_path, const std::string& format,
        const std::map<std::string, std::string>& options = {}) const {
        auto fs = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs->Open(file_path));
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get(format, options));
        EXPECT_OK_AND_ASSIGN(auto reader_builder,
                             file_format->CreateReaderBuilder(/*batch_size=*/10));
        return reader_builder->Build(input_stream).value();
    }

    Result<std::unique_ptr<AppendOnlyWriter>> CreateAppendOnlyWriter(
        const CoreOptions& core_options, int64_t schema_id,
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::optional<std::vector<std::string>>& write_cols, int64_t max_sequence_number,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<CompactManager>& compact_manager) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<MapSharedShreddingContext> shredding_context,
            MapSharedShreddingUtils::CreateShreddingContext(logical_schema, core_options));
        return std::make_unique<AppendOnlyWriter>(core_options, schema_id, logical_schema,
                                                  write_cols, max_sequence_number, path_factory,
                                                  compact_manager, shredding_context, pool_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> logical_schema_ = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    std::map<std::string, std::string> options_ = {
        {Options::FILE_SYSTEM, "local"},
        {Options::FILE_FORMAT, "mock_format"},
        {Options::MANIFEST_FORMAT, "mock_format"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "2"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {Options::WRITE_ONLY, "true"},
    };
};

TEST_F(MapSharedShreddingFileReaderTest, TestGetFileSchemaReturnsLogicalMapSchema) {
    auto reader = CreateReader();

    ASSERT_OK_AND_ASSIGN(auto c_schema, reader->GetFileSchema());
    auto schema = arrow::ImportSchema(c_schema.get()).ValueOrDie();

    ASSERT_TRUE(schema->Equals(logical_schema_, /*check_metadata=*/false))
        << "Expected:\n"
        << logical_schema_->ToString() << "\nActual:\n"
        << schema->ToString();
    ASSERT_FALSE(schema->field(1)->HasMetadata());
}

TEST_F(MapSharedShreddingFileReaderTest, TestAllExistSelectedKeysWithoutOverflow) {
    auto reader = CreateReader(/*physical_array=*/nullptr, /*physical_schema=*/nullptr,
                               /*selected_keys=*/"b");
    auto read_schema = ExportSchema(ReadSchema("b"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema_->fields()), {R"([
                        [1, [["b", 20]]],
                        [2, []],
                        [3, null],
                        [4, []]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestAllExistSelectedKeysWithOverflow) {
    auto reader = CreateReader(/*physical_array=*/nullptr, /*physical_schema=*/nullptr,
                               /*selected_keys=*/"a,c");
    auto read_schema = ExportSchema(ReadSchema("a,c"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema_->fields()), {R"([
                        [1, [["a", 10]]],
                        [2, [["a", 40], ["c", 30]]],
                        [3, null],
                        [4, [["a", 80], ["c", null]]]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestPartialExistSelectedKeys) {
    auto reader = CreateReader(/*physical_array=*/nullptr, /*physical_schema=*/nullptr,
                               /*selected_keys=*/"a,c,missing");
    auto read_schema = ExportSchema(ReadSchema("a,c,missing"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema_->fields()), {R"([
                        [1, [["a", 10]]],
                        [2, [["a", 40], ["c", 30]]],
                        [3, null],
                        [4, [["a", 80], ["c", null]]]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestMissingSelectedKeysReadsWholeMap) {
    auto reader = CreateReader();
    auto read_schema = ExportSchema(ReadSchema(std::nullopt));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema_->fields()), {R"([
                        [1, [["a", 10], ["b", 20]]],
                        [2, [["a", 40], ["c", 30]]],
                        [3, null],
                        [4, [["a", 80], ["c", null], ["d", 60], ["e", 70]]]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestSpecialSelectedKeys) {
    MapSharedShreddingFieldMeta meta;
    meta.name_to_id = {{"", 0}, {"     ", 1}, {".", 2}, {"a", 3}};
    meta.field_to_columns = {{0, {0}}, {1, {1}}, {2, {0}}, {3, {1}}};
    meta.num_columns = 2;
    meta.max_row_width = 2;
    auto physical_schema = PhysicalSchemaWithMetadata(meta);
    std::string json = R"([
        [1, [[0, 1], 10, 20, null]],
        [2, [[2, 3], 30, 40, null]],
        [3, null]
    ])";
    auto physical_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(physical_schema->fields()), json)
            .ValueOrDie();

    auto assert_read = [&](const std::string& selected_keys, const std::string& expected_json) {
        auto reader = CreateReader(physical_array, physical_schema, selected_keys);
        auto read_schema = ExportSchema(ReadSchema(selected_keys));
        ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

        std::shared_ptr<arrow::ChunkedArray> expected;
        ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                        arrow::struct_(logical_schema_->fields()), {expected_json}, &expected)
                        .ok());
        AssertChunkedArrayEquals(expected, actual);
    };

    assert_read("     ", R"([
        [1, [["     ", 20]]],
        [2, []],
        [3, null]
    ])");
    assert_read(".", R"([
        [1, []],
        [2, [[".", 30]]],
        [3, null]
    ])");
    assert_read("a,", R"([
        [1, [["", 10]]],
        [2, [["a", 40]]],
        [3, null]
    ])");
    assert_read("", R"([
        [1, [["", 10]]],
        [2, []],
        [3, null]
    ])");
}

TEST_F(MapSharedShreddingFileReaderTest, TestUnknownSelectedKeyReturnsEmptyMap) {
    auto reader = CreateReader(/*physical_array=*/nullptr, /*physical_schema=*/nullptr,
                               /*selected_keys=*/"missing");
    auto read_schema = ExportSchema(ReadSchema("missing"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema_->fields()), {R"([
                        [1, []],
                        [2, []],
                        [3, null],
                        [4, []]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestInvalidNullFieldMappingField) {
    auto physical_schema = PhysicalSchemaWithMetadata();
    std::string json = R"([
        [1, [null, 10, null, null]]
    ])";
    auto physical_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(physical_schema->fields()), json)
            .ValueOrDie();
    auto reader = CreateReader(physical_array, physical_schema, /*selected_keys=*/"a");
    auto read_schema = ExportSchema(ReadSchema("a"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(reader.get()),
                        "__field_mapping cannot be null");
}

TEST_F(MapSharedShreddingFileReaderTest, TestInvalidNullFieldMappingFieldElement) {
    auto physical_schema = PhysicalSchemaWithMetadata();
    std::string json = R"([
        [1, [[0, null], 10, null, null]]
    ])";
    auto physical_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(physical_schema->fields()), json)
            .ValueOrDie();
    auto reader = CreateReader(physical_array, physical_schema, /*selected_keys=*/"b");
    auto read_schema = ExportSchema(ReadSchema("b"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(reader.get()),
                        "__field_mapping element cannot be null");
}

TEST_F(MapSharedShreddingFileReaderTest, TestListValue) {
    std::shared_ptr<arrow::Schema> logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::list(arrow::int32()))),
    });
    MapSharedShreddingFieldMeta meta;
    meta.name_to_id = {{"a", 0}, {"b", 1}, {"c", 2}};
    meta.field_to_columns = {{0, {0, 1}}, {1, {0}}, {2, {0}}};
    meta.overflow_field_set = {2};
    meta.num_columns = 2;
    meta.max_row_width = 3;

    std::map<std::string, int32_t> field_to_num_columns = {{"tags", 2}};
    auto physical_schema =
        MapSharedShreddingUtils::LogicalToPhysicalSchema(logical_schema, field_to_num_columns);
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    ASSERT_OK(MapSharedShreddingUtils::SerializeMetadata(
        meta, MapSharedShreddingDefine::kDefaultDictCompression, metadata.get()));
    arrow::FieldVector physical_fields = physical_schema->fields();
    physical_fields[1] = physical_fields[1]->WithMetadata(metadata);
    physical_schema = arrow::schema(std::move(physical_fields));

    auto physical_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(physical_schema->fields()), R"([
        [1, [[0, 1], [1, null, 2], [3], null]],
        [2, [[2, 0], [5, 6], [7], null]],
        [3, null],
        [4, [[1, 0], [8], [9, 10], [[2, [null]]]]]
    ])")
            .ValueOrDie();
    auto reader = CreateReader(physical_array, physical_schema,
                               /*selected_keys=*/"a,c");  // NOLINT(whitespace/comma)

    auto read_metadata = std::make_shared<arrow::KeyValueMetadata>();
    read_metadata->Append("paimon.map.selected-keys", "a,c");
    arrow::FieldVector read_fields = logical_schema->fields();
    read_fields[1] = read_fields[1]->WithMetadata(read_metadata);
    auto read_schema = ExportSchema(arrow::schema(std::move(read_fields)));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema->fields()), {R"([
                        [1, [["a", [1, null, 2]]]],
                        [2, [["a", [7]], ["c", [5, 6]]]],
                        [3, null],
                        [4, [["a", [9, 10]], ["c", [null]]]]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestOrcDictionaryEncodedStringValue) {
    std::shared_ptr<arrow::Schema> logical_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::utf8())),
    });
    auto options = options_;
    std::string format = "orc";
    options[Options::FILE_FORMAT] = format;
    options["orc.dictionary-key-size-threshold"] = "1";
    ASSERT_OK_AND_ASSIGN(auto table_schema,
                         TableSchema::Create(TableSchema::FIRST_SCHEMA_ID, logical_schema,
                                             /*partition_keys=*/{}, /*primary_keys=*/{}, options));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    auto path_factory = CreatePathFactory(dir->Str(), format, core_options);
    auto compact_manager = std::make_shared<NoopCompactManager>();
    ASSERT_OK_AND_ASSIGN(
        auto writer,
        CreateAppendOnlyWriter(core_options, /*schema_id=*/0, logical_schema,
                               /*write_cols=*/std::nullopt,
                               /*max_sequence_number=*/-1, path_factory, compact_manager));
    auto batch = CreateBatch(logical_schema, R"([
        [1, [["a", "red"], ["b", "blue"]]],
        [2, [["c", "green"], ["a", "red"], ["b", "blue"]]],
        [3, null],
        [4, [["d", "yellow"], ["e", "blue"], ["c", null], ["a", "red"]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(auto inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    std::string data_file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);
    std::map<std::string, std::string> reader_options = {{"orc.read.enable-lazy-decoding", "true"}};
    auto reader = WrapReader(OpenFormatReader(data_file_path, format, reader_options),
                             /*selected_keys_str=*/"a,c");

    auto read_metadata = std::make_shared<arrow::KeyValueMetadata>();
    read_metadata->Append("paimon.map.selected-keys", "a,c");
    arrow::FieldVector read_fields = logical_schema->fields();
    read_fields[1] = read_fields[1]->WithMetadata(read_metadata);
    auto read_schema = ExportSchema(arrow::schema(std::move(read_fields)));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema->fields()), {R"([
                        [1, [["a", "red"]]],
                        [2, [["a", "red"], ["c", "green"]]],
                        [3, null],
                        [4, [["a", "red"], ["c", null]]]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

TEST_F(MapSharedShreddingFileReaderTest, TestReadsRealFormatFile) {
    // TODO(lisizhuo.lsz): support other format
    auto options = options_;
    std::string format = "orc";
    options[Options::FILE_FORMAT] = format;
    ASSERT_OK_AND_ASSIGN(auto table_schema,
                         TableSchema::Create(TableSchema::FIRST_SCHEMA_ID, logical_schema_,
                                             /*partition_keys=*/{}, /*primary_keys=*/{}, options));

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    auto path_factory = CreatePathFactory(dir->Str(), format, core_options);
    auto compact_manager = std::make_shared<NoopCompactManager>();
    ASSERT_OK_AND_ASSIGN(
        auto writer,
        CreateAppendOnlyWriter(core_options, /*schema_id=*/0, logical_schema_,
                               /*write_cols=*/std::nullopt,
                               /*max_sequence_number=*/-1, path_factory, compact_manager));
    auto batch = CreateBatch(logical_schema_, R"([
        [1, [["a", 1], ["b", 2]]],
        [2, [["c", 3], ["a", 4], ["b", 5]]],
        [3, null],
        [4, [["d", 6], ["e", 7], ["c", null], ["a", 9]]]
    ])");
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(auto inc, writer->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_OK(writer->Close());

    std::string data_file_path =
        path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);
    auto reader = WrapReader(OpenFormatReader(data_file_path, format),
                             /*selected_keys_str=*/"a,c");

    auto read_schema = ExportSchema(ReadSchema("a,c"));
    ASSERT_OK(reader->SetReadSchema(read_schema.get(), /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(logical_schema_->fields()), {R"([
                        [1, [["a", 1]]],
                        [2, [["a", 4], ["c", 3]]],
                        [3, null],
                        [4, [["a", 9], ["c", null]]]
                    ])"},
                    &expected)
                    .ok());
    AssertChunkedArrayEquals(expected, actual);
}

}  // namespace paimon::test
