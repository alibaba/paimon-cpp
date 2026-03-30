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
#include "paimon/common/global_index/rangebitmap/range_bitmap_global_index.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"
#include "paimon/common/global_index/wrap/file_index_writer_wrapper.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class RangeBitmapGlobalIndexTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto schema = arrow::schema({arrow::field("f0", data_type)});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& index_root,
                                               const std::shared_ptr<arrow::DataType>& type,
                                               const std::shared_ptr<arrow::Array>& array,
                                               const Range& expected_range) const {
        std::map<std::string, std::string> options;
        auto file_index = std::make_shared<RangeBitmapFileIndex>(options);
        auto global_index = std::make_shared<RangeBitmapGlobalIndex>(file_index);

        auto path_factory = std::make_shared<MockIndexPathFactory>(index_root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<GlobalIndexWriter> global_writer,
            global_index->CreateWriter("f0", CreateArrowSchema(type).get(), file_writer, pool_));

        auto wrapper = std::dynamic_pointer_cast<FileIndexWriterWrapper>(global_writer);
        EXPECT_TRUE(wrapper);
        wrapper->max_write_size_ = 128;

        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(global_writer->AddBatch(&c_array));
        PAIMON_ASSIGN_OR_RAISE(auto result_metas, global_writer->Finish());
        // check meta
        EXPECT_EQ(result_metas.size(), 1);
        auto file_name = PathUtil::GetName(result_metas[0].file_path);
        EXPECT_TRUE(StringUtils::StartsWith(file_name, "range-bitmap-global-index-"));
        EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
        EXPECT_EQ(result_metas[0].range_end, expected_range.to);
        EXPECT_FALSE(result_metas[0].metadata);
        return result_metas[0];
    }

    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap64::From(expected))
            << "result=" << (typed_result->GetBitmap().value())->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    }

    std::shared_ptr<GlobalIndexReader> CreateGlobalIndexReader(
        const std::string& index_root, const std::shared_ptr<arrow::DataType>& type,
        const GlobalIndexIOMeta& meta) const {
        auto file_index =
            std::make_shared<RangeBitmapFileIndex>(std::map<std::string, std::string>());
        auto global_index = std::make_shared<RangeBitmapGlobalIndex>(file_index);

        auto path_factory = std::make_shared<MockIndexPathFactory>(index_root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        EXPECT_OK_AND_ASSIGN(
            auto global_index_reader,
            global_index->CreateReader(CreateArrowSchema(type).get(), file_reader, {meta}, pool_));
        EXPECT_TRUE(global_index_reader);
        return global_index_reader;
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
};

TEST_F(RangeBitmapGlobalIndexTest, TestIntType) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data:  0, 1, 2, 3, null
    auto type = arrow::int32();

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                  R"([
        [0],
        [1],
        [2],
        [3],
        [null]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, type, array, Range(0, 4)));

    auto reader = CreateGlobalIndexReader(test_root, type, meta);

    Literal lit_0(static_cast<int32_t>(0));
    Literal lit_1(static_cast<int32_t>(1));
    Literal lit_2(static_cast<int32_t>(2));
    Literal lit_3(static_cast<int32_t>(3));
    Literal lit_5(static_cast<int32_t>(5));

    // equal
    CheckResult(reader->VisitEqual(lit_0).value(), {0});
    CheckResult(reader->VisitEqual(lit_1).value(), {1});
    CheckResult(reader->VisitEqual(lit_2).value(), {2});
    CheckResult(reader->VisitEqual(lit_3).value(), {3});
    CheckResult(reader->VisitEqual(lit_5).value(), {});

    // not equal
    CheckResult(reader->VisitNotEqual(lit_0).value(), {1, 2, 3});
    CheckResult(reader->VisitNotEqual(lit_5).value(), {0, 1, 2, 3});

    // null
    CheckResult(reader->VisitIsNull().value(), {4});
    CheckResult(reader->VisitIsNotNull().value(), {0, 1, 2, 3});

    // in / not in
    CheckResult(reader->VisitIn({lit_0, lit_1, lit_5}).value(), {0, 1});
    CheckResult(reader->VisitNotIn({lit_0, lit_1, lit_5}).value(), {2, 3});

    // range queries - the core advantage of range-bitmap
    // greater than
    CheckResult(reader->VisitGreaterThan(lit_1).value(), {2, 3});
    CheckResult(reader->VisitGreaterThan(lit_3).value(), {});

    // less than
    CheckResult(reader->VisitLessThan(lit_2).value(), {0, 1});
    CheckResult(reader->VisitLessThan(lit_0).value(), {});

    // greater or equal
    CheckResult(reader->VisitGreaterOrEqual(lit_2).value(), {2, 3});
    CheckResult(reader->VisitGreaterOrEqual(lit_0).value(), {0, 1, 2, 3});

    // less or equal
    CheckResult(reader->VisitLessOrEqual(lit_1).value(), {0, 1});
    CheckResult(reader->VisitLessOrEqual(lit_3).value(), {0, 1, 2, 3});

    // test visit vector search
    ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                            "f0", 10, std::vector<float>({1.0f, 2.0f}), nullptr, nullptr,
                            std::nullopt, std::map<std::string, std::string>())),
                        "FileIndexReaderWrapper is not supposed to handle vector search query");
}

TEST_F(RangeBitmapGlobalIndexTest, TestAllNull) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data: null, null, null, null
    auto type = arrow::int32();

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                  R"([
        [null],
        [null],
        [null],
        [null]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, type, array, Range(0, 3)));

    auto reader = CreateGlobalIndexReader(test_root, type, meta);

    Literal lit_0(static_cast<int32_t>(0));
    CheckResult(reader->VisitEqual(lit_0).value(), {});
    CheckResult(reader->VisitIsNull().value(), {0, 1, 2, 3});
    CheckResult(reader->VisitIsNotNull().value(), {});
    CheckResult(reader->VisitGreaterThan(lit_0).value(), {});
    CheckResult(reader->VisitLessThan(lit_0).value(), {});
}

TEST_F(RangeBitmapGlobalIndexTest, TestHighCardinality) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);

    ScopeGuard guard([seed]() { std::cout << "case failed with seed=" << seed << std::endl; });

    auto type = arrow::int32();
    // use 10 unique integer values
    std::vector<int32_t> unique_values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<std::vector<int64_t>> expected_bitmaps(unique_values.size());

    arrow::StructBuilder struct_builder(arrow::struct_({arrow::field("f0", type)}),
                                        arrow::default_memory_pool(),
                                        {std::make_shared<arrow::Int32Builder>()});
    auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(0));

    for (int32_t i = 0; i < 100000; i++) {
        ASSERT_TRUE(struct_builder.Append().ok());
        int64_t idx = paimon::test::RandomNumber(0, unique_values.size() - 1);
        ASSERT_TRUE(int_builder->Append(unique_values[idx]).ok());
        expected_bitmaps[idx].push_back(i);
    }
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());

    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, type, array, Range(0, 99999)));

    auto reader = CreateGlobalIndexReader(test_root, type, meta);

    for (size_t i = 0; i < unique_values.size(); i++) {
        Literal lit(unique_values[i]);
        CheckResult(reader->VisitEqual(lit).value(), expected_bitmaps[i]);
    }
    CheckResult(reader->VisitIsNull().value(), {});

    // range query on high cardinality data
    // values >= 5
    std::vector<int64_t> expected_gte_5;
    for (size_t i = 5; i < unique_values.size(); i++) {
        expected_gte_5.insert(expected_gte_5.end(), expected_bitmaps[i].begin(),
                              expected_bitmaps[i].end());
    }
    std::sort(expected_gte_5.begin(), expected_gte_5.end());
    CheckResult(reader->VisitGreaterOrEqual(Literal(static_cast<int32_t>(5))).value(),
                expected_gte_5);
    guard.Release();
}

TEST_F(RangeBitmapGlobalIndexTest, TestDoubleType) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data: 1.1, 2.2, 3.3, 4.4, null, 2.2
    auto type = arrow::float64();

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                  R"([
        [1.1],
        [2.2],
        [3.3],
        [4.4],
        [null],
        [2.2]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, type, array, Range(0, 5)));

    auto reader = CreateGlobalIndexReader(test_root, type, meta);

    Literal lit_1_1(1.1);
    Literal lit_2_2(2.2);
    Literal lit_3_3(3.3);
    Literal lit_4_4(4.4);
    Literal lit_5_5(5.5);

    // equal
    CheckResult(reader->VisitEqual(lit_1_1).value(), {0});
    CheckResult(reader->VisitEqual(lit_2_2).value(), {1, 5});
    CheckResult(reader->VisitEqual(lit_3_3).value(), {2});
    CheckResult(reader->VisitEqual(lit_4_4).value(), {3});
    CheckResult(reader->VisitEqual(lit_5_5).value(), {});

    // not equal
    CheckResult(reader->VisitNotEqual(lit_2_2).value(), {0, 2, 3});
    CheckResult(reader->VisitNotEqual(lit_5_5).value(), {0, 1, 2, 3, 5});

    // null
    CheckResult(reader->VisitIsNull().value(), {4});
    CheckResult(reader->VisitIsNotNull().value(), {0, 1, 2, 3, 5});

    // in / not in
    CheckResult(reader->VisitIn({lit_1_1, lit_3_3, lit_5_5}).value(), {0, 2});
    CheckResult(reader->VisitNotIn({lit_1_1, lit_3_3, lit_5_5}).value(), {1, 3, 5});

    // range queries
    // greater than
    CheckResult(reader->VisitGreaterThan(lit_2_2).value(), {2, 3});
    CheckResult(reader->VisitGreaterThan(lit_4_4).value(), {});

    // less than
    CheckResult(reader->VisitLessThan(lit_3_3).value(), {0, 1, 5});
    CheckResult(reader->VisitLessThan(lit_1_1).value(), {});

    // greater or equal
    CheckResult(reader->VisitGreaterOrEqual(lit_3_3).value(), {2, 3});
    CheckResult(reader->VisitGreaterOrEqual(lit_1_1).value(), {0, 1, 2, 3, 5});

    // less or equal
    CheckResult(reader->VisitLessOrEqual(lit_2_2).value(), {0, 1, 5});
    CheckResult(reader->VisitLessOrEqual(lit_4_4).value(), {0, 1, 2, 3, 5});
}

}  // namespace paimon::test
