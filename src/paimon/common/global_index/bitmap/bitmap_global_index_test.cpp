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
#include "paimon/common/global_index/bitmap/bitmap_global_index.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/global_index/wrap/file_index_writer_wrapper.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/data/timestamp.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class BitmapGlobalIndexTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeIndexPathFactory : public IndexPathFactory {
     public:
        explicit FakeIndexPathFactory(const std::string& index_path) : index_path_(index_path) {}
        std::string NewPath() const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::string& file_name) const override {
            return PathUtil::JoinPath(index_path_, file_name);
        }
        bool IsExternalPath() const override {
            return false;
        }

     private:
        std::string index_path_;
    };

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto schema = arrow::schema({arrow::field("f0", data_type)});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& index_root,
                                               const std::shared_ptr<arrow::DataType>& type,
                                               int32_t version,
                                               const std::shared_ptr<arrow::Array>& array,
                                               const Range& expected_range) const {
        std::map<std::string, std::string> options;
        options["version"] = std::to_string(version);
        auto file_index = std::make_shared<BitmapFileIndex>(options);
        auto global_index = std::make_shared<BitmapGlobalIndex>(file_index);

        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
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
        EXPECT_TRUE(StringUtils::StartsWith(result_metas[0].file_name, "bitmap-global-index-"));
        EXPECT_TRUE(StringUtils::EndsWith(result_metas[0].file_name, ".index"));
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
        const GlobalIndexIOMeta& meta) {
        auto file_index = std::make_shared<BitmapFileIndex>(std::map<std::string, std::string>());
        auto global_index = std::make_shared<BitmapGlobalIndex>(file_index);

        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
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

TEST_F(BitmapGlobalIndexTest, TestToGlobalIndexResult) {
    {
        ASSERT_OK_AND_ASSIGN(auto global_result, BitmapGlobalIndex::ToGlobalIndexResult(
                                                     /*range_end=*/5l, FileIndexResult::Remain()));
        CheckResult(global_result, {0l, 1l, 2l, 3l, 4l, 5l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto global_result, BitmapGlobalIndex::ToGlobalIndexResult(
                                                     /*range_end=*/5l, FileIndexResult::Skip()));
        CheckResult(global_result, {});
    }
    {
        auto bitmap_supplier = []() -> Result<RoaringBitmap32> {
            return RoaringBitmap32::From({1, 4, 2147483647});
        };
        auto file_result = std::make_shared<BitmapIndexResult>(bitmap_supplier);
        ASSERT_OK_AND_ASSIGN(auto global_result, BitmapGlobalIndex::ToGlobalIndexResult(
                                                     /*range_end=*/2147483647l, file_result));
        CheckResult(global_result, {1l, 4l, 2147483647l});
    }
    {
        class FakeFileIndexResult : public FileIndexResult {
            Result<bool> IsRemain() const override {
                return true;
            }
            std::string ToString() const override {
                return "fake file index result";
            }
        };
        auto file_result = std::make_shared<FakeFileIndexResult>();
        ASSERT_NOK_WITH_MSG(
            BitmapGlobalIndex::ToGlobalIndexResult(/*range_end=*/10l, file_result),
            "invalid FileIndexResult, supposed to be Remain or Skip or BitmapIndexResult");
    }
}

TEST_F(BitmapGlobalIndexTest, TestStringType) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data: a, null, b, null, a
    auto type = arrow::utf8();
    auto write_index = [&](int32_t version) -> Result<GlobalIndexIOMeta> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        ["a"],
        [null],
        ["b"],
        [null],
        ["a"]
    ])")
                .ValueOrDie());

        return WriteGlobalIndex(test_root, type, version, array, Range(0, 4));
    };
    auto check_index = [&](const GlobalIndexIOMeta& meta) {
        auto reader = CreateGlobalIndexReader(test_root, type, meta);

        Literal lit_a(FieldType::STRING, "a", 1);
        CheckResult(reader->VisitEqual(lit_a).value(), {0, 4});

        Literal lit_b(FieldType::STRING, "b", 1);
        CheckResult(reader->VisitEqual(lit_b).value(), {2});

        CheckResult(reader->VisitIsNull().value(), {1, 3});

        CheckResult(reader->VisitIn({lit_a, lit_b}).value(), {0, 2, 4});
        CheckResult(reader->VisitNotIn({lit_a, lit_b}).value(), {});

        // non-exist
        Literal lit_c(FieldType::STRING, "c", 1);
        CheckResult(reader->VisitEqual(lit_c).value(), {});

        // greater than return REMAIN file index result, will convert to all range global index
        // result
        CheckResult(reader->VisitGreaterThan(lit_c).value(), {0, 1, 2, 3, 4});

        // test visit vector search
        ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                "f0", 10, std::vector<float>({1.0f, 2.0f}), nullptr, nullptr)),
                            "FileIndexReaderWrapper is not supposed to handle vector search query");
    };

    {
        // test version 1
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/1));
        check_index(meta);
    }
    {
        // test version 2
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/2));
        check_index(meta);
    }
}

TEST_F(BitmapGlobalIndexTest, TestIntType) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data:  0, 1, null
    auto type = arrow::int32();
    auto write_index = [&](int32_t version) -> Result<GlobalIndexIOMeta> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [0],
        [1],
        [null]
    ])")
                .ValueOrDie());

        return WriteGlobalIndex(test_root, type, version, array, Range(0, 2));
    };
    auto check_index = [&](const GlobalIndexIOMeta& meta) {
        auto reader = CreateGlobalIndexReader(test_root, type, meta);

        Literal lit_0(static_cast<int32_t>(0));
        Literal lit_1(static_cast<int32_t>(1));
        Literal lit_2(static_cast<int32_t>(2));
        CheckResult(reader->VisitEqual(lit_0).value(), {0});
        CheckResult(reader->VisitEqual(lit_1).value(), {1});
        CheckResult(reader->VisitIsNull().value(), {2});
        CheckResult(reader->VisitIn({lit_0, lit_1, lit_2}).value(), {0, 1});
        CheckResult(reader->VisitNotIn({lit_0, lit_1, lit_2}).value(), {});
    };

    {
        // test version 1
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/1));
        check_index(meta);
    }
    {
        // test version 2
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/2));
        check_index(meta);
    }
}

TEST_F(BitmapGlobalIndexTest, TestTimestampType) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data:
    // 1745542802000lms, 123000ns
    // 1745542902000lms, 123000ns
    // 1745542602000lms, 123000ns
    // -1745lms, 123000ns
    // -1765lms, 123000ns
    // null
    // 1745542802000lms, 123001ns
    // -1725lms, 123000ns
    auto type = arrow::timestamp(arrow::TimeUnit::NANO);
    auto write_index = [&](int32_t version) -> Result<GlobalIndexIOMeta> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [1745542802000123000],
        [1745542902000123000],
        [1745542602000123000],
        [-1744877000],
        [-1764877000],
        [null],
        [1745542802000123001],
        [-1724877000]
    ])")
                .ValueOrDie());

        return WriteGlobalIndex(test_root, type, version, array, Range(0, 7));
    };
    auto check_index = [&](const GlobalIndexIOMeta& meta) {
        auto reader = CreateGlobalIndexReader(test_root, type, meta);

        CheckResult(reader->VisitIsNull().value(), {5});
        CheckResult(reader->VisitIsNotNull().value(), {0, 1, 2, 3, 4, 6, 7});
        CheckResult(reader->VisitEqual(Literal(Timestamp(1745542502000l, 123000))).value(), {});
        // as timestamp is normalized by micro seconds, there is a loss of precision in the
        // nanosecond part
        CheckResult(reader->VisitEqual(Literal(Timestamp(1745542802000l, 123000))).value(), {0, 6});
        CheckResult(reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 123000))).value(),
                    {1, 2, 3, 4, 7});
        CheckResult(reader
                        ->VisitIn({Literal(Timestamp(1745542802000l, 123000)),
                                   Literal(Timestamp(-1745, 123000)),
                                   Literal(Timestamp(1745542602000, 123000))})
                        .value(),
                    {0, 2, 3, 6});
        CheckResult(reader
                        ->VisitNotIn({Literal(Timestamp(1745542802000l, 123000)),
                                      Literal(Timestamp(-1745, 123000)),
                                      Literal(Timestamp(1745542602000, 123000))})
                        .value(),
                    {1, 4, 7});
    };

    {
        // test version 1
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/1));
        check_index(meta);
    }
    {
        // test version 2
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/2));
        check_index(meta);
    }
}

TEST_F(BitmapGlobalIndexTest, TestHighCardinality) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    auto type = arrow::utf8();
    std::vector<std::string> unique_values = {
        "asdfghjkl123_0", "asdfghjkl123_2500", "asdfghjkl123_300", "asdfghjkl123_20",
        "asdfghjkl123_1", "asdfghjkl123_2",    "asdfghjkl123_3",   "asdfghjkl123_4",
        "asdfghjkl123_5", "asdfghjkl123_6"};
    std::vector<std::vector<int64_t>> expected_bitmaps(unique_values.size());

    auto write_index = [&](int32_t version) -> Result<GlobalIndexIOMeta> {
        // clear expected bitmaps
        for (auto& bitmap : expected_bitmaps) {
            bitmap.clear();
        }

        arrow::StructBuilder struct_builder(arrow::struct_({arrow::field("f0", type)}),
                                            arrow::default_memory_pool(),
                                            {std::make_shared<arrow::StringBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));

        for (int32_t i = 0; i < 100000; i++) {
            EXPECT_TRUE(struct_builder.Append().ok());
            int64_t idx = paimon::test::RandomNumber(0, unique_values.size() - 1);
            EXPECT_TRUE(string_builder->Append(unique_values[idx].data()).ok());
            expected_bitmaps[idx].push_back(i);
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());

        return WriteGlobalIndex(test_root, type, version, array, Range(0, 99999));
    };

    auto check_index = [&](const GlobalIndexIOMeta& meta) {
        auto reader = CreateGlobalIndexReader(test_root, type, meta);

        for (size_t i = 0; i < unique_values.size(); i++) {
            Literal lit(FieldType::STRING, unique_values[i].data(), unique_values[i].size());
            CheckResult(reader->VisitEqual(lit).value(), expected_bitmaps[i]);
        }
        CheckResult(reader->VisitIsNull().value(), {});
    };

    {
        // test version 1
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/1));
        check_index(meta);
    }
    {
        // test version 2
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/2));
        check_index(meta);
    }
}

TEST_F(BitmapGlobalIndexTest, TestAllNull) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    // data: null, null, null, null
    auto type = arrow::int32();
    auto write_index = [&](int32_t version) -> Result<GlobalIndexIOMeta> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [null],
        [null],
        [null],
        [null]
    ])")
                .ValueOrDie());

        return WriteGlobalIndex(test_root, type, version, array, Range(0, 3));
    };
    auto check_index = [&](const GlobalIndexIOMeta& meta) {
        auto reader = CreateGlobalIndexReader(test_root, type, meta);

        Literal lit_0(static_cast<int32_t>(0));
        CheckResult(reader->VisitEqual(lit_0).value(), {});
        CheckResult(reader->VisitIsNull().value(), {0, 1, 2, 3});
        CheckResult(reader->VisitIsNotNull().value(), {});
    };

    {
        // test version 1
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/1));
        check_index(meta);
    }
    {
        // test version 2
        ASSERT_OK_AND_ASSIGN(auto meta, write_index(/*version=*/2));
        check_index(meta);
    }
}

}  // namespace paimon::test
