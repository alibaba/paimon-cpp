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
#include "paimon/global_index/lumina/lumina_global_index.h"

#include <random>
#include <thread>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_vector_search_global_index_result.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lumina::test {
class LuminaGlobalIndexTest : public ::testing::Test {
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
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& index_root,
                                               const std::shared_ptr<arrow::DataType>& data_type,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<arrow::Array>& array,
                                               const Range& expected_range) const {
        auto global_index = std::make_shared<LuminaGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> global_writer,
                               global_index->CreateWriter("f0", CreateArrowSchema(data_type).get(),
                                                          file_writer, pool_));

        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(global_writer->AddBatch(&c_array));
        PAIMON_ASSIGN_OR_RAISE(auto result_metas, global_writer->Finish());
        // check meta
        EXPECT_EQ(result_metas.size(), 1);
        auto file_name = PathUtil::GetName(result_metas[0].file_path);
        EXPECT_TRUE(StringUtils::StartsWith(file_name, "lumina-global-index-"));
        EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
        EXPECT_EQ(result_metas[0].range_end, expected_range.to);
        EXPECT_TRUE(result_metas[0].metadata);
        return result_metas[0];
    }

    void CheckResult(const std::shared_ptr<VectorSearchGlobalIndexResult>& result,
                     const std::vector<int64_t>& expected_ids,
                     const std::vector<float>& expected_scores) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap64::From(expected_ids))
            << "result=" << (typed_result->GetBitmap().value())->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_ids).ToString();
        ASSERT_EQ(typed_result->scores_.size(), expected_scores.size());

        std::map<int64_t, float> id_to_score;
        for (size_t i = 0; i < expected_ids.size(); i++) {
            id_to_score[expected_ids[i]] = expected_scores[i];
        }
        std::vector<float> expected_scores_ordered_by_id;
        for (const auto& [id, score] : id_to_score) {
            expected_scores_ordered_by_id.push_back(score);
        }
        for (size_t i = 0; i < expected_scores.size(); i++) {
            ASSERT_NEAR(typed_result->scores_[i], expected_scores_ordered_by_id[i], 0.01);
        }
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateGlobalIndexReader(
        const std::string& index_root, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta) const {
        auto global_index = std::make_shared<LuminaGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        return global_index->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta},
                                          pool_);
    }

    std::shared_ptr<arrow::Array> CreateRandomVector(int32_t element_size,
                                                     int32_t dimension) const {
        int64_t total_values = element_size * dimension;
        auto float_builder = std::make_shared<arrow::FloatBuilder>();
        arrow::ListBuilder list_builder(arrow::default_memory_pool(), float_builder);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<float> dis(0.0f, 2.0f);

        EXPECT_TRUE(float_builder->Reserve(total_values).ok());
        EXPECT_TRUE(list_builder.Reserve(element_size).ok());

        for (int64_t i = 0; i < element_size; ++i) {
            EXPECT_TRUE(list_builder.Append().ok());
            for (int64_t j = 0; j < dimension; ++j) {
                float val = dis(gen);
                EXPECT_TRUE(float_builder->Append(val).ok());
            }
        }

        std::shared_ptr<arrow::Array> list_array;
        EXPECT_TRUE(list_builder.Finish(&list_array).ok());

        auto struct_array = arrow::StructArray::Make({list_array}, {"f0"}).ValueOrDie();
        return struct_array;
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::map<std::string, std::string> options_ = {{"lumina.index.dimension", "4"},
                                                   {"lumina.index.type", "bruteforce"},
                                                   {"lumina.distance.metric", "l2"},
                                                   {"lumina.encoding.type", "rawf32"},
                                                   {"lumina.search.parallel_number", "10"}};
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::list(arrow::float32()))});
    std::shared_ptr<arrow::Array> array_ = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                     R"([
        [[0.0, 0.0, 0.0, 0.0]],
        [[0.0, 1.0, 0.0, 1.0]],
        [[1.0, 0.0, 1.0, 0.0]],
        [[1.0, 1.0, 1.0, 1.0]]
    ])")
                                               .ValueOrDie();
    std::vector<float> query_ = {1.0f, 1.0f, 1.0f, 1.1f};
};

TEST_F(LuminaGlobalIndexTest, TestSimple) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    ASSERT_OK_AND_ASSIGN(auto meta,
                         WriteGlobalIndex(test_root, data_type_, options_, array_, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        // recall all data
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(vector_search_result, {3l, 1l, 2l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
    }
    {
        // small limit
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/3, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(vector_search_result, {3l, 1l, 2l}, {0.01f, 2.01f, 2.21f});
    }
    {
        // visit equal will return all rows
        ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
        ASSERT_EQ(is_null_result->ToString(), "{0,1,2,3}");
    }
}

TEST_F(LuminaGlobalIndexTest, TestWithFilter) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    ASSERT_OK_AND_ASSIGN(auto meta,
                         WriteGlobalIndex(test_root, data_type_, options_, array_, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/2, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(vector_search_result, {3l, 1l}, {0.01f, 2.01f});
    }
    {
        auto filter = [](int64_t id) -> bool { return id < 3; };
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/2, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(vector_search_result, {1l, 2l}, {2.01f, 2.21f});
    }
    {
        auto filter = [](int64_t id) -> bool { return id < 3; };
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(vector_search_result, {1l, 2l, 0l}, {2.01f, 2.21f, 4.21f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestInvalidInputs) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string index_root = test_root_dir->Str();
    // invalid inputs in write
    {
        auto data_type = arrow::int32();
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "arrow schema must be struct type when create LuminaIndexWriter");
    }
    {
        auto data_type = arrow::struct_({arrow::field("f1", arrow::list(arrow::float32()))});
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "field f0 not exist in arrow schema when create LuminaIndexWriter");
    }
    {
        auto data_type = arrow::struct_({arrow::field("f0", arrow::float32())});
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "field type must be list[float] when create LuminaIndexWriter");
    }
    {
        auto data_type = arrow::struct_({arrow::field("f0", arrow::list(arrow::float64()))});
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "field type must be list[float] when create LuminaIndexWriter");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0, 0.0]],
               null
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
                            "arrow_array in LuminaIndexWriter is invalid, must not null");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0, 0.0]],
               [[0.0, 1.0, 0.0, null]]
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
                            "field value array in LuminaIndexWriter is invalid, must not null");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0, 0.0]],
               [[0.0, 1.0, 0.0]]
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
            "invalid input array in LuminaIndexWriter, length of field  array [2] multiplied "
            "dimension [4] must match length of field value array [7]");
    }

    {
        // invalid inputs in read
        auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(test_root_dir);
        std::string index_root = test_root_dir->Str();
        ASSERT_OK_AND_ASSIGN(
            auto meta, WriteGlobalIndex(index_root, data_type_, options_, array_, Range(0, 3)));
        // read
        {
            auto fake_meta = meta;
            fake_meta.metadata = nullptr;
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, /*meta=*/fake_meta),
                "Lumina global index must have meta data");
        }
        {
            auto fake_meta = meta;
            auto fake_index_meta_json = StringUtils::Replace(
                std::string(fake_meta.metadata->data(), fake_meta.metadata->size()),
                /*search_string=*/"l2", /*replacement=*/"unknown");
            fake_meta.metadata = std::make_shared<Bytes>(fake_index_meta_json, pool_.get());
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, fake_meta),
                "invalid distance type unknown for lumina");
        }
        {
            auto global_index = std::make_shared<LuminaGlobalIndex>(options_);
            auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
            auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);

            ASSERT_NOK_WITH_MSG(global_index->CreateReader(CreateArrowSchema(data_type_).get(),
                                                           file_reader, {meta, meta}, pool_),
                                "lumina index only has one index file per shard");
        }
        {
            auto data_type = arrow::struct_({arrow::field("f0", arrow::list(arrow::float32())),
                                             arrow::field("f1", arrow::list(arrow::float32()))});
            ASSERT_NOK_WITH_MSG(CreateGlobalIndexReader(index_root, data_type, options_, meta),
                                "LuminaGlobalIndex now only support one field");
        }
        {
            auto data_type = arrow::struct_({arrow::field("f0", arrow::float32())});
            ASSERT_NOK_WITH_MSG(CreateGlobalIndexReader(index_root, data_type, options_, meta),
                                "field type must be list[float] when create LuminaIndexReader");
        }
        {
            auto data_type = arrow::struct_({arrow::field("f0", arrow::list(arrow::float64()))});
            ASSERT_NOK_WITH_MSG(CreateGlobalIndexReader(index_root, data_type, options_, meta),
                                "field type must be list[float] when create LuminaIndexReader");
        }
        {
            auto fake_meta = meta;
            fake_meta.file_path = "non-exist-file";
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, fake_meta),
                "non-exist-file\' not exists");
        }
        {
            auto fake_meta = meta;
            fake_meta.range_end = 50;
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, fake_meta),
                "lumina index row count 4 mismatch row count 51 in io meta");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query_, /*filter=*/nullptr,
                                    PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f0",
                                                            FieldType::BIGINT, Literal(5l)),
                                    /*distance_type=*/std::nullopt,
                                    /*options=*/std::map<std::string, std::string>())),
                                "lumina index not support predicate in VisitVectorSearch");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query_, /*filter=*/nullptr,
                                    /*predicate=*/nullptr,
                                    /*distance_type=*/VectorSearch::DistanceType::COSINE,
                                    /*options=*/std::map<std::string, std::string>())),
                                "distance type for index and search not match");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            auto query = query_;
            query.push_back(1.0f);
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query, /*filter=*/nullptr,
                                    /*predicate=*/nullptr,
                                    /*distance_type=*/std::nullopt,
                                    /*options=*/std::map<std::string, std::string>())),
                                "dimension for index and search not match");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            auto fake_options = options_;
            fake_options["lumina.index.type"] = "diskann";
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query_, /*filter=*/nullptr,
                                    /*predicate=*/nullptr,
                                    /*distance_type=*/std::nullopt,
                                    /*options=*/fake_options)),
                                "index type for index and search not match");
        }
    }
}

TEST_F(LuminaGlobalIndexTest, TestHighCardinalityAndMultiThreadSearch) {
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    auto array = CreateRandomVector(/*element_size*/ 10000, /*dimension=*/4);
    ASSERT_OK_AND_ASSIGN(
        auto meta, WriteGlobalIndex(test_root, data_type_, options_, array, Range(0, 10000 - 1)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));

    auto search_with_filter = [&]() {
        int32_t limit = paimon::test::RandomNumber(1, 100);
        auto filter = [](int64_t id) -> bool { return id % 2; };
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                "f0", limit, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        auto typed_result =
            std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(vector_search_result);
        ASSERT_TRUE(typed_result);
        ASSERT_EQ(typed_result->bitmap_.Cardinality(), limit);
    };

    auto search = [&]() {
        int32_t limit = paimon::test::RandomNumber(1, 100);
        ASSERT_OK_AND_ASSIGN(
            auto vector_search_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                "f0", limit, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        auto typed_result =
            std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(vector_search_result);
        ASSERT_TRUE(typed_result);
        ASSERT_EQ(typed_result->bitmap_.Cardinality(), limit);
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back(search);
    }
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back(search_with_filter);
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

}  // namespace paimon::lumina::test
