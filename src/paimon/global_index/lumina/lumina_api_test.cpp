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
#include "lumina/api/LuminaBuilder.h"
#include "lumina/core/Constants.h"
#include "lumina/core/Status.h"
#include "lumina/core/Types.h"
#include "lumina/extensions/SearchWithFilterExtension.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/lumina/lumina_file_reader.h"
#include "paimon/global_index/lumina/lumina_file_writer.h"
#include "paimon/global_index/lumina/lumina_memory_pool.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lumina::test {
class LuminaInterfaceTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void WriteAndFlush(const std::string& index_path,
                       const std::vector<::lumina::core::vector_id_t>& row_ids) const {
        auto fs = std::make_shared<LocalFileSystem>();
        std::shared_ptr<MemoryPool> paimon_pool = GetMemoryPool();
        auto pool = std::make_shared<LuminaMemoryPool>(paimon_pool);
        ::lumina::core::MemoryResourceConfig memory_resource(pool.get());
        {  // create writer
            ::lumina::api::BuilderOptions builder_options;
            builder_options.Set(::lumina::core::kIndexType, ::lumina::core::kIndexTypeBruteforce)
                .Set(::lumina::core::kDimension, 4)
                .Set(::lumina::core::kDistanceMetric, ::lumina::core::kDistanceL2)
                .Set(::lumina::core::kEncodingType, ::lumina::core::kEncodingRawf32);
            auto builder_result =
                ::lumina::api::LuminaBuilder::Create(builder_options, memory_resource);
            ASSERT_TRUE(builder_result.IsOk()) << builder_result.GetStatus().Message();
            auto writer = std::move(builder_result).TakeValue();
            // pretrain
            ASSERT_TRUE(writer.Pretrain(/*data=*/nullptr, /*n=*/0).IsOk());
            // insert data
            auto status = writer.InsertBatch(data_vec_.data(), row_ids.data(), 4);
            ASSERT_TRUE(status.IsOk());
            // check memory paimon_pool
            ASSERT_GT(paimon_pool->CurrentUsage(), 0);
            ASSERT_GT(paimon_pool->MaxMemoryUsage(), 0);
            // dump index
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                                 fs->Create(index_path, /*overwrite=*/false));
            auto file_writer = std::make_unique<LuminaFileWriter>(out);
            ASSERT_TRUE(writer.Dump(std::move(file_writer), ::lumina::api::IOOptions()).IsOk());
        }
        // check memory paimon_pool
        ASSERT_EQ(paimon_pool->CurrentUsage(), 0);
        ASSERT_GT(paimon_pool->MaxMemoryUsage(), 0);
    }

    void Search(const std::string& index_path, int32_t topk,
                const std::vector<::lumina::core::vector_id_t>& expected_row_ids,
                const std::vector<float>& expected_distances,
                const std::function<bool(::lumina::core::vector_id_t id)>& filter = nullptr) const {
        ASSERT_EQ(expected_row_ids.size(), expected_distances.size());
        auto fs = std::make_shared<LocalFileSystem>();
        std::shared_ptr<MemoryPool> paimon_pool = GetMemoryPool();
        auto pool = std::make_shared<LuminaMemoryPool>(paimon_pool);
        ::lumina::core::MemoryResourceConfig memory_resource(pool.get());
        {
            // create reader
            ::lumina::api::SearcherOptions searcher_options;
            searcher_options.Set(::lumina::core::kIndexType, ::lumina::core::kIndexTypeBruteforce)
                .Set(::lumina::core::kDimension, 4);
            auto reader_result =
                ::lumina::api::LuminaSearcher::Create(searcher_options, memory_resource);
            ASSERT_TRUE(reader_result.IsOk());
            auto reader = std::move(reader_result).TakeValue();
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(index_path));
            auto file_reader = std::make_unique<LuminaFileReader>(in);
            ASSERT_TRUE(reader.Open(std::move(file_reader), ::lumina::api::IOOptions()).IsOk());
            // check meta
            auto meta = reader.GetMeta();
            ASSERT_EQ(meta.dim, 4);
            ASSERT_EQ(meta.count, 4);

            // check memory paimon_pool
            ASSERT_GT(paimon_pool->CurrentUsage(), 0);
            ASSERT_GT(paimon_pool->MaxMemoryUsage(), 0);

            auto search = [&](int32_t parallel_number) {
                ::lumina::api::Query query(query_.data(), query_.size());
                ::lumina::api::SearchOptions search_options;
                search_options.Set(::lumina::core::kTopK, topk);
                if (parallel_number > 0) {
                    search_options.Set(::lumina::core::kSearchParallelNumber, parallel_number);
                }

                if (!filter) {
                    auto search_result = reader.Search(query, search_options, *pool);
                    ASSERT_TRUE(search_result.IsOk()) << search_result.GetStatus().Message();
                    CheckResult(search_result.Value().topk, expected_row_ids, expected_distances);
                } else {
                    search_options.Set(::lumina::core::kSearchThreadSafeFilter, true);
                    ::lumina::extensions::SearchWithFilterExtension reader_with_filter;
                    ASSERT_TRUE(reader.Attach(reader_with_filter).IsOk());
                    auto search_result =
                        reader_with_filter.SearchWithFilter(query, filter, search_options, *pool);
                    ASSERT_TRUE(search_result.IsOk()) << search_result.GetStatus().Message();
                    CheckResult(search_result.Value().topk, expected_row_ids, expected_distances);
                }

                // TODO(xinyu.lxy): check memory paimon_pool, current memory use = query mem +
                // reader mem
                ASSERT_GT(paimon_pool->CurrentUsage(), 0);
                ASSERT_GT(paimon_pool->MaxMemoryUsage(), 0);
            };

            // single thread search result
            search(-1);

            // multi thread search result
            search(4);
        }
        // check memory paimon_pool
        ASSERT_EQ(paimon_pool->CurrentUsage(), 0);
        ASSERT_GT(paimon_pool->MaxMemoryUsage(), 0);
    }

    void CheckResult(const std::vector<::lumina::api::LuminaSearcher::SearchHit>& search_result,
                     const std::vector<::lumina::core::vector_id_t>& expected_row_ids,
                     const std::vector<float>& expected_distances) const {
        ASSERT_EQ(search_result.size(), expected_row_ids.size());
        for (size_t i = 0; i < search_result.size(); i++) {
            ASSERT_EQ(expected_row_ids[i], search_result[i].id);
            ASSERT_NEAR(expected_distances[i], search_result[i].distance, 0.01);
        }
    }

 private:
    std::vector<float> data_vec_ = {
        0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 1.0f,
        1.0f, 0.0f, 1.0f, 0.0f, 1.0f, 1.0f, 1.0f, 1.0f,
    };
    std::vector<float> query_ = {1.0f, 1.0f, 1.0f, 1.1f};
};

TEST_F(LuminaInterfaceTest, TestSimple) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lumina_test.index";

    // write index
    std::vector<::lumina::core::vector_id_t> row_ids = {0l, 1l, 2l, 3l};
    WriteAndFlush(index_path, row_ids);

    // read index
    std::vector<::lumina::core::vector_id_t> expected_row_ids = {3l, 1l, 2l, 0l};
    std::vector<float> expected_distances = {0.01f, 2.01f, 2.21f, 4.21f};
    Search(index_path, /*topk=*/4, expected_row_ids, expected_distances);
}

TEST_F(LuminaInterfaceTest, TestWithDocIdGap) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lumina_test.index";

    // write index
    std::vector<::lumina::core::vector_id_t> row_ids = {0l, 2l, 4l, 6l};
    WriteAndFlush(index_path, row_ids);

    // read index
    std::vector<::lumina::core::vector_id_t> expected_row_ids = {6l, 2l, 4l, 0l};
    std::vector<float> expected_distances = {0.01f, 2.01f, 2.21f, 4.21f};
    Search(index_path, /*topk=*/4, expected_row_ids, expected_distances);
}

TEST_F(LuminaInterfaceTest, TestWithSmallTopk) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lumina_test.index";

    // write index
    std::vector<::lumina::core::vector_id_t> row_ids = {0l, 1l, 2l, 3l};
    WriteAndFlush(index_path, row_ids);

    // read index
    std::vector<::lumina::core::vector_id_t> expected_row_ids = {3l, 1l, 2l};
    std::vector<float> expected_distances = {0.01f, 2.01f, 2.21f};
    Search(index_path, /*topk=*/3, expected_row_ids, expected_distances);
}

TEST_F(LuminaInterfaceTest, TestWithFilter) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lumina_test.index";

    // write index
    std::vector<::lumina::core::vector_id_t> row_ids = {0l, 1l, 2l, 3l};
    WriteAndFlush(index_path, row_ids);

    // read index
    {
        std::vector<::lumina::core::vector_id_t> expected_row_ids = {1l, 2l};
        std::vector<float> expected_distances = {2.01f, 2.21f};
        auto filter = [](::lumina::core::vector_id_t id) -> bool { return id < 3; };
        Search(index_path, /*topk=*/2, expected_row_ids, expected_distances, filter);
    }
    {
        std::vector<::lumina::core::vector_id_t> expected_row_ids = {1l, 2l, 0l};
        std::vector<float> expected_distances = {2.01f, 2.21f, 4.21f};
        auto filter = [](::lumina::core::vector_id_t id) -> bool { return id < 3; };
        Search(index_path, /*topk=*/4, expected_row_ids, expected_distances, filter);
    }
}

}  // namespace paimon::lumina::test
