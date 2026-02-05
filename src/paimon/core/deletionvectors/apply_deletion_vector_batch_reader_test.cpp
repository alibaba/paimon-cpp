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

#include "paimon/core/deletionvectors/apply_deletion_vector_batch_reader.h"

#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"
#include "paimon/executor.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/mock/mock_format_reader_builder.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/read_ahead_cache.h"

namespace arrow {
class Array;
}  // namespace arrow
namespace paimon {
class FileSystem;
}  // namespace paimon

namespace paimon::test {
class ApplyDeletionVectorBatchReaderTest : public ::testing::Test,
                                           public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        int_type_ = arrow::int32();
        target_type_ = arrow::struct_({arrow::field("f1", int_type_)});

        pool_ = GetDefaultPool();
        fs_ = std::make_shared<MockFileSystem>();
        executor_ = CreateDefaultExecutor(/*thread_count=*/2);
    }
    void TearDown() override {}

    void CheckResult(BatchReader* apply_dv_batch_reader,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_chunk_array) {
        ASSERT_OK_AND_ASSIGN(auto result_chunk_array,
                             ReadResultCollector::CollectResult(apply_dv_batch_reader));
        if (expected_chunk_array) {
            ASSERT_EQ(expected_chunk_array->length(), result_chunk_array->length());
            ASSERT_TRUE(expected_chunk_array->Equals(result_chunk_array));
        } else {
            ASSERT_FALSE(result_chunk_array);
        }
    }

    void CheckResult(const std::string& data_str, const std::vector<char>& dv_data,
                     const std::string& expected_str) {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(int_type_, data_str).ValueOrDie();
        std::shared_ptr<arrow::Array> data =
            arrow::StructArray::Make({f1}, target_type_->fields()).ValueOrDie();

        int32_t prefetch_batch_count = 3;
        for (int32_t batch_size : {1, 2, 4, 10}) {
            auto dv = DeletionVector::FromPrimitiveArray(dv_data, pool_.get());
            std::unique_ptr<FileBatchReader> file_batch_reader;
            bool enable_prefetch = GetParam();
            if (enable_prefetch) {
                MockFormatReaderBuilder reader_builder(data, target_type_, batch_size);
                ASSERT_OK_AND_ASSIGN(
                    file_batch_reader,
                    PrefetchFileBatchReaderImpl::Create(
                        /*data_file_path=*/"DUMMY", &reader_builder, fs_, prefetch_batch_count,
                        batch_size, prefetch_batch_count * 2,
                        /*enable_adaptive_prefetch_strategy=*/false, executor_,
                        /*initialize_read_ranges=*/true,
                        /*prefetch_cache_mode=*/PrefetchCacheMode::ALWAYS, CacheConfig(), pool_));
            } else {
                file_batch_reader =
                    std::make_unique<MockFileBatchReader>(data, target_type_, batch_size);
            }
            auto apply_dv_batch_reader = std::make_unique<ApplyDeletionVectorBatchReader>(
                std::move(file_batch_reader), std::move(dv));
            if (expected_str.empty()) {
                CheckResult(apply_dv_batch_reader.get(), nullptr);
            } else {
                auto expected =
                    arrow::ipc::internal::json::ArrayFromJSON(int_type_, expected_str).ValueOrDie();
                std::shared_ptr<arrow::Array> expect_array =
                    arrow::StructArray::Make({expected}, target_type_->fields()).ValueOrDie();
                auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expect_array);
                CheckResult(apply_dv_batch_reader.get(), expected_chunk_array);
            }
            auto read_metrics = apply_dv_batch_reader->GetReaderMetrics();
            ASSERT_TRUE(read_metrics);
            apply_dv_batch_reader->Close();
        }
    }

 private:
    std::shared_ptr<arrow::DataType> int_type_;
    std::shared_ptr<arrow::DataType> target_type_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<Executor> executor_;
};

TEST_P(ApplyDeletionVectorBatchReaderTest, TestSimple) {
    std::string data_str = "[10, 11, 12, 13]";
    {
        std::vector<char> dv_data = {0, 1, 1, 0};
        CheckResult(data_str, dv_data, "[10, 13]");
    }
    {
        std::vector<char> dv_data = {1, 0, 0, 1};
        CheckResult(data_str, dv_data, "[11, 12]");
    }
    {
        std::vector<char> dv_data = {1, 1, 1, 1};
        // empty result
        CheckResult(data_str, dv_data, "");
    }
    {
        std::vector<char> dv_data = {0, 0, 0, 0};
        CheckResult(data_str, dv_data, "[10, 11, 12, 13]");
    }
}

TEST_P(ApplyDeletionVectorBatchReaderTest, TestSimple2) {
    std::string data_str = "[10, 11, 12, 13, 14, 15, 16]";
    {
        std::vector<char> dv_data = {0, 1, 1, 0, 1, 0, 0};
        CheckResult(data_str, dv_data, "[10, 13, 15, 16]");
    }
    {
        std::vector<char> dv_data = {0, 1, 1, 0, 0, 0, 1};
        CheckResult(data_str, dv_data, "[10, 13, 14, 15]");
    }
    {
        std::vector<char> dv_data = {1, 1, 1, 1, 0, 0, 1};
        CheckResult(data_str, dv_data, "[14, 15]");
    }

    {
        std::vector<char> dv_data = {1, 1, 1, 1, 1, 1, 1};
        // empty result
        CheckResult(data_str, dv_data, "");
    }
    {
        std::vector<char> dv_data = {0, 0, 0, 0, 0, 0, 0};
        CheckResult(data_str, dv_data, "[10, 11, 12, 13, 14, 15, 16]");
    }
}
INSTANTIATE_TEST_SUITE_P(EnablePrefetch, ApplyDeletionVectorBatchReaderTest,
                         ::testing::Values(false, true));
}  // namespace paimon::test
