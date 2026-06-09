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

#include "paimon/common/reader/blob_view_resolving_batch_reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobViewResolvingBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

    class InMemoryBatchReader : public BatchReader {
     public:
        explicit InMemoryBatchReader(const std::shared_ptr<arrow::StructArray>& struct_array)
            : struct_array_(struct_array) {
            if (!struct_array_) {
                exhausted_ = true;
            }
        }

        Result<ReadBatch> NextBatch() override {
            if (exhausted_) {
                return MakeEofBatch();
            }
            exhausted_ = true;
            auto c_array = std::make_unique<ArrowArray>();
            auto c_schema = std::make_unique<ArrowSchema>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*struct_array_, c_array.get(), c_schema.get()));
            return std::make_pair(std::move(c_array), std::move(c_schema));
        }

        std::shared_ptr<Metrics> GetReaderMetrics() const override {
            return std::make_shared<MetricsImpl>();
        }

        void Close() override {}

     private:
        std::shared_ptr<arrow::StructArray> struct_array_;
        bool exhausted_ = false;
    };

    std::string MakeBlobViewStructBytes(const std::string& database, const std::string& table,
                                        int32_t field_id, int64_t row_id) const {
        Identifier identifier(database, table);
        BlobViewStruct view_struct(identifier, field_id, row_id);
        auto bytes = view_struct.Serialize(pool_);
        return std::string(bytes->data(), bytes->size());
    }

    Result<std::string> MakeBlobDescriptorBytes(const std::string& uri, int64_t offset,
                                                int64_t length) const {
        PAIMON_ASSIGN_OR_RAISE(auto descriptor, BlobDescriptor::Create(uri, offset, length));
        auto bytes = descriptor->Serialize(pool_);
        return std::string(bytes->data(), bytes->size());
    }

    std::shared_ptr<arrow::StructArray> BuildStructArray(const std::vector<std::string>& values,
                                                         const std::vector<bool>& valid) const {
        arrow::LargeBinaryBuilder builder;
        EXPECT_TRUE(builder.Reserve(static_cast<int64_t>(values.size())).ok());
        for (size_t i = 0; i < values.size(); ++i) {
            if (!valid[i]) {
                EXPECT_TRUE(builder.AppendNull().ok());
            } else {
                EXPECT_TRUE(builder
                                .Append(reinterpret_cast<const uint8_t*>(values[i].data()),
                                        static_cast<int64_t>(values[i].size()))
                                .ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());

        arrow::FieldVector fields = {BlobUtils::ToArrowField("blob_col", /*nullable=*/true)};
        arrow::ArrayVector arrays = {array};
        auto result = arrow::StructArray::Make(arrays, fields).ValueOrDie();
        return result;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BlobViewResolvingBatchReaderTest, TestEofBatch) {
    auto inner_reader = std::make_unique<InMemoryBatchReader>(nullptr);
    auto resolver = BlobViewResolver([](const BlobViewStruct&) -> Result<std::shared_ptr<Bytes>> {
        return std::shared_ptr<Bytes>();
    });
    BlobViewResolvingBatchReader reader(std::move(inner_reader), {"blob_col"}, std::move(resolver),
                                        pool_);
    ASSERT_OK_AND_ASSIGN(auto batch, reader.NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST_F(BlobViewResolvingBatchReaderTest, TestEmptyReadBlobViewFields) {
    std::string view_bytes = MakeBlobViewStructBytes("db", "table", /*field_id=*/1, /*row_id=*/7);
    std::shared_ptr<arrow::StructArray> struct_array = BuildStructArray({view_bytes}, {true});

    bool resolver_called = false;
    auto resolver = BlobViewResolver(
        [&resolver_called](const BlobViewStruct&) -> Result<std::shared_ptr<Bytes>> {
            resolver_called = true;
            return std::shared_ptr<Bytes>();
        });

    auto inner_reader = std::make_unique<InMemoryBatchReader>(struct_array);
    BlobViewResolvingBatchReader reader(std::move(inner_reader), /*read_blob_view_fields=*/{},
                                        std::move(resolver), pool_);
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(&reader));
    auto expected_array = std::make_shared<arrow::ChunkedArray>(struct_array);
    ASSERT_TRUE(expected_array->Equals(*result_array));
    ASSERT_FALSE(resolver_called);
}

TEST_F(BlobViewResolvingBatchReaderTest, TestResolvesBlobViewColumn) {
    auto row0_view = MakeBlobViewStructBytes("db", "tbl", /*field_id=*/3, /*row_id=*/100);
    auto row1_view = MakeBlobViewStructBytes("db", "tbl", /*field_id=*/3, /*row_id=*/200);
    std::shared_ptr<arrow::StructArray> src_struct =
        BuildStructArray({row0_view, row1_view}, {true, true});

    ASSERT_OK_AND_ASSIGN(auto expected_row0_descriptor,
                         MakeBlobDescriptorBytes("/path/a", /*offset=*/0, /*length=*/8));
    ASSERT_OK_AND_ASSIGN(auto expected_row1_descriptor,
                         MakeBlobDescriptorBytes("/path/b", /*offset=*/16, /*length=*/32));

    auto resolver =
        BlobViewResolver([&](const BlobViewStruct& view_struct) -> Result<std::shared_ptr<Bytes>> {
            if (view_struct.RowId() == 100) {
                return std::make_shared<Bytes>(expected_row0_descriptor, pool_.get());
            }
            if (view_struct.RowId() == 200) {
                return std::make_shared<Bytes>(expected_row1_descriptor, pool_.get());
            }
            return Status::Invalid("unexpected view struct");
        });

    auto inner_reader = std::make_unique<InMemoryBatchReader>(src_struct);
    BlobViewResolvingBatchReader reader(std::move(inner_reader), {"blob_col"}, std::move(resolver),
                                        pool_);
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(&reader));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(result_array->chunk(0));

    auto result_blob_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
    ASSERT_FALSE(result_blob_column->IsNull(0));
    ASSERT_FALSE(result_blob_column->IsNull(1));
    ASSERT_EQ(result_blob_column->GetString(0), expected_row0_descriptor);
    ASSERT_EQ(result_blob_column->GetString(1), expected_row1_descriptor);
}

TEST_F(BlobViewResolvingBatchReaderTest, TestResolverError) {
    auto view_bytes = MakeBlobViewStructBytes("db", "tbl", /*field_id=*/1, /*row_id=*/5);
    std::shared_ptr<arrow::StructArray> src_struct = BuildStructArray({view_bytes}, {true});
    auto resolver = BlobViewResolver([](const BlobViewStruct&) -> Result<std::shared_ptr<Bytes>> {
        return Status::Invalid("cache miss");
    });
    auto inner_reader = std::make_unique<InMemoryBatchReader>(src_struct);
    BlobViewResolvingBatchReader reader(std::move(inner_reader), {"blob_col"}, std::move(resolver),
                                        pool_);
    ASSERT_NOK_WITH_MSG(reader.NextBatch(), "cache miss");
}

}  // namespace paimon::test
