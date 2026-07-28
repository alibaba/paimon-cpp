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

#include "paimon/core/io/infer_shredding_file_writer.h"

#include <map>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_shredding_write_plan_factory.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/variant_test_data.h"

namespace paimon::test {

namespace {

/// A file-less writer standing in for the actual data file writer: it applies the shredding
/// conversion like the injected converter lambda would and collects the written batches.
class CollectingFileWriter : public SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> {
 public:
    CollectingFileWriter(const std::shared_ptr<ShreddingBatchConverter>& converter,
                         const std::shared_ptr<arrow::DataType>& logical_type,
                         std::vector<std::shared_ptr<arrow::Array>>* sink)
        : SingleFileWriter("", std::function<Status(::ArrowArray*, ::ArrowArray*)>()),
          converter_(converter),
          logical_type_(logical_type),
          sink_(sink) {}

    Status Write(::ArrowArray* record) override {
        record_count_ += record->length;
        std::shared_ptr<arrow::Array> array;
        if (converter_) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowArray> physical,
                                   converter_->Convert(record));
            auto physical_type = arrow::struct_(converter_->GetPhysicalSchema()->fields());
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(array,
                                              arrow::ImportArray(physical.get(), physical_type));
        } else {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(array, arrow::ImportArray(record, logical_type_));
        }
        sink_->push_back(std::move(array));
        return Status::OK();
    }

    Status Close() override {
        closed = true;
        return Status::OK();
    }

    Result<std::shared_ptr<DataFileMeta>> GetResult() override {
        return std::shared_ptr<DataFileMeta>(nullptr);
    }

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) override {
        return false;
    }

    Result<AbortExecutor> GetAbortExecutor() const override {
        return AbortExecutor(nullptr, "");
    }

    void Abort() override {}

    int64_t RecordCount() const override {
        return record_count_;
    }

    bool closed = false;

 private:
    std::shared_ptr<ShreddingBatchConverter> converter_;
    std::shared_ptr<arrow::DataType> logical_type_;
    std::vector<std::shared_ptr<arrow::Array>>* sink_;
    int64_t record_count_ = 0;
};

}  // namespace

class InferShreddingFileWriterTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        std::vector<DataField> fields = {DataField(1, arrow::field("id", arrow::int32())),
                                         DataField(2, VariantTypeUtils::ToArrowField("v"))};
        schema_ = DataField::ConvertDataFieldsToArrowSchema(fields);
        logical_type_ = arrow::struct_(schema_->fields());
    }

    std::shared_ptr<arrow::Array> BuildBatch(const std::vector<const char*>& jsons) {
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::StructArray> batch,
            VariantTestData::BuildVariantBatch(schema_->field(0), schema_->field(1), jsons, pool_));
        return batch;
    }

    Status WriteBatch(InferShreddingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>* w,
                      const std::vector<const char*>& jsons) {
        std::shared_ptr<arrow::Array> array = BuildBatch(jsons);
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        return w->Write(&c_array);
    }

    std::unique_ptr<InferShreddingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>
    MakeWriter(int32_t buffer_rows) {
        std::map<std::string, std::string> option_map = {
            {"variant.inferShreddingSchema", "true"},
            {"variant.shredding.maxInferBufferRow", std::to_string(buffer_rows)},
            // Keep the manifest format resolvable in test binaries without the avro plugin.
            {"manifest.format", "parquet"}};
        EXPECT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(option_map));
        auto plan_factory = VariantShreddingWritePlanFactory::Create(options, schema_, pool_);
        auto create_inner = [this](const std::shared_ptr<ShreddingBatchConverter>& converter)
            -> Result<
                std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
            captured_converters_.push_back(converter);
            auto writer = std::make_unique<CollectingFileWriter>(converter, logical_type_, &sink_);
            inner_ = writer.get();
            return std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
                std::move(writer));
        };
        return std::make_unique<
            InferShreddingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            schema_, plan_factory, "parquet", create_inner);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::DataType> logical_type_;
    // The converters own the arrow pool backing the collected arrays; keep them declared first
    // so the arrays are destroyed before the pool.
    std::vector<std::shared_ptr<ShreddingBatchConverter>> captured_converters_;
    std::vector<std::shared_ptr<arrow::Array>> sink_;
    CollectingFileWriter* inner_ = nullptr;
};

TEST_F(InferShreddingFileWriterTest, BuffersUntilThresholdThenReplays) {
    auto writer = MakeWriter(/*buffer_rows=*/4);
    // Never rolls while buffering.
    ASSERT_OK_AND_ASSIGN(bool reach, writer->ReachTargetSize(true, 1));
    ASSERT_FALSE(reach);

    ASSERT_OK(WriteBatch(
        writer.get(), {R"({"age": 35, "city": "Chicago"})", R"({"age": 25, "city": "Hangzhou"})"}));
    ASSERT_TRUE(sink_.empty());
    ASSERT_EQ(writer->RecordCount(), 2);

    // Crossing the row threshold finalizes the plan and replays the buffered batches.
    ASSERT_OK(WriteBatch(
        writer.get(), {R"({"age": 18, "city": "Beijing"})", R"({"age": 60, "city": "Shanghai"})"}));
    ASSERT_EQ(sink_.size(), 2);
    ASSERT_EQ(captured_converters_.size(), 1);
    ASSERT_NE(captured_converters_[0], nullptr);
    const auto& physical_type = static_cast<const arrow::StructType&>(*sink_[0]->type());
    const auto& variant_physical =
        static_cast<const arrow::StructType&>(*physical_type.GetFieldByName("v")->type());
    ASSERT_NE(variant_physical.GetFieldByName("typed_value"), nullptr);

    // Subsequent writes stream through the finalized writer directly.
    ASSERT_OK(WriteBatch(writer.get(), {R"({"age": 1, "city": "Suzhou"})"}));
    ASSERT_EQ(sink_.size(), 3);
    ASSERT_EQ(writer->RecordCount(), 5);

    ASSERT_OK(writer->Close());
    ASSERT_TRUE(inner_->closed);
}

TEST_F(InferShreddingFileWriterTest, CloseFlushesPartialBuffer) {
    auto writer = MakeWriter(/*buffer_rows=*/100);
    ASSERT_OK(WriteBatch(writer.get(), {R"({"age": 35, "city": "Chicago"})"}));
    ASSERT_TRUE(sink_.empty());
    ASSERT_OK(writer->Close());
    ASSERT_EQ(sink_.size(), 1);
    ASSERT_EQ(captured_converters_.size(), 1);
    ASSERT_NE(captured_converters_[0], nullptr);
    ASSERT_TRUE(inner_->closed);
}

TEST_F(InferShreddingFileWriterTest, EmptyFileUsesUntypedVariantPlan) {
    auto writer = MakeWriter(/*buffer_rows=*/4);
    ASSERT_OK(writer->Close());
    // Even without typed fields, inference creates the complete untyped Variant physical plan.
    ASSERT_EQ(captured_converters_.size(), 1);
    ASSERT_NE(captured_converters_[0], nullptr);
    const auto& physical_type = static_cast<const arrow::StructType&>(
        *captured_converters_[0]->GetPhysicalSchema()->GetFieldByName("v")->type());
    ASSERT_EQ(physical_type.GetFieldByName("typed_value"), nullptr);
    ASSERT_TRUE(sink_.empty());
    ASSERT_TRUE(inner_->closed);
}

}  // namespace paimon::test
