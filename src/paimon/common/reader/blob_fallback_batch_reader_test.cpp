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

#include "paimon/common/reader/blob_fallback_batch_reader.h"

#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/range.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

/// "PH" stands for a placeholder row (the sentinel bytes emitted by the placeholder-aware blob
/// reader), std::nullopt for null.
using BlobRows = std::vector<std::optional<std::string>>;

class BlobFallbackBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        struct_type_ = arrow::struct_({BlobUtils::ToArrowField("blob_col", true)});
        read_schema_ = arrow::schema(struct_type_->fields());
    }

    static std::string Sentinel() {
        return std::string(BlobDefs::PlaceholderSentinelView());
    }

    std::shared_ptr<arrow::Array> MakeBlobStruct(const BlobRows& rows) const {
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        for (const auto& row : rows) {
            EXPECT_TRUE(struct_builder.Append().ok());
            if (!row) {
                EXPECT_TRUE(blob_builder->AppendNull().ok());
            } else if (*row == "PH") {
                std::string sentinel = Sentinel();
                EXPECT_TRUE(blob_builder->Append(sentinel.data(), sentinel.size()).ok());
            } else {
                EXPECT_TRUE(blob_builder->Append(row->data(), row->size()).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    /// One segment of a group: File(rows) for a file segment, Gap(n) for a placeholder gap
    /// of n selected rows (with synthetic row ids when the schema does not read them, or the
    /// given ranges via GapRanges).
    struct SegmentSpec {
        std::vector<Range> gap_ranges;
        std::optional<BlobRows> file_rows;
        static SegmentSpec Gap(int64_t rows) {
            return SegmentSpec{{Range(0, rows - 1)}, std::nullopt};
        }
        static SegmentSpec GapRanges(std::vector<Range> ranges) {
            return SegmentSpec{std::move(ranges), std::nullopt};
        }
        static SegmentSpec File(BlobRows rows) {
            return SegmentSpec{{}, std::move(rows)};
        }
    };

    std::vector<BlobFallbackBatchReader::Segment> MakeGroup(const std::vector<SegmentSpec>& specs,
                                                            int32_t file_batch_size) const {
        std::vector<BlobFallbackBatchReader::Segment> segments;
        for (const auto& spec : specs) {
            if (spec.file_rows) {
                auto reader = std::make_unique<MockFileBatchReader>(MakeBlobStruct(*spec.file_rows),
                                                                    struct_type_, file_batch_size);
                segments.push_back(BlobFallbackBatchReader::Segment{std::move(reader), {}});
            } else {
                segments.push_back(BlobFallbackBatchReader::Segment{nullptr, spec.gap_ranges});
            }
        }
        return segments;
    }

    /// Runs the fallback over the groups with several batch sizes and compares to expected rows.
    void CheckFallback(const std::vector<std::vector<SegmentSpec>>& group_specs,
                       const BlobRows& expected_rows) const {
        auto expected_array = MakeBlobStruct(expected_rows);
        for (auto batch_size : arrow::internal::Iota(1, 8)) {
            for (auto file_batch_size : {1, 3, 1024}) {
                std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
                groups.reserve(group_specs.size());
                for (const auto& specs : group_specs) {
                    groups.push_back(MakeGroup(specs, file_batch_size));
                }
                ASSERT_OK_AND_ASSIGN(
                    auto reader, BlobFallbackBatchReader::Create(std::move(groups), read_schema_,
                                                                 batch_size, pool_));
                ASSERT_OK_AND_ASSIGN(
                    auto result, paimon::test::ReadResultCollector::CollectResult(reader.get()));
                reader->Close();
                auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
                ASSERT_TRUE(result->Equals(expected_chunk_array))
                    << "batch_size=" << batch_size << " file_batch_size=" << file_batch_size
                    << "\nresult: " << result->ToString()
                    << "\nexpected: " << expected_chunk_array->ToString();
            }
        }
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::DataType> struct_type_;
    std::shared_ptr<arrow::Schema> read_schema_;
};

TEST_F(BlobFallbackBatchReaderTest, TestBasicFallback) {
    // newer layer updates row 1 only; rows 0 and 2 fall back to the older layer
    CheckFallback(
        {{SegmentSpec::File({"PH", "u1", "PH"})}, {SegmentSpec::File({"b0", "b1", "b2"})}},
        {"b0", "u1", "b2"});
}

TEST_F(BlobFallbackBatchReaderTest, TestGapPadding) {
    // the newer layer only covers rows 2-3; the gaps stand for placeholders
    CheckFallback({{SegmentSpec::Gap(2), SegmentSpec::File({"u2", "PH"})},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3"})}},
                  {"b0", "b1", "u2", "b3"});
    // trailing gap
    CheckFallback({{SegmentSpec::File({"PH", "u1"}), SegmentSpec::Gap(2)},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3"})}},
                  {"b0", "u1", "b2", "b3"});
    // middle gap between two files of one layer
    CheckFallback({{SegmentSpec::File({"u0"}), SegmentSpec::Gap(2), SegmentSpec::File({"u3"})},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3"})}},
                  {"u0", "b1", "b2", "u3"});
    // a gap segment covering multiple disjoint selected ranges
    CheckFallback({{SegmentSpec::GapRanges({Range(0, 0), Range(2, 2)}), SegmentSpec::File({"u3"})},
                   {SegmentSpec::File({"b0", "b2", "b3"})}},
                  {"b0", "b2", "u3"});
}

TEST_F(BlobFallbackBatchReaderTest, TestAllPlaceholdersBecomesNull) {
    // a row that is a placeholder in every layer degrades to null
    CheckFallback({{SegmentSpec::File({"PH", "PH"})}, {SegmentSpec::File({"b0", "PH"})}},
                  {"b0", std::nullopt});
    CheckFallback({{SegmentSpec::Gap(2)}, {SegmentSpec::File({"PH", "PH"})}},
                  {std::nullopt, std::nullopt});
}

TEST_F(BlobFallbackBatchReaderTest, TestNullIsNotPlaceholder) {
    // a real null in a newer layer wins: null means "updated to null", not "not updated"
    CheckFallback({{SegmentSpec::File({std::nullopt, "u1"})}, {SegmentSpec::File({"b0", "b1"})}},
                  {std::nullopt, "u1"});
}

TEST_F(BlobFallbackBatchReaderTest, TestSentinelPrefixedValueIsNotPlaceholder) {
    // placeholders are identified by exact equality with the sentinel bytes only: a real value
    // that merely starts with them passes through unchanged, whether it falls back or wins as
    // the newest layer
    std::string prefixed = Sentinel() + "suffix";
    CheckFallback({{SegmentSpec::File({"PH", "u1"})}, {SegmentSpec::File({prefixed, "b1"})}},
                  {prefixed, "u1"});
    CheckFallback({{SegmentSpec::File({prefixed, "PH"})}, {SegmentSpec::File({"b0", "b1"})}},
                  {prefixed, "b1"});
}

TEST_F(BlobFallbackBatchReaderTest, TestThreeLayers) {
    CheckFallback({{SegmentSpec::File({"PH", "PH", "u2"})},
                   {SegmentSpec::File({"PH", "m1", "PH"})},
                   {SegmentSpec::File({"b0", "b1", "b2"})}},
                  {"b0", "m1", "u2"});
}

TEST_F(BlobFallbackBatchReaderTest, TestLayeredFilesAndGaps) {
    // mirrors the compacted-sequence-groups shape: layers partially cover [0, 9]
    CheckFallback(
        {{SegmentSpec::Gap(6), SegmentSpec::File({"u66", "PH"}), SegmentSpec::Gap(1),
          SegmentSpec::File({"u69"})},
         {SegmentSpec::File({"u40", "PH", "PH", "PH"}), SegmentSpec::Gap(4),
          SegmentSpec::File({"u48", "PH"})},
         {SegmentSpec::File({"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"})}},
        {"u40", "b1", "b2", "b3", "b4", "b5", "u66", "b7", "u48", "u69"});
}

TEST_F(BlobFallbackBatchReaderTest, TestRowTrackingFieldsPreserved) {
    // Row-tracking projections: resolved rows keep their layer's row id and sequence number;
    // an all-placeholder row keeps its row id (here provided by the newest group's gap
    // segment), reports -1 as its sequence number, and degrades the blob to null. Covers the
    // schema variants {blob, _ROW_ID, _SEQUENCE_NUMBER}, {blob, _ROW_ID} and
    // {blob, _SEQUENCE_NUMBER}.
    struct RowSpec {
        std::optional<std::string> blob;
        int64_t row_id;
        int64_t seq_num;
    };
    for (bool with_row_id : {true, false}) {
        for (bool with_seq_num : {true, false}) {
            if (!with_row_id && !with_seq_num) {
                continue;
            }
            arrow::FieldVector fields = {BlobUtils::ToArrowField("blob_col", true)};
            if (with_row_id) {
                fields.push_back(SpecialFields::RowId().field_);
            }
            if (with_seq_num) {
                fields.push_back(SpecialFields::SequenceNumber().field_);
            }
            auto struct_type = arrow::struct_(fields);
            auto schema = arrow::schema(fields);

            auto make_rows = [&](const std::vector<RowSpec>& rows) {
                std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders = {
                    std::make_shared<arrow::LargeBinaryBuilder>()};
                for (size_t i = 1; i < fields.size(); i++) {
                    field_builders.push_back(std::make_shared<arrow::Int64Builder>());
                }
                arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                                    std::move(field_builders));
                auto blob_builder =
                    static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
                for (const auto& row : rows) {
                    EXPECT_TRUE(struct_builder.Append().ok());
                    if (!row.blob) {
                        EXPECT_TRUE(blob_builder->AppendNull().ok());
                    } else if (*row.blob == "PH") {
                        std::string sentinel = Sentinel();
                        EXPECT_TRUE(blob_builder->Append(sentinel.data(), sentinel.size()).ok());
                    } else {
                        EXPECT_TRUE(blob_builder->Append(row.blob->data(), row.blob->size()).ok());
                    }
                    int32_t next_field = 1;
                    if (with_row_id) {
                        auto builder = static_cast<arrow::Int64Builder*>(
                            struct_builder.field_builder(next_field++));
                        EXPECT_TRUE(builder->Append(row.row_id).ok());
                    }
                    if (with_seq_num) {
                        auto builder = static_cast<arrow::Int64Builder*>(
                            struct_builder.field_builder(next_field));
                        EXPECT_TRUE(builder->Append(row.seq_num).ok());
                    }
                }
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(struct_builder.Finish(&array).ok());
                return array;
            };

            for (auto batch_size : arrow::internal::Iota(1, 5)) {
                for (auto file_batch_size : {1, 1024}) {
                    // newest layer (seq 20) covers only row 2; rows 0-1 are a gap
                    std::vector<BlobFallbackBatchReader::Segment> newest;
                    newest.push_back(BlobFallbackBatchReader::Segment{nullptr, {Range(0, 1)}});
                    newest.push_back(BlobFallbackBatchReader::Segment{
                        std::make_unique<MockFileBatchReader>(make_rows({{"u2", 2, 20}}),
                                                              struct_type, file_batch_size),
                        {}});
                    // oldest layer (seq 10) covers rows 0-2, row 1 is a placeholder there too
                    std::vector<BlobFallbackBatchReader::Segment> oldest;
                    oldest.push_back(BlobFallbackBatchReader::Segment{
                        std::make_unique<MockFileBatchReader>(
                            make_rows({{"b0", 0, 10}, {"PH", 1, 10}, {"PH", 2, 10}}), struct_type,
                            file_batch_size),
                        {}});
                    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
                    groups.push_back(std::move(newest));
                    groups.push_back(std::move(oldest));

                    ASSERT_OK_AND_ASSIGN(auto reader,
                                         BlobFallbackBatchReader::Create(std::move(groups), schema,
                                                                         batch_size, pool_));
                    ASSERT_OK_AND_ASSIGN(
                        auto result,
                        paimon::test::ReadResultCollector::CollectResult(reader.get()));
                    reader->Close();

                    // row 0 falls back to seq 10, row 1 is all-placeholder (null blob, row id
                    // kept, seq -1), row 2 takes seq 20
                    auto expected_array =
                        make_rows({{"b0", 0, 10}, {std::nullopt, 1, -1}, {"u2", 2, 20}});
                    auto expected_chunk_array =
                        std::make_shared<arrow::ChunkedArray>(expected_array);
                    ASSERT_TRUE(result->Equals(expected_chunk_array))
                        << "with_row_id=" << with_row_id << " with_seq_num=" << with_seq_num
                        << " batch_size=" << batch_size << " file_batch_size=" << file_batch_size
                        << "\nresult: " << result->ToString()
                        << "\nexpected: " << expected_chunk_array->ToString();
                }
            }
        }
    }
}

TEST_F(BlobFallbackBatchReaderTest, TestMisalignedGroupsFail) {
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
    groups.push_back(MakeGroup({SegmentSpec::File({"PH", "u1", "PH"})}, 1024));
    groups.push_back(MakeGroup({SegmentSpec::File({"b0", "b1"})}, 1024));
    ASSERT_OK_AND_ASSIGN(
        auto reader, BlobFallbackBatchReader::Create(std::move(groups), read_schema_, 1024, pool_));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "same number of rows");
}

TEST_F(BlobFallbackBatchReaderTest, TestCreateValidation) {
    // a single group needs no fallback
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> single_group;
    single_group.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    ASSERT_NOK_WITH_MSG(
        BlobFallbackBatchReader::Create(std::move(single_group), read_schema_, 1024, pool_),
        "at least two sequence groups");

    // the read schema must contain a blob field
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
    groups.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    groups.push_back(MakeGroup({SegmentSpec::File({"b1"})}, 1024));
    auto plain_schema =
        arrow::schema({arrow::field("not_blob", arrow::large_binary(), /*nullable=*/true)});
    ASSERT_NOK_WITH_MSG(
        BlobFallbackBatchReader::Create(std::move(groups), plain_schema, 1024, pool_),
        "should contain a blob field");

    // groups must not be empty
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> with_empty_group;
    with_empty_group.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    with_empty_group.emplace_back();
    ASSERT_NOK_WITH_MSG(
        BlobFallbackBatchReader::Create(std::move(with_empty_group), read_schema_, 1024, pool_),
        "should not be empty");

    // a gap segment must cover at least one selected row id
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> with_empty_gap;
    with_empty_gap.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    with_empty_gap.push_back(MakeGroup({SegmentSpec::GapRanges({})}, 1024));
    ASSERT_NOK_WITH_MSG(
        BlobFallbackBatchReader::Create(std::move(with_empty_gap), read_schema_, 1024, pool_),
        "at least one selected row id");
}

}  // namespace paimon::test
