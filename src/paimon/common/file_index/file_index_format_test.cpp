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
#include "paimon/file_index/file_index_format.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"
#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.h"
#include "paimon/common/file_index/empty/empty_file_index_reader.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class FileIndexFormatTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::Schema>& schema) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(FileIndexFormatTest, TestCreateEmptyFileIndexReader) {
    auto schema = arrow::schema({arrow::field("c1", arrow::utf8())});
    std::vector<char> index_file_bytes = {0,  5,  78, 78, -48, 26, 53,  -82, 0,   0,   0,   1,
                                          0,  0,  0,  47, 0,   0,  0,   1,   0,   2,   99,  49,
                                          0,  0,  0,  1,  0,   5,  101, 109, 112, 116, 121, -1,
                                          -1, -1, -1, 0,  0,   0,  0,   0,   0,   0,   0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_file_bytes.data(), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                         reader->ReadColumnIndex("c1", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, index_file_readers.size());
    auto* empty_reader = dynamic_cast<EmptyFileIndexReader*>(index_file_readers[0].get());
    ASSERT_TRUE(empty_reader);
}

TEST_F(FileIndexFormatTest, TestSimple) {
    auto schema =
        arrow::schema({arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::int32()),
                       arrow::field("non-exist", arrow::int32())});
    std::vector<uint8_t> index_file_bytes = {
        0,   5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,  0,   0,   96,  0,   0,
        0,   3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98, 105, 116, 109, 97,  112,
        0,   0,   0,   96,  0,   0,   0,   131, 0,   2,   102, 49,  0,  0,   0,   1,   0,   6,
        98,  105, 116, 109, 97,  112, 0,   0,   0,   227, 0,   0,   0,  74,  0,   2,   102, 50,
        0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,  0,   1,   45,  0,   0,
        0,   76,  0,   0,   0,   0,   1,   0,   0,   0,   8,   0,   0,  0,   5,   0,   0,   0,
        0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,  0,   4,   76,  117, 99,
        121, 255, 255, 255, 251, 0,   0,   0,   3,   66,  111, 98,  0,  0,   0,   20,  0,   0,
        0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 0,   0,  0,   4,   84,  111, 110,
        121, 0,   0,   0,   40,  58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   1,   0,   16,
        0,   0,   0,   0,   0,   7,   0,   58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   1,
        0,   16,  0,   0,   0,   1,   0,   5,   0,   58,  48,  0,   0,  1,   0,   0,   0,   0,
        0,   1,   0,   16,  0,   0,   0,   3,   0,   6,   0,   1,   0,  0,   0,   8,   0,   0,
        0,   2,   0,   0,   0,   0,   20,  0,   0,   0,   0,   0,   0,  0,   10,  0,   0,   0,
        22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,  16,  0,   0,   0,   4,
        0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   4,   0,   16,
        0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,  1,   0,   0,   0,   8,
        0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   0,  0,   0,   0,   0,   0,
        0,   0,   1,   0,   0,   0,   22,  58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   2,
        0,   16,  0,   0,   0,   2,   0,   3,   0,   6,   0,   58,  48, 0,   0,   1,   0,   0,
        0,   0,   0,   3,   0,   16,  0,   0,   0,   0,   0,   1,   0,  4,   0,   5,   0};
    auto input_stream = std::make_shared<ByteArrayInputStream>(
        reinterpret_cast<char*>(index_file_bytes.data()), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("f1", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, index_file_readers.size());
        auto* bitmap_reader = dynamic_cast<BitmapFileIndexReader*>(index_file_readers[0].get());
        ASSERT_TRUE(bitmap_reader);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(10)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{0,1,2,3,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(Literal(10)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(50)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{}", result->ToString());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("f2", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, index_file_readers.size());
        auto* bitmap_reader = dynamic_cast<BitmapFileIndexReader*>(index_file_readers[0].get());
        ASSERT_TRUE(bitmap_reader);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(0)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(1)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{0,1,4,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_TRUE(result);
            ASSERT_EQ("{7}", result->ToString());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("non-exist", CreateArrowSchema(schema).get()));
        ASSERT_TRUE(index_file_readers.empty());
    }
}

// NOLINTNEXTLINE(google-readability-function-size)
TEST_F(FileIndexFormatTest, TestBitmapIndexWithTimestamp) {
    auto schema = arrow::schema({
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Tokyo")),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Tokyo")),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Tokyo")),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Tokyo")),
    });

    auto fs = std::make_shared<LocalFileSystem>();
    std::string file_name = GetDataDir() +
                            "orc/timestamp_index.db/timestamp_index/"
                            "bucket-0/data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc.index";
    std::string index_file_bytes;
    ASSERT_OK(fs->ReadFile(file_name, &index_file_bytes));
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_file_bytes.data(), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input_stream, pool_));
    auto check_second = [&](const std::string& field_name) {
        // data: second
        // 1745542802000lms, 0ns
        // 1745542902000lms, 0ns
        // 1745542602000lms, 0ns
        // -1745000lms, 0ns
        // -1765000lms, 0ns
        // null
        // 1745542802000lms, 0ns
        // -1725000lms, 0ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542502000l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitIn({Literal(Timestamp(1745542802000l, 0)),
                                                         Literal(Timestamp(-1745000l, 0)),
                                                         Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802000l, 0)),
                                                        Literal(Timestamp(-1745000l, 0)),
                                                        Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502000l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802000l, 0)),
                                                      Literal(Timestamp(-1745000l, 0)),
                                                      Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotIn({Literal(Timestamp(1745542802000l, 0)),
                                                         Literal(Timestamp(-1745000l, 0)),
                                                         Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitGreaterThan(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745000l, 0))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 0))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
    };

    auto check_milli = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 0ns
        // 1745542902001lms, 0ns
        // 1745542602001lms, 0ns
        // -1745001lms, 0ns
        // -1765001lms, 0ns
        // null
        // 1745542802001lms, 0ns
        // -1725001lms, 0ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 0)),
                                                         Literal(Timestamp(-1745001l, 0)),
                                                         Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 0)),
                                                        Literal(Timestamp(-1745001l, 0)),
                                                        Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 0)),
                                                      Literal(Timestamp(-1745001l, 0)),
                                                      Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 0)),
                                                         Literal(Timestamp(-1745001l, 0)),
                                                         Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitGreaterThan(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 0))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 0))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
    };
    auto check_micro = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 1000ns
        // 1745542902001lms, 1000ns
        // 1745542602001lms, 1000ns
        // -1745001lms, 1000ns
        // -1765001lms, 1000ns
        // null
        // 1745542802001lms, 1000ns
        // -1725001lms, 1000ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 1000)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1000)),
                                                        Literal(Timestamp(-1745001l, 1000)),
                                                        Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 1000)),
                                                      Literal(Timestamp(-1745001l, 1000)),
                                                      Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1000)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterThan(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 1000))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 1000))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
    };

    auto check_nano = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 1001ns
        // 1745542902001lms, 1001ns
        // 1745542602001lms, 1001ns
        // -1745001lms, 1001ns
        // -1765001lms, 1001ns
        // null
        // 1745542802001lms, 1001ns
        // -1725001lms, 1001ns

        // as timestamp is normalized by micro seconds, there is a loss of precision in the
        // nanosecond part
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(
                                                  Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 1001)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1001)),
                                                        Literal(Timestamp(-1745001l, 1000)),
                                                        Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 1001)),
                                                      Literal(Timestamp(-1745001l, 1000)),
                                                      Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1001)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterThan(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 1000))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 1000))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
    };
    check_second("ts_sec");
    check_second("ts_tz_sec");

    check_milli("ts_milli");
    check_milli("ts_tz_milli");

    check_micro("ts_micro");
    check_micro("ts_tz_micro");

    check_nano("ts_nano");
    check_nano("ts_tz_nano");
}

}  // namespace paimon::test
