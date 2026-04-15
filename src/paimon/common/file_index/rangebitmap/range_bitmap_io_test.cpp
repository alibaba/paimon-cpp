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

#include <gtest/gtest.h>

#include <memory>
#include <numeric>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {

class RangeBitmapIoTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        fs_ = dir_->GetFileSystem();
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        ASSERT_OK(fs_->Delete(dir_->Str()));
    }

    // Helper function to create writer, write data and return serialized bytes
    template <typename ArrowBuilder, typename ValueType>
    Result<PAIMON_UNIQUE_PTR<Bytes>> CreateIndexData(
        const std::shared_ptr<arrow::DataType>& arrow_type,
        const std::vector<ValueType>& test_data) {
        // Create Arrow array from test data
        auto builder = std::make_shared<ArrowBuilder>();
        for (const auto& value : test_data) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(value));
        }
        std::shared_ptr<arrow::Array> arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&arrow_array));

        // Wrap in StructArray
        arrow::FieldVector fields = {arrow::field("test_field", arrow_type)};
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> struct_array,
                                          arrow::StructArray::Make({arrow_array}, fields));
        auto c_array = std::make_unique<::ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));

        // Create schema
        const auto schema = arrow::schema({arrow::field("test_field", arrow_type)});

        // Create writer and write data
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RangeBitmapFileIndexWriter> writer,
                               RangeBitmapFileIndexWriter::Create(schema, "test_field", {}, pool_));

        PAIMON_RETURN_NOT_OK(writer->AddBatch(c_array.get()));
        return writer->SerializedBytes();
    }

 protected:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<paimon::FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
};

// Test normal read/write operations
TEST_F(RangeBitmapIoTest, TestSimple) {
    // Prepare test data
    std::vector<int32_t> test_data = {10, 20, 30, 40, 50};
    const auto& arrow_type = arrow::int32();

    // Create index data
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> serialized_bytes,
                         (CreateIndexData<arrow::Int32Builder, int32_t>(arrow_type, test_data)));
    ASSERT_TRUE(serialized_bytes);
    ASSERT_GT(serialized_bytes->size(), 0);

    // Write to file
    std::string file_path = dir_->Str() + "/range_bitmap_test.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(
        int32_t write_len,
        out->Write(reinterpret_cast<char*>(serialized_bytes->data()), serialized_bytes->size()));
    ASSERT_EQ(write_len, serialized_bytes->size());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    // Read from file and create reader
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<RangeBitmapFileIndexReader> reader,
        RangeBitmapFileIndexReader::Create(arrow_type, 0, serialized_bytes->size(), in, pool_));

    // Test equality query
    ASSERT_OK_AND_ASSIGN(auto eq_result, reader->VisitEqual(Literal(static_cast<int32_t>(30))));
    auto bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(eq_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap_result, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap_result);
    // Value 30 is at position 2
    ASSERT_TRUE(bitmap_result->Contains(2));
    ASSERT_FALSE(bitmap_result->Contains(0));
    ASSERT_FALSE(bitmap_result->Contains(1));

    // Test range query
    ASSERT_OK_AND_ASSIGN(auto gt_result,
                         reader->VisitGreaterThan(Literal(static_cast<int32_t>(25))));
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(gt_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap_result, bitmap_typed_result->GetBitmap());
    // Values > 25: 30, 40, 50 at positions 2, 3, 4
    ASSERT_TRUE(bitmap_result->Contains(2));
    ASSERT_TRUE(bitmap_result->Contains(3));
    ASSERT_TRUE(bitmap_result->Contains(4));
    ASSERT_FALSE(bitmap_result->Contains(0));
    ASSERT_FALSE(bitmap_result->Contains(1));

    ASSERT_OK(in->Close());
}

// Test I/O exceptions using IOHook
TEST_F(RangeBitmapIoTest, TestIOException) {
    bool run_complete = false;
    auto io_hook = paimon::IOHook::GetInstance();

    for (size_t i = 0; i < 200; i++) {
        auto test_dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(test_dir);
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, paimon::IOHook::Mode::RETURN_ERROR);

        // Prepare test data
        std::vector<int32_t> test_data = {10, 20, 30, 40, 50};
        const auto& arrow_type = arrow::int32();

        // Create index data
        auto serialized_bytes_result =
            CreateIndexData<arrow::Int32Builder, int32_t>(arrow_type, test_data);
        CHECK_HOOK_STATUS(serialized_bytes_result.status(), i);
        PAIMON_UNIQUE_PTR<Bytes> serialized_bytes = std::move(serialized_bytes_result).value();

        // Write to file
        std::string file_path = test_dir->Str() + "/range_bitmap_io_exception_test.data";

        auto out_result = fs_->Create(file_path, /*overwrite=*/false);
        CHECK_HOOK_STATUS(out_result.status(), i);
        std::shared_ptr<OutputStream> out = std::move(out_result).value();

        auto write_result =
            out->Write(reinterpret_cast<char*>(serialized_bytes->data()), serialized_bytes->size());
        CHECK_HOOK_STATUS(write_result.status(), i);

        CHECK_HOOK_STATUS(out->Flush(), i);
        CHECK_HOOK_STATUS(out->Close(), i);

        // Read from file and create reader
        auto in_result = fs_->Open(file_path);
        CHECK_HOOK_STATUS(in_result.status(), i);
        std::shared_ptr<InputStream> in = std::move(in_result).value();

        auto reader_result =
            RangeBitmapFileIndexReader::Create(arrow_type, 0, serialized_bytes->size(), in, pool_);
        CHECK_HOOK_STATUS(reader_result.status(), i);
        std::shared_ptr<RangeBitmapFileIndexReader> reader = std::move(reader_result).value();

        // Test query
        auto eq_result = reader->VisitEqual(Literal(static_cast<int32_t>(30)));
        CHECK_HOOK_STATUS(eq_result.status(), i);
        auto bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(eq_result.value());
        ASSERT_TRUE(bitmap_typed_result);
        auto bitmap_result = bitmap_typed_result->GetBitmap();
        CHECK_HOOK_STATUS(bitmap_result.status(), i);
        ASSERT_TRUE(bitmap_result.value()->Contains(2));  // Value 30 is at position 2

        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

// Test Java compatibility - read java-generated index file
// data: [1, 3, 5, 7, 9, null, null, 10]
TEST_F(RangeBitmapIoTest, TestJavaCompatibility) {
    // Load pre-generated range bitmap index file
    std::string index_file = GetDataDir() + "/file_index/rangebitmap/rangebitmap.index";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(index_file));
    ASSERT_OK_AND_ASSIGN(int64_t file_size, in->Length());
    ASSERT_GT(file_size, 0);

    // Create reader from the index file (int32 type)
    const auto& arrow_type = arrow::int32();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RangeBitmapFileIndexReader> reader,
                         RangeBitmapFileIndexReader::Create(
                             arrow_type, 0, static_cast<int32_t>(file_size), in, pool_));

    // Test equality query - value 1 at position 0
    ASSERT_OK_AND_ASSIGN(auto eq_result, reader->VisitEqual(Literal(static_cast<int32_t>(1))));
    auto bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(eq_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(0));  // value 1 at position 0
    ASSERT_FALSE(bitmap->Contains(1));

    // Test equality query - value 5 at position 2
    ASSERT_OK_AND_ASSIGN(eq_result, reader->VisitEqual(Literal(static_cast<int32_t>(5))));
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(eq_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(2));  // value 5 at position 2

    // Test equality query - value 10 at position 7
    ASSERT_OK_AND_ASSIGN(eq_result, reader->VisitEqual(Literal(static_cast<int32_t>(10))));
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(eq_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(7));  // value 10 at position 7

    // Test greater than query - values > 5: 7, 9, 10 at positions 3, 4, 7
    ASSERT_OK_AND_ASSIGN(auto gt_result,
                         reader->VisitGreaterThan(Literal(static_cast<int32_t>(5))));
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(gt_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(3));   // 7
    ASSERT_TRUE(bitmap->Contains(4));   // 9
    ASSERT_TRUE(bitmap->Contains(7));   // 10
    ASSERT_FALSE(bitmap->Contains(0));  // 1
    ASSERT_FALSE(bitmap->Contains(2));  // 5

    // Test less than query - values < 5: 1, 3 at positions 0, 1
    ASSERT_OK_AND_ASSIGN(auto lt_result, reader->VisitLessThan(Literal(static_cast<int32_t>(5))));
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(lt_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(0));   // 1
    ASSERT_TRUE(bitmap->Contains(1));   // 3
    ASSERT_FALSE(bitmap->Contains(2));  // 5
    ASSERT_FALSE(bitmap->Contains(7));  // 10

    // Test is_not_null query - non-null positions: 0, 1, 2, 3, 4, 7
    ASSERT_OK_AND_ASSIGN(auto not_null_result, reader->VisitIsNotNull());
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(not_null_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(0));
    ASSERT_TRUE(bitmap->Contains(1));
    ASSERT_TRUE(bitmap->Contains(2));
    ASSERT_TRUE(bitmap->Contains(3));
    ASSERT_TRUE(bitmap->Contains(4));
    ASSERT_TRUE(bitmap->Contains(7));
    ASSERT_FALSE(bitmap->Contains(5));  // null
    ASSERT_FALSE(bitmap->Contains(6));  // null

    // Test is_null query - null positions: 5, 6
    ASSERT_OK_AND_ASSIGN(auto null_result, reader->VisitIsNull());
    bitmap_typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(null_result);
    ASSERT_TRUE(bitmap_typed_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_typed_result->GetBitmap());
    ASSERT_TRUE(bitmap);
    ASSERT_TRUE(bitmap->Contains(5));  // null at position 5
    ASSERT_TRUE(bitmap->Contains(6));  // null at position 6
    ASSERT_FALSE(bitmap->Contains(0));
    ASSERT_FALSE(bitmap->Contains(7));

    ASSERT_OK(in->Close());
}

}  // namespace paimon::test
