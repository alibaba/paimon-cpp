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

#include "paimon/common/global_index/btree/lazy_filtered_btree_reader.h"

#include <numeric>
#include <string>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_global_indexer.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FakeLazyFileWriter : public GlobalIndexFileWriter {
 public:
    FakeLazyFileWriter(const std::shared_ptr<FileSystem>& fs, const std::string& base_path)
        : fs_(fs), base_path_(base_path) {}

    Result<std::string> NewFileName(const std::string& prefix) const override {
        return prefix + "_" + std::to_string(file_counter_++);
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_name) const override {
        return fs_->Create(base_path_ + "/" + file_name, true);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(base_path_ + "/" + file_name));
        return static_cast<int64_t>(file_status->GetLen());
    }

    std::string ToPath(const std::string& file_name) const override {
        return base_path_ + "/" + file_name;
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
    mutable int64_t file_counter_ = 0;
};

class FakeLazyFileReader : public GlobalIndexFileReader {
 public:
    FakeLazyFileReader(const std::shared_ptr<FileSystem>& fs, const std::string& base_path)
        : fs_(fs), base_path_(base_path) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        return fs_->Open(file_path);
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
};

class LazyFilteredBTreeReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        test_dir_ = UniqueTestDirectory::Create("local");
        pool_ = GetDefaultPool();
        fs_ = test_dir_->GetFileSystem();
        base_path_ = test_dir_->Str();
        file_writer_ = std::make_shared<FakeLazyFileWriter>(fs_, base_path_);

        // Create 3 btree index files with different key ranges:
        //
        // file1: keys [1, 1, null, 2, 2], row_ids [0, 1, 2, 3, 4]
        //        range [1, 2], has_null=true
        //
        // file2: keys [5, 6, 7], row_ids [5, 6, 7]
        //        range [5, 7], has_null=false
        //
        // file3: keys [10, null, 12], row_ids [8, 9, 10]
        //        range [10, 12], has_null=true
        all_metas_ = {};
        WriteSingleFile(R"([[1],[1],[null],[2],[2]])", {0, 1, 2, 3, 4});
        WriteSingleFile(R"([[5],[6],[7]])", {5, 6, 7});
        WriteSingleFile(R"([[10],[null],[12]])", {8, 9, 10});
    }

    void WriteSingleFile(const std::string& json_data, const std::vector<int64_t>& row_ids) {
        auto c_schema = CreateArrowSchema();
        std::map<std::string, std::string> options = {{BtreeDefs::kBtreeIndexBlockSize, "4096"},
                                                      {BtreeDefs::kBtreeIndexCompression, "NONE"}};
        ASSERT_OK_AND_ASSIGN(auto indexer, BTreeGlobalIndexer::Create(options));
        ASSERT_OK_AND_ASSIGN(
            auto writer, indexer->CreateWriter("int_field", c_schema.get(), file_writer_, pool_));
        auto array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::struct_({arrow::field("int_field", arrow::int32())}), json_data)
                         .ValueOrDie();
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());

        ASSERT_OK(writer->AddBatch(&c_array, std::vector<int64_t>(row_ids)));
        ASSERT_OK_AND_ASSIGN(auto metas, writer->Finish());
        ASSERT_EQ(metas.size(), 1);
        all_metas_.push_back(metas[0]);
    }

    std::unique_ptr<ArrowSchema> CreateArrowSchema() const {
        auto schema = arrow::schema({arrow::field("int_field", arrow::int32())});
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    std::shared_ptr<LazyFilteredBTreeReader> CreateReader(
        const std::shared_ptr<Executor>& executor = nullptr) const {
        auto file_reader = std::make_shared<FakeLazyFileReader>(fs_, base_path_);
        auto cache_manager = std::make_shared<CacheManager>(1024 * 1024, 0.5);
        return std::make_shared<LazyFilteredBTreeReader>(/*read_buffer_size=*/std::nullopt,
                                                         all_metas_, arrow::int32(), file_reader,
                                                         cache_manager, pool_, executor);
    }

    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected) const {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    }

    void CheckEmpty(const std::shared_ptr<GlobalIndexResult>& result) const {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_TRUE(bitmap->IsEmpty());
    }

 private:
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
    std::shared_ptr<FakeLazyFileWriter> file_writer_;
    std::vector<GlobalIndexIOMeta> all_metas_;
};

// --- VisitEqual ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitEqualHitSingleFile) {
    auto reader = CreateReader();
    // key=1 is in file1 only -> rows 0, 1
    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_1));
    CheckResult(result, {0, 1});
}

TEST_F(LazyFilteredBTreeReaderTest, TestVisitEqualHitDifferentFile) {
    auto reader = CreateReader();
    // key=6 is in file2 only -> row 6
    Literal literal_6(6);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_6));
    CheckResult(result, {6});
}

TEST_F(LazyFilteredBTreeReaderTest, TestVisitEqualNoMatch) {
    auto reader = CreateReader();
    // key=99 is out of all ranges -> empty bitmap
    Literal literal_99(99);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_99));
    CheckEmpty(result);
}

// --- VisitNotEqual ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitNotEqual) {
    auto reader = CreateReader();
    // NotEqual 5 -> all non-null rows except row 5
    // non-null rows: 0,1,3,4 (file1) + 5,6,7 (file2) + 8,10 (file3)
    // minus row 5 (key=5) -> 0,1,3,4,6,7,8,10
    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(literal_5));
    CheckResult(result, {0, 1, 3, 4, 6, 7, 8, 10});
}

// --- VisitLessThan ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitLessThan) {
    auto reader = CreateReader();
    // LessThan 5 -> keys 1,2 from file1 -> rows 0,1,3,4
    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal_5));
    CheckResult(result, {0, 1, 3, 4});
}

TEST_F(LazyFilteredBTreeReaderTest, TestVisitLessThanNoMatch) {
    auto reader = CreateReader();
    // LessThan 1 -> no key < 1 -> empty
    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal_1));
    CheckEmpty(result);
}

// --- VisitLessOrEqual ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitLessOrEqual) {
    auto reader = CreateReader();
    // LessOrEqual 5 -> keys 1,2 from file1 + key 5 from file2 -> rows 0,1,3,4,5
    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(literal_5));
    CheckResult(result, {0, 1, 3, 4, 5});
}

// --- VisitGreaterThan ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitGreaterThan) {
    auto reader = CreateReader();
    // GreaterThan 7 -> keys 10,12 from file3 -> rows 8,10
    Literal literal_7(7);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(literal_7));
    CheckResult(result, {8, 10});
}

TEST_F(LazyFilteredBTreeReaderTest, TestVisitGreaterThanNoMatch) {
    auto reader = CreateReader();
    // GreaterThan 12 -> no key > 12 -> empty
    Literal literal_12(12);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(literal_12));
    CheckEmpty(result);
}

// --- VisitGreaterOrEqual ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitGreaterOrEqual) {
    auto reader = CreateReader();
    // GreaterOrEqual 6 -> keys 6,7 from file2 + keys 10,12 from file3 -> rows 6,7,8,10
    Literal literal_6(6);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(literal_6));
    CheckResult(result, {6, 7, 8, 10});
}

// --- VisitIn ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitInAcrossFiles) {
    auto reader = CreateReader();
    // IN(2, 7) -> file1 has key 2 (rows 3,4), file2 has key 7 (row 7)
    std::vector<Literal> literals = {Literal(2), Literal(7)};
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(literals));
    CheckResult(result, {3, 4, 7});
}

TEST_F(LazyFilteredBTreeReaderTest, TestVisitInNoMatch) {
    auto reader = CreateReader();
    // IN(99, 100) -> out of all ranges -> empty
    std::vector<Literal> literals = {Literal(99), Literal(100)};
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(literals));
    CheckEmpty(result);
}

// --- VisitNotIn ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitNotIn) {
    auto reader = CreateReader();
    // NotIn(1, 5, 10) -> all non-null rows minus rows with key=1,5,10
    // non-null rows: 0,1,3,4 + 5,6,7 + 8,10
    // remove key=1 (rows 0,1), key=5 (row 5), key=10 (row 8)
    // -> 3,4,6,7,10
    std::vector<Literal> literals = {Literal(1), Literal(5), Literal(10)};
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotIn(literals));
    CheckResult(result, {3, 4, 6, 7, 10});
}

// --- VisitIsNull ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitIsNull) {
    auto reader = CreateReader();
    // file1 has_null=true (row 2), file3 has_null=true (row 9)
    // file2 has no nulls
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
    CheckResult(result, {2, 9});
}

// --- VisitIsNotNull ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitIsNotNull) {
    auto reader = CreateReader();
    // All non-null rows across all files: 0,1,3,4 + 5,6,7 + 8,10
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
    CheckResult(result, {0, 1, 3, 4, 5, 6, 7, 8, 10});
}

// --- VisitVectorSearch / VisitFullTextSearch ---

TEST_F(LazyFilteredBTreeReaderTest, TestVisitVectorSearchNotSupported) {
    auto reader = CreateReader();
    ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(nullptr),
                        "LazyFilteredBTreeReader does not support vector search");
}

TEST_F(LazyFilteredBTreeReaderTest, TestVisitFullTextSearchNotSupported) {
    auto reader = CreateReader();
    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(nullptr),
                        "LazyFilteredBTreeReader does not support full text search");
}

// --- GetIndexType / IsThreadSafe ---

TEST_F(LazyFilteredBTreeReaderTest, TestGetIndexType) {
    auto reader = CreateReader();
    ASSERT_EQ(reader->GetIndexType(), BtreeDefs::kIdentifier);
}

TEST_F(LazyFilteredBTreeReaderTest, TestIsNotThreadSafe) {
    auto reader = CreateReader();
    ASSERT_FALSE(reader->IsThreadSafe());
}

// --- Reader cache verification ---

TEST_F(LazyFilteredBTreeReaderTest, TestReaderCacheReuse) {
    auto reader = CreateReader();
    // Call VisitEqual twice on the same file's range to verify reader cache works
    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result1, reader->VisitEqual(literal_1));
    CheckResult(result1, {0, 1});

    Literal literal_2(2);
    ASSERT_OK_AND_ASSIGN(auto result2, reader->VisitEqual(literal_2));
    CheckResult(result2, {3, 4});
}

// --- Empty files list ---

TEST_F(LazyFilteredBTreeReaderTest, TestEmptyFilesList) {
    std::vector<GlobalIndexIOMeta> empty_metas;
    auto file_reader = std::make_shared<FakeLazyFileReader>(fs_, base_path_);
    auto cache_manager = std::make_shared<CacheManager>(1024 * 1024, 0.5);
    auto reader = std::make_shared<LazyFilteredBTreeReader>(
        /*read_buffer_size=*/std::nullopt, empty_metas, arrow::int32(), file_reader, cache_manager,
        pool_, /*executor=*/nullptr);

    // Any query on empty files should return empty bitmap
    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_1));
    CheckEmpty(result);

    ASSERT_OK_AND_ASSIGN(result, reader->VisitIsNotNull());
    CheckEmpty(result);
}

// ============================================================================
// Parallel execution with Executor
// ============================================================================

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitEqual) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // key=1 is in file1 -> rows 0, 1
    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_1));
    CheckResult(result, {0, 1});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitNotEqual) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // NotEqual 5 -> all non-null rows except row 5 -> 0,1,3,4,6,7,8,10
    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(literal_5));
    CheckResult(result, {0, 1, 3, 4, 6, 7, 8, 10});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitLessThan) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // LessThan 5 -> keys 1,2 from file1 -> rows 0,1,3,4
    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal_5));
    CheckResult(result, {0, 1, 3, 4});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitGreaterOrEqual) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // GreaterOrEqual 6 -> keys 6,7 from file2 + keys 10,12 from file3 -> rows 6,7,8,10
    Literal literal_6(6);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(literal_6));
    CheckResult(result, {6, 7, 8, 10});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitIn) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // IN(2, 7) -> file1 has key 2 (rows 3,4), file2 has key 7 (row 7)
    std::vector<Literal> literals = {Literal(2), Literal(7)};
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(literals));
    CheckResult(result, {3, 4, 7});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitIsNull) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // file1 has_null=true (row 2), file3 has_null=true (row 9)
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
    CheckResult(result, {2, 9});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitIsNotNull) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // All non-null rows across all files: 0,1,3,4 + 5,6,7 + 8,10
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
    CheckResult(result, {0, 1, 3, 4, 5, 6, 7, 8, 10});
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelVisitEqualNoMatch) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto reader = CreateReader(executor);
    // key=99 out of range -> empty bitmap
    Literal literal_99(99);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_99));
    CheckEmpty(result);
}

TEST_F(LazyFilteredBTreeReaderTest, TestParallelEmptyFilesList) {
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    std::vector<GlobalIndexIOMeta> empty_metas;
    auto file_reader = std::make_shared<FakeLazyFileReader>(fs_, base_path_);
    auto cache_manager = std::make_shared<CacheManager>(1024 * 1024, 0.5);
    auto reader = std::make_shared<LazyFilteredBTreeReader>(
        /*read_buffer_size=*/std::nullopt, empty_metas, arrow::int32(), file_reader, cache_manager,
        pool_, executor);
    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal_1));
    CheckEmpty(result);
}

}  // namespace paimon::test
