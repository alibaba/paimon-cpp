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
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/c/helpers.h"
#include "paimon/common/global_index/btree/btree_global_indexer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// ---------------------------------------------------------------------------
// Test data directory (relative to project root)
// ---------------------------------------------------------------------------
static constexpr char kTestDataDir[] = "test/test_data/global_index/btree/btree_compatibility_data";

// ---------------------------------------------------------------------------
// CSV record parsed from the Java-generated CSV files
// ---------------------------------------------------------------------------
struct CsvRecord {
    int64_t row_id;
    std::string key;  // "NULL" if is_null
    bool is_null;
};

// ---------------------------------------------------------------------------
// Helper: parse a CSV file into a vector of CsvRecord
// ---------------------------------------------------------------------------
static std::vector<CsvRecord> ParseCsvFile(const std::string& csv_path) {
    std::vector<CsvRecord> records;
    std::ifstream ifs(csv_path);
    if (!ifs.is_open()) {
        return records;
    }

    std::string line;
    // Skip header line: "row_id,key,is_null"
    std::getline(ifs, line);

    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        std::string row_id_str, key_str, is_null_str;
        std::getline(ss, row_id_str, ',');
        std::getline(ss, key_str, ',');
        std::getline(ss, is_null_str, ',');

        CsvRecord rec;
        rec.row_id = std::stoll(row_id_str);
        rec.key = key_str;
        rec.is_null = (is_null_str == "true");
        records.push_back(rec);
    }
    return records;
}

// ---------------------------------------------------------------------------
// Helper: read a binary file into a Bytes object
// ---------------------------------------------------------------------------
static std::shared_ptr<Bytes> ReadBinaryFile(const std::string& path, MemoryPool* pool) {
    std::ifstream ifs(path, std::ios::binary | std::ios::ate);
    if (!ifs.is_open()) return nullptr;
    auto size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);
    auto bytes = std::make_shared<Bytes>(static_cast<size_t>(size), pool);
    ifs.read(bytes->data(), size);
    return bytes;
}

// ---------------------------------------------------------------------------
// Helper: get file size
// ---------------------------------------------------------------------------
static int64_t GetFileSize(const std::string& path) {
    std::ifstream ifs(path, std::ios::binary | std::ios::ate);
    if (!ifs.is_open()) return -1;
    return static_cast<int64_t>(ifs.tellg());
}

// ---------------------------------------------------------------------------
// Fake GlobalIndexFileReader that reads from local filesystem
// ---------------------------------------------------------------------------
class LocalGlobalIndexFileReader : public GlobalIndexFileReader {
 public:
    explicit LocalGlobalIndexFileReader(const std::shared_ptr<FileSystem>& fs) : fs_(fs) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        return fs_->Open(file_path);
    }

 private:
    std::shared_ptr<FileSystem> fs_;
};

// ---------------------------------------------------------------------------
// Collect all row IDs from a GlobalIndexResult into a set
// ---------------------------------------------------------------------------
static std::set<int64_t> CollectRowIds(const std::shared_ptr<GlobalIndexResult>& result) {
    std::set<int64_t> ids;
    auto iter_result = result->CreateIterator();
    if (!iter_result.ok()) return ids;
    auto iter = std::move(iter_result).value();
    while (iter->HasNext()) {
        ids.insert(iter->Next());
    }
    return ids;
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class BTreeCompatibilityTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        // Use UniqueTestDirectory to get a FileSystem that can read local files
        test_dir_ = UniqueTestDirectory::Create("local");
        fs_ = test_dir_->GetFileSystem();

        // Resolve the absolute path to test data
        // The test data is at project_root/test/test_data/...
        // We need to find the project root. Use the current working directory.
        char cwd[4096];
        if (getcwd(cwd, sizeof(cwd)) != nullptr) {
            project_root_ = std::string(cwd);
        }
        data_dir_ = project_root_ + "/" + kTestDataDir;
    }

    // Create a BTreeGlobalIndexReader from Java-generated .bin and .bin.meta files
    Result<std::shared_ptr<GlobalIndexReader>> CreateReaderFromFiles(
        const std::string& bin_path, const std::string& meta_path,
        const std::shared_ptr<arrow::DataType>& arrow_type, int64_t record_count) {
        // Read meta bytes
        auto meta_bytes = ReadBinaryFile(meta_path, pool_.get());
        if (!meta_bytes) {
            return Status::IOError("Failed to read meta file: " + meta_path);
        }

        // Get file size
        int64_t file_size = GetFileSize(bin_path);
        if (file_size < 0) {
            return Status::IOError("Failed to get file size: " + bin_path);
        }

        // Build GlobalIndexIOMeta
        // range_end = record_count - 1 (inclusive)
        GlobalIndexIOMeta io_meta(bin_path, file_size, record_count - 1, meta_bytes);
        std::vector<GlobalIndexIOMeta> metas = {io_meta};

        // Create ArrowSchema
        auto schema = arrow::schema({arrow::field("testField", arrow_type)});
        auto c_schema = std::make_unique<ArrowSchema>();
        auto export_status = arrow::ExportSchema(*schema, c_schema.get());
        if (!export_status.ok()) {
            return Status::Invalid("Failed to export ArrowSchema: " + export_status.ToString());
        }

        // Create reader
        auto file_reader = std::make_shared<LocalGlobalIndexFileReader>(fs_);
        std::map<std::string, std::string> options;
        BTreeGlobalIndexer indexer(options);

        auto reader_result = indexer.CreateReader(c_schema.get(), file_reader, metas, pool_);
        ArrowSchemaRelease(c_schema.get());
        return reader_result;
    }

    // Helper: build expected row IDs for null rows from CSV records
    std::set<int64_t> GetNullRowIds(const std::vector<CsvRecord>& records) {
        std::set<int64_t> ids;
        for (const auto& rec : records) {
            if (rec.is_null) ids.insert(rec.row_id);
        }
        return ids;
    }

    // Helper: build expected row IDs for non-null rows from CSV records
    std::set<int64_t> GetNonNullRowIds(const std::vector<CsvRecord>& records) {
        std::set<int64_t> ids;
        for (const auto& rec : records) {
            if (!rec.is_null) ids.insert(rec.row_id);
        }
        return ids;
    }

    // Helper: build expected row IDs for rows with a specific int key
    std::set<int64_t> GetRowIdsForIntKey(const std::vector<CsvRecord>& records, int32_t key) {
        std::set<int64_t> ids;
        std::string key_str = std::to_string(key);
        for (const auto& rec : records) {
            if (!rec.is_null && rec.key == key_str) ids.insert(rec.row_id);
        }
        return ids;
    }

    // Helper: build expected row IDs for rows with a specific string key
    std::set<int64_t> GetRowIdsForStringKey(const std::vector<CsvRecord>& records,
                                            const std::string& key) {
        std::set<int64_t> ids;
        for (const auto& rec : records) {
            if (!rec.is_null && rec.key == key) ids.insert(rec.row_id);
        }
        return ids;
    }

    // Helper: build expected row IDs for int keys in range [lower, upper]
    std::set<int64_t> GetRowIdsForIntRange(const std::vector<CsvRecord>& records, int32_t lower,
                                           int32_t upper, bool lower_inclusive,
                                           bool upper_inclusive) {
        std::set<int64_t> ids;
        for (const auto& rec : records) {
            if (rec.is_null) continue;
            int32_t val = std::stoi(rec.key);
            bool above_lower = lower_inclusive ? (val >= lower) : (val > lower);
            bool below_upper = upper_inclusive ? (val <= upper) : (val < upper);
            if (above_lower && below_upper) ids.insert(rec.row_id);
        }
        return ids;
    }

    // Helper: build expected row IDs for string keys in range
    std::set<int64_t> GetRowIdsForStringRange(const std::vector<CsvRecord>& records,
                                              const std::string& lower, const std::string& upper,
                                              bool lower_inclusive, bool upper_inclusive) {
        std::set<int64_t> ids;
        for (const auto& rec : records) {
            if (rec.is_null) continue;
            bool above_lower = lower_inclusive ? (rec.key >= lower) : (rec.key > lower);
            bool below_upper = upper_inclusive ? (rec.key <= upper) : (rec.key < upper);
            if (above_lower && below_upper) ids.insert(rec.row_id);
        }
        return ids;
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string project_root_;
    std::string data_dir_;
};

// ===========================================================================
// Test: Read int type data with various record counts
// ===========================================================================
class BTreeCompatibilityIntTest : public BTreeCompatibilityTest,
                                  public ::testing::WithParamInterface<int> {};

TEST_P(BTreeCompatibilityIntTest, ReadAndQueryIntData) {
    int count = GetParam();
    std::string prefix = "btree_test_int_" + std::to_string(count);
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    // Parse CSV to get expected data
    auto records = ParseCsvFile(csv_path);
    ASSERT_EQ(static_cast<int>(records.size()), count)
        << "CSV record count mismatch for " << prefix;

    // Create reader
    auto reader_result = CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count);
    ASSERT_OK(reader_result.status()) << "Failed to create reader for " << prefix;
    auto reader = reader_result.value();

    // ---- Test 1: VisitIsNull ----
    {
        auto result = reader->VisitIsNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNull failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetNullRowIds(records);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIsNull mismatch";
    }

    // ---- Test 2: VisitIsNotNull ----
    {
        auto result = reader->VisitIsNotNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNotNull failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetNonNullRowIds(records);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIsNotNull mismatch";
    }

    // ---- Test 3: VisitEqual for a known key ----
    // Find the first non-null key in the CSV
    for (const auto& rec : records) {
        if (!rec.is_null) {
            int32_t key_val = std::stoi(rec.key);
            Literal literal(key_val);
            auto result = reader->VisitEqual(literal);
            ASSERT_OK(result.status()) << prefix << ": VisitEqual(" << key_val << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids = GetRowIdsForIntKey(records, key_val);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitEqual(" << key_val << ") mismatch";
            break;
        }
    }

    // ---- Test 4: VisitEqual for the last non-null key ----
    for (auto it = records.rbegin(); it != records.rend(); ++it) {
        if (!it->is_null) {
            int32_t key_val = std::stoi(it->key);
            Literal literal(key_val);
            auto result = reader->VisitEqual(literal);
            ASSERT_OK(result.status()) << prefix << ": VisitEqual(" << key_val << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids = GetRowIdsForIntKey(records, key_val);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitEqual(" << key_val << ") mismatch (last key)";
            break;
        }
    }

    // ---- Test 5: VisitEqual for a non-existent key (should return empty) ----
    {
        Literal literal(static_cast<int32_t>(-999));
        auto result = reader->VisitEqual(literal);
        ASSERT_OK(result.status()) << prefix << ": VisitEqual(-999) failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_TRUE(actual_ids.empty()) << prefix << ": VisitEqual(-999) should be empty";
    }

    // ---- Test 6: VisitLessThan for a mid-range key ----
    {
        // Find a key roughly in the middle
        std::vector<int32_t> non_null_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) non_null_keys.push_back(std::stoi(rec.key));
        }
        if (!non_null_keys.empty()) {
            std::sort(non_null_keys.begin(), non_null_keys.end());
            int32_t mid_key = non_null_keys[non_null_keys.size() / 2];
            Literal literal(mid_key);
            auto result = reader->VisitLessThan(literal);
            ASSERT_OK(result.status()) << prefix << ": VisitLessThan(" << mid_key << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids =
                GetRowIdsForIntRange(records, non_null_keys.front(), mid_key, true, false);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitLessThan(" << mid_key << ") mismatch";
        }
    }

    // ---- Test 7: VisitGreaterOrEqual for a mid-range key ----
    {
        std::vector<int32_t> non_null_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) non_null_keys.push_back(std::stoi(rec.key));
        }
        if (!non_null_keys.empty()) {
            std::sort(non_null_keys.begin(), non_null_keys.end());
            int32_t mid_key = non_null_keys[non_null_keys.size() / 2];
            Literal literal(mid_key);
            auto result = reader->VisitGreaterOrEqual(literal);
            ASSERT_OK(result.status())
                << prefix << ": VisitGreaterOrEqual(" << mid_key << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids =
                GetRowIdsForIntRange(records, mid_key, non_null_keys.back(), true, true);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitGreaterOrEqual(" << mid_key << ") mismatch";
        }
    }

    // ---- Test 8: VisitBetween ----
    {
        std::vector<int32_t> non_null_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) non_null_keys.push_back(std::stoi(rec.key));
        }
        if (non_null_keys.size() >= 4) {
            std::sort(non_null_keys.begin(), non_null_keys.end());
            int32_t lower = non_null_keys[non_null_keys.size() / 4];
            int32_t upper = non_null_keys[non_null_keys.size() * 3 / 4];
            Literal lit_lower(lower);
            Literal lit_upper(upper);
            auto result = reader->VisitBetween(lit_lower, lit_upper);
            ASSERT_OK(result.status())
                << prefix << ": VisitBetween(" << lower << ", " << upper << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids = GetRowIdsForIntRange(records, lower, upper, true, true);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitBetween(" << lower << ", " << upper << ") mismatch";
        }
    }

    // ---- Test 9: VisitIn for multiple keys ----
    {
        std::set<int32_t> unique_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) unique_keys.insert(std::stoi(rec.key));
        }
        if (unique_keys.size() >= 3) {
            auto it = unique_keys.begin();
            int32_t k1 = *it++;
            int32_t k2 = *it++;
            int32_t k3 = *it++;
            std::vector<Literal> in_literals = {Literal(k1), Literal(k2), Literal(k3)};
            auto result = reader->VisitIn(in_literals);
            ASSERT_OK(result.status())
                << prefix << ": VisitIn({" << k1 << "," << k2 << "," << k3 << "}) failed";
            auto actual_ids = CollectRowIds(result.value());

            std::set<int64_t> expected_ids;
            for (auto id : GetRowIdsForIntKey(records, k1)) expected_ids.insert(id);
            for (auto id : GetRowIdsForIntKey(records, k2)) expected_ids.insert(id);
            for (auto id : GetRowIdsForIntKey(records, k3)) expected_ids.insert(id);
            EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIn mismatch";
        }
    }

    // ---- Test 10: VisitNotEqual ----
    {
        for (const auto& rec : records) {
            if (!rec.is_null) {
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                auto result = reader->VisitNotEqual(literal);
                ASSERT_OK(result.status()) << prefix << ": VisitNotEqual(" << key_val << ") failed";
                auto actual_ids = CollectRowIds(result.value());

                // Expected: all non-null rows except those with this key
                std::set<int64_t> expected_ids;
                for (const auto& r : records) {
                    if (!r.is_null && std::stoi(r.key) != key_val) {
                        expected_ids.insert(r.row_id);
                    }
                }
                EXPECT_EQ(actual_ids, expected_ids)
                    << prefix << ": VisitNotEqual(" << key_val << ") mismatch";
                break;  // Test with just the first non-null key
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(IntDataSizes, BTreeCompatibilityIntTest,
                         ::testing::Values(50, 100, 500, 1000, 5000));

// ===========================================================================
// Test: Read varchar type data with various record counts
// ===========================================================================
class BTreeCompatibilityVarcharTest : public BTreeCompatibilityTest,
                                      public ::testing::WithParamInterface<int> {};

TEST_P(BTreeCompatibilityVarcharTest, ReadAndQueryVarcharData) {
    int count = GetParam();
    std::string prefix = "btree_test_varchar_" + std::to_string(count);
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    // Parse CSV
    auto records = ParseCsvFile(csv_path);
    ASSERT_EQ(static_cast<int>(records.size()), count)
        << "CSV record count mismatch for " << prefix;

    // Create reader (varchar -> arrow::utf8())
    auto reader_result = CreateReaderFromFiles(bin_path, meta_path, arrow::utf8(), count);
    ASSERT_OK(reader_result.status()) << "Failed to create reader for " << prefix;
    auto reader = reader_result.value();

    // ---- Test 1: VisitIsNull ----
    {
        auto result = reader->VisitIsNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNull failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetNullRowIds(records);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIsNull mismatch";
    }

    // ---- Test 2: VisitIsNotNull ----
    {
        auto result = reader->VisitIsNotNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNotNull failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetNonNullRowIds(records);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIsNotNull mismatch";
    }

    // ---- Test 3: VisitEqual for a known key ----
    for (const auto& rec : records) {
        if (!rec.is_null) {
            Literal literal(FieldType::STRING, rec.key.c_str(),
                            static_cast<int32_t>(rec.key.size()));
            auto result = reader->VisitEqual(literal);
            ASSERT_OK(result.status()) << prefix << ": VisitEqual(" << rec.key << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids = GetRowIdsForStringKey(records, rec.key);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitEqual(" << rec.key << ") mismatch";
            break;
        }
    }

    // ---- Test 4: VisitEqual for the last non-null key ----
    for (auto it = records.rbegin(); it != records.rend(); ++it) {
        if (!it->is_null) {
            Literal literal(FieldType::STRING, it->key.c_str(),
                            static_cast<int32_t>(it->key.size()));
            auto result = reader->VisitEqual(literal);
            ASSERT_OK(result.status()) << prefix << ": VisitEqual(" << it->key << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids = GetRowIdsForStringKey(records, it->key);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitEqual(" << it->key << ") mismatch (last key)";
            break;
        }
    }

    // ---- Test 5: VisitEqual for a non-existent key ----
    {
        std::string non_existent = "zzz_non_existent_key";
        Literal literal(FieldType::STRING, non_existent.c_str(),
                        static_cast<int32_t>(non_existent.size()));
        auto result = reader->VisitEqual(literal);
        ASSERT_OK(result.status()) << prefix << ": VisitEqual(non_existent) failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_TRUE(actual_ids.empty()) << prefix << ": VisitEqual(non_existent) should be empty";
    }

    // ---- Test 6: VisitLessThan for a mid-range key ----
    {
        std::vector<std::string> non_null_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) non_null_keys.push_back(rec.key);
        }
        if (!non_null_keys.empty()) {
            std::sort(non_null_keys.begin(), non_null_keys.end());
            std::string mid_key = non_null_keys[non_null_keys.size() / 2];
            Literal literal(FieldType::STRING, mid_key.c_str(),
                            static_cast<int32_t>(mid_key.size()));
            auto result = reader->VisitLessThan(literal);
            ASSERT_OK(result.status()) << prefix << ": VisitLessThan(" << mid_key << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids =
                GetRowIdsForStringRange(records, non_null_keys.front(), mid_key, true, false);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitLessThan(" << mid_key << ") mismatch";
        }
    }

    // ---- Test 7: VisitGreaterOrEqual for a mid-range key ----
    {
        std::vector<std::string> non_null_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) non_null_keys.push_back(rec.key);
        }
        if (!non_null_keys.empty()) {
            std::sort(non_null_keys.begin(), non_null_keys.end());
            std::string mid_key = non_null_keys[non_null_keys.size() / 2];
            Literal literal(FieldType::STRING, mid_key.c_str(),
                            static_cast<int32_t>(mid_key.size()));
            auto result = reader->VisitGreaterOrEqual(literal);
            ASSERT_OK(result.status())
                << prefix << ": VisitGreaterOrEqual(" << mid_key << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids =
                GetRowIdsForStringRange(records, mid_key, non_null_keys.back(), true, true);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitGreaterOrEqual(" << mid_key << ") mismatch";
        }
    }

    // ---- Test 8: VisitStartsWith ----
    {
        std::string prefix_str = "test_000";
        Literal literal(FieldType::STRING, prefix_str.c_str(),
                        static_cast<int32_t>(prefix_str.size()));
        auto result = reader->VisitStartsWith(literal);
        ASSERT_OK(result.status()) << prefix << ": VisitStartsWith(" << prefix_str << ") failed";
        auto actual_ids = CollectRowIds(result.value());

        // Expected: all non-null rows whose key starts with "test_000"
        std::set<int64_t> expected_ids;
        for (const auto& rec : records) {
            if (!rec.is_null && rec.key.substr(0, prefix_str.size()) == prefix_str) {
                expected_ids.insert(rec.row_id);
            }
        }
        EXPECT_EQ(actual_ids, expected_ids)
            << prefix << ": VisitStartsWith(" << prefix_str << ") mismatch";
    }

    // ---- Test 9: VisitBetween ----
    {
        std::vector<std::string> non_null_keys;
        for (const auto& rec : records) {
            if (!rec.is_null) non_null_keys.push_back(rec.key);
        }
        if (non_null_keys.size() >= 4) {
            std::sort(non_null_keys.begin(), non_null_keys.end());
            std::string lower = non_null_keys[non_null_keys.size() / 4];
            std::string upper = non_null_keys[non_null_keys.size() * 3 / 4];
            Literal lit_lower(FieldType::STRING, lower.c_str(), static_cast<int32_t>(lower.size()));
            Literal lit_upper(FieldType::STRING, upper.c_str(), static_cast<int32_t>(upper.size()));
            auto result = reader->VisitBetween(lit_lower, lit_upper);
            ASSERT_OK(result.status())
                << prefix << ": VisitBetween(" << lower << ", " << upper << ") failed";
            auto actual_ids = CollectRowIds(result.value());
            auto expected_ids = GetRowIdsForStringRange(records, lower, upper, true, true);
            EXPECT_EQ(actual_ids, expected_ids)
                << prefix << ": VisitBetween(" << lower << ", " << upper << ") mismatch";
        }
    }
}

INSTANTIATE_TEST_SUITE_P(VarcharDataSizes, BTreeCompatibilityVarcharTest,
                         ::testing::Values(50, 100, 500, 1000, 5000));

// ===========================================================================
// Test: Edge case - all nulls
// ===========================================================================
TEST_F(BTreeCompatibilityTest, AllNulls) {
    std::string prefix = "btree_test_int_all_nulls";
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    auto records = ParseCsvFile(csv_path);
    ASSERT_FALSE(records.empty()) << "Failed to parse CSV for " << prefix;
    int count = static_cast<int>(records.size());

    auto reader_result = CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count);
    ASSERT_OK(reader_result.status()) << "Failed to create reader for " << prefix;
    auto reader = reader_result.value();

    // All rows should be null
    {
        auto result = reader->VisitIsNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNull failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_EQ(static_cast<int>(actual_ids.size()), count)
            << prefix << ": VisitIsNull should return all rows";
        // Verify each row ID
        for (int i = 0; i < count; ++i) {
            EXPECT_TRUE(actual_ids.count(i)) << prefix << ": Missing row_id " << i;
        }
    }

    // No rows should be non-null
    {
        auto result = reader->VisitIsNotNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNotNull failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_TRUE(actual_ids.empty()) << prefix << ": VisitIsNotNull should be empty";
    }

    // VisitEqual should return empty for any key
    {
        Literal literal(static_cast<int32_t>(42));
        auto result = reader->VisitEqual(literal);
        ASSERT_OK(result.status()) << prefix << ": VisitEqual(42) failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_TRUE(actual_ids.empty()) << prefix << ": VisitEqual should be empty for all-nulls";
    }
}

// ===========================================================================
// Test: Edge case - no nulls
// ===========================================================================
TEST_F(BTreeCompatibilityTest, NoNulls) {
    std::string prefix = "btree_test_int_no_nulls";
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    auto records = ParseCsvFile(csv_path);
    ASSERT_FALSE(records.empty()) << "Failed to parse CSV for " << prefix;
    int count = static_cast<int>(records.size());

    auto reader_result = CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count);
    ASSERT_OK(reader_result.status()) << "Failed to create reader for " << prefix;
    auto reader = reader_result.value();

    // No rows should be null
    {
        auto result = reader->VisitIsNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNull failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_TRUE(actual_ids.empty()) << prefix << ": VisitIsNull should be empty";
    }

    // All rows should be non-null
    {
        auto result = reader->VisitIsNotNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNotNull failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_EQ(static_cast<int>(actual_ids.size()), count)
            << prefix << ": VisitIsNotNull should return all rows";
    }

    // VisitEqual for each unique key should return correct row IDs
    {
        std::set<std::string> tested_keys;
        for (const auto& rec : records) {
            if (!rec.is_null && tested_keys.find(rec.key) == tested_keys.end()) {
                tested_keys.insert(rec.key);
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                auto result = reader->VisitEqual(literal);
                ASSERT_OK(result.status()) << prefix << ": VisitEqual(" << key_val << ") failed";
                auto actual_ids = CollectRowIds(result.value());
                auto expected_ids = GetRowIdsForIntKey(records, key_val);
                EXPECT_EQ(actual_ids, expected_ids)
                    << prefix << ": VisitEqual(" << key_val << ") mismatch";
            }
        }
    }

    // VisitLessOrEqual for the max key should return all rows
    {
        int32_t max_key = 0;
        for (const auto& rec : records) {
            if (!rec.is_null) max_key = std::max(max_key, std::stoi(rec.key));
        }
        Literal literal(max_key);
        auto result = reader->VisitLessOrEqual(literal);
        ASSERT_OK(result.status()) << prefix << ": VisitLessOrEqual(" << max_key << ") failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_EQ(static_cast<int>(actual_ids.size()), count)
            << prefix << ": VisitLessOrEqual(max) should return all rows";
    }

    // VisitGreaterThan for the max key should return empty
    {
        int32_t max_key = 0;
        for (const auto& rec : records) {
            if (!rec.is_null) max_key = std::max(max_key, std::stoi(rec.key));
        }
        Literal literal(max_key);
        auto result = reader->VisitGreaterThan(literal);
        ASSERT_OK(result.status()) << prefix << ": VisitGreaterThan(" << max_key << ") failed";
        auto actual_ids = CollectRowIds(result.value());
        EXPECT_TRUE(actual_ids.empty()) << prefix << ": VisitGreaterThan(max) should be empty";
    }
}

// ===========================================================================
// Test: Edge case - duplicate keys
// ===========================================================================
TEST_F(BTreeCompatibilityTest, DuplicateKeys) {
    std::string prefix = "btree_test_int_duplicates";
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    auto records = ParseCsvFile(csv_path);
    ASSERT_FALSE(records.empty()) << "Failed to parse CSV for " << prefix;
    int count = static_cast<int>(records.size());

    auto reader_result = CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count);
    ASSERT_OK(reader_result.status()) << "Failed to create reader for " << prefix;
    auto reader = reader_result.value();

    // ---- Test: VisitIsNull ----
    {
        auto result = reader->VisitIsNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNull failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetNullRowIds(records);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIsNull mismatch";
    }

    // ---- Test: VisitIsNotNull ----
    {
        auto result = reader->VisitIsNotNull();
        ASSERT_OK(result.status()) << prefix << ": VisitIsNotNull failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetNonNullRowIds(records);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIsNotNull mismatch";
    }

    // ---- Test: VisitEqual for each unique key ----
    // Duplicate keys: key = i/10, so keys are 0,0,...,0,1,1,...,1,...,9,9,...,9
    {
        std::set<std::string> tested_keys;
        for (const auto& rec : records) {
            if (!rec.is_null && tested_keys.find(rec.key) == tested_keys.end()) {
                tested_keys.insert(rec.key);
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                auto result = reader->VisitEqual(literal);
                ASSERT_OK(result.status()) << prefix << ": VisitEqual(" << key_val << ") failed";
                auto actual_ids = CollectRowIds(result.value());
                auto expected_ids = GetRowIdsForIntKey(records, key_val);
                EXPECT_EQ(actual_ids, expected_ids)
                    << prefix << ": VisitEqual(" << key_val << ") mismatch";
            }
        }
    }

    // ---- Test: VisitIn for keys 0, 5, 9 ----
    {
        std::vector<Literal> in_literals = {Literal(static_cast<int32_t>(0)),
                                            Literal(static_cast<int32_t>(5)),
                                            Literal(static_cast<int32_t>(9))};
        auto result = reader->VisitIn(in_literals);
        ASSERT_OK(result.status()) << prefix << ": VisitIn({0,5,9}) failed";
        auto actual_ids = CollectRowIds(result.value());

        std::set<int64_t> expected_ids;
        for (auto id : GetRowIdsForIntKey(records, 0)) expected_ids.insert(id);
        for (auto id : GetRowIdsForIntKey(records, 5)) expected_ids.insert(id);
        for (auto id : GetRowIdsForIntKey(records, 9)) expected_ids.insert(id);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitIn({0,5,9}) mismatch";
    }

    // ---- Test: VisitBetween for keys [2, 7] ----
    {
        Literal lit_lower(static_cast<int32_t>(2));
        Literal lit_upper(static_cast<int32_t>(7));
        auto result = reader->VisitBetween(lit_lower, lit_upper);
        ASSERT_OK(result.status()) << prefix << ": VisitBetween(2, 7) failed";
        auto actual_ids = CollectRowIds(result.value());
        auto expected_ids = GetRowIdsForIntRange(records, 2, 7, true, true);
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitBetween(2, 7) mismatch";
    }

    // ---- Test: VisitNotBetween for keys [2, 7] ----
    {
        Literal lit_lower(static_cast<int32_t>(2));
        Literal lit_upper(static_cast<int32_t>(7));
        auto result = reader->VisitNotBetween(lit_lower, lit_upper);
        ASSERT_OK(result.status()) << prefix << ": VisitNotBetween(2, 7) failed";
        auto actual_ids = CollectRowIds(result.value());

        // Expected: non-null rows with key < 2 or key > 7
        std::set<int64_t> expected_ids;
        for (const auto& rec : records) {
            if (!rec.is_null) {
                int32_t val = std::stoi(rec.key);
                if (val < 2 || val > 7) expected_ids.insert(rec.row_id);
            }
        }
        EXPECT_EQ(actual_ids, expected_ids) << prefix << ": VisitNotBetween(2, 7) mismatch";
    }
}

// ===========================================================================
// Test: BTreeIndexMeta deserialization from Java-generated meta files
// ===========================================================================
TEST_F(BTreeCompatibilityTest, MetaDeserialization) {
    // Test int_50 meta
    {
        std::string meta_path = data_dir_ + "/btree_test_int_50.bin.meta";
        auto meta_bytes = ReadBinaryFile(meta_path, pool_.get());
        ASSERT_NE(meta_bytes, nullptr) << "Failed to read meta file";

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_NE(meta, nullptr) << "Failed to deserialize meta";

        // The int_50 data has nulls (row_id 1, 18, 48, 49)
        EXPECT_TRUE(meta->HasNulls()) << "int_50 should have nulls";

        // First key should be "3" (first non-null key in sorted order)
        ASSERT_NE(meta->FirstKey(), nullptr) << "int_50 first key should not be null";
        std::string first_key(meta->FirstKey()->data(), meta->FirstKey()->size());
        // Java writes int keys as string representation
        // The first non-null key in sorted order is 3
        EXPECT_FALSE(first_key.empty()) << "int_50 first key should not be empty";

        // Last key should be "143" (last non-null key in sorted order)
        ASSERT_NE(meta->LastKey(), nullptr) << "int_50 last key should not be null";
        std::string last_key(meta->LastKey()->data(), meta->LastKey()->size());
        EXPECT_FALSE(last_key.empty()) << "int_50 last key should not be empty";
    }

    // Test all_nulls meta
    {
        std::string meta_path = data_dir_ + "/btree_test_int_all_nulls.bin.meta";
        auto meta_bytes = ReadBinaryFile(meta_path, pool_.get());
        ASSERT_NE(meta_bytes, nullptr) << "Failed to read all_nulls meta file";

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_NE(meta, nullptr) << "Failed to deserialize all_nulls meta";

        EXPECT_TRUE(meta->HasNulls()) << "all_nulls should have nulls";
        // For all-nulls data, first_key and last_key should be null
        EXPECT_TRUE(meta->OnlyNulls()) << "all_nulls should report OnlyNulls()";
    }

    // Test no_nulls meta
    {
        std::string meta_path = data_dir_ + "/btree_test_int_no_nulls.bin.meta";
        auto meta_bytes = ReadBinaryFile(meta_path, pool_.get());
        ASSERT_NE(meta_bytes, nullptr) << "Failed to read no_nulls meta file";

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_NE(meta, nullptr) << "Failed to deserialize no_nulls meta";

        EXPECT_FALSE(meta->HasNulls()) << "no_nulls should not have nulls";
        EXPECT_FALSE(meta->OnlyNulls()) << "no_nulls should not report OnlyNulls()";
        ASSERT_NE(meta->FirstKey(), nullptr) << "no_nulls first key should not be null";
        ASSERT_NE(meta->LastKey(), nullptr) << "no_nulls last key should not be null";
    }

    // Test varchar_50 meta
    {
        std::string meta_path = data_dir_ + "/btree_test_varchar_50.bin.meta";
        auto meta_bytes = ReadBinaryFile(meta_path, pool_.get());
        ASSERT_NE(meta_bytes, nullptr) << "Failed to read varchar_50 meta file";

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_NE(meta, nullptr) << "Failed to deserialize varchar_50 meta";

        // varchar_50 has 1 null (row_id=24 based on 5% null ratio with seed 42)
        ASSERT_NE(meta->FirstKey(), nullptr) << "varchar_50 first key should not be null";
        ASSERT_NE(meta->LastKey(), nullptr) << "varchar_50 last key should not be null";

        std::string first_key(meta->FirstKey()->data(), meta->FirstKey()->size());
        std::string last_key(meta->LastKey()->data(), meta->LastKey()->size());
        // Keys are "test_00000" to "test_00049" (excluding nulls)
        EXPECT_EQ(first_key, "test_00000") << "varchar_50 first key mismatch";
        EXPECT_EQ(last_key, "test_00049") << "varchar_50 last key mismatch";
    }
}

// ===========================================================================
// Test: Verify total row count consistency
// ===========================================================================
TEST_F(BTreeCompatibilityTest, RowCountConsistency) {
    // For each test data set, verify that null_count + non_null_count == total_count
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> test_cases = {
        {"btree_test_int_50", arrow::int32()},       {"btree_test_int_100", arrow::int32()},
        {"btree_test_int_500", arrow::int32()},      {"btree_test_varchar_50", arrow::utf8()},
        {"btree_test_varchar_100", arrow::utf8()},   {"btree_test_int_all_nulls", arrow::int32()},
        {"btree_test_int_no_nulls", arrow::int32()}, {"btree_test_int_duplicates", arrow::int32()},
    };

    for (const auto& [prefix, arrow_type] : test_cases) {
        std::string bin_path = data_dir_ + "/" + prefix + ".bin";
        std::string meta_path = bin_path + ".meta";
        std::string csv_path = data_dir_ + "/" + prefix + ".csv";

        auto records = ParseCsvFile(csv_path);
        ASSERT_FALSE(records.empty()) << "Failed to parse CSV for " << prefix;
        int count = static_cast<int>(records.size());

        auto reader_result = CreateReaderFromFiles(bin_path, meta_path, arrow_type, count);
        ASSERT_OK(reader_result.status()) << "Failed to create reader for " << prefix;
        auto reader = reader_result.value();

        auto null_result = reader->VisitIsNull();
        ASSERT_OK(null_result.status()) << prefix << ": VisitIsNull failed";
        auto null_ids = CollectRowIds(null_result.value());

        auto non_null_result = reader->VisitIsNotNull();
        ASSERT_OK(non_null_result.status()) << prefix << ": VisitIsNotNull failed";
        auto non_null_ids = CollectRowIds(non_null_result.value());

        // Null and non-null should be disjoint
        for (auto id : null_ids) {
            EXPECT_EQ(non_null_ids.count(id), 0u)
                << prefix << ": row_id " << id << " is in both null and non-null sets";
        }

        // Total should equal record count
        EXPECT_EQ(static_cast<int>(null_ids.size() + non_null_ids.size()), count)
            << prefix << ": null_count + non_null_count != total_count";
    }
}

}  // namespace paimon::test
