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
#include <algorithm>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/btree/btree_global_indexer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class BTreeCompatibilityTest : public ::testing::Test {
 protected:
    struct CsvRecord {
        int64_t row_id;
        std::string key;  // "NULL" if is_null
        bool is_null;
    };

    void SetUp() override {
        pool_ = GetDefaultPool();
        ASSERT_OK_AND_ASSIGN(fs_, FileSystemFactory::Get("local", "/", {}));
        data_dir_ = GetDataDir() + "/global_index/btree/btree_compatibility_data";
    }

    std::string ReadFileAsString(const std::string& path) const {
        EXPECT_OK_AND_ASSIGN(auto input, fs_->Open(path));
        EXPECT_OK_AND_ASSIGN(auto length, input->Length());
        std::string buffer(static_cast<size_t>(length), '\0');
        EXPECT_OK_AND_ASSIGN([[maybe_unused]] auto bytes_read,
                             input->Read(buffer.data(), static_cast<uint32_t>(length)));
        return buffer;
    }

    // Parse a CSV file into a vector of CsvRecord
    std::vector<CsvRecord> ParseCsvFile(const std::string& csv_path) const {
        std::vector<CsvRecord> records;
        std::string content = ReadFileAsString(csv_path);
        if (content.empty()) {
            return records;
        }

        std::istringstream iss(content);
        std::string line;
        // Skip header line: "row_id,key,is_null"
        std::getline(iss, line);

        while (std::getline(iss, line)) {
            if (line.empty()) {
                continue;
            }
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

    std::set<int64_t> CollectRowIds(const std::shared_ptr<GlobalIndexResult>& result) const {
        std::set<int64_t> ids;
        EXPECT_OK_AND_ASSIGN(auto iter, result->CreateIterator());
        while (iter->HasNext()) {
            ids.insert(iter->Next());
        }
        return ids;
    }

    std::set<int64_t> CollectMatchingRows(const std::vector<CsvRecord>& records,
                                          std::function<bool(const CsvRecord&)> predicate) const {
        std::set<int64_t> ids;
        for (const auto& rec : records) {
            if (predicate(rec)) {
                ids.insert(rec.row_id);
            }
        }
        return ids;
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateReaderFromFiles(
        const std::string& bin_path, const std::string& meta_path,
        const std::shared_ptr<arrow::DataType>& arrow_type, int64_t record_count) const {
        auto meta_str = ReadFileAsString(meta_path);
        std::shared_ptr<Bytes> meta_bytes = Bytes::AllocateBytes(meta_str, pool_.get());
        PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(bin_path));
        auto file_size = static_cast<int64_t>(file_status->GetLen());

        GlobalIndexIOMeta io_meta(bin_path, file_size, record_count - 1, meta_bytes);
        std::vector<GlobalIndexIOMeta> metas = {io_meta};

        auto schema = arrow::schema({arrow::field("testField", arrow_type)});
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, c_schema.get()));

        auto file_reader = std::make_shared<LocalGlobalIndexFileReader>(fs_);
        std::map<std::string, std::string> options;
        BTreeGlobalIndexer indexer(options);
        return indexer.CreateReader(c_schema.get(), file_reader, metas, pool_);
    }

    void RunIntQueries(const std::shared_ptr<GlobalIndexReader>& reader,
                       const std::vector<CsvRecord>& records) const {
        // VisitIsNull
        {
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
            auto actual_ids = CollectRowIds(result);
            auto expected_ids =
                CollectMatchingRows(records, [](const CsvRecord& r) { return r.is_null; });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitIsNotNull
        {
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
            auto actual_ids = CollectRowIds(result);
            auto expected_ids =
                CollectMatchingRows(records, [](const CsvRecord& r) { return !r.is_null; });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitEqual for the first non-null key
        for (const auto& rec : records) {
            if (!rec.is_null) {
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(records, [key_val](const CsvRecord& r) {
                    return !r.is_null && std::stoi(r.key) == key_val;
                });
                ASSERT_EQ(actual_ids, expected_ids);
                break;
            }
        }

        // VisitEqual for the last non-null key
        for (auto it = records.rbegin(); it != records.rend(); ++it) {
            if (!it->is_null) {
                int32_t key_val = std::stoi(it->key);
                Literal literal(key_val);
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(records, [key_val](const CsvRecord& r) {
                    return !r.is_null && std::stoi(r.key) == key_val;
                });
                ASSERT_EQ(actual_ids, expected_ids);
                break;
            }
        }

        // VisitEqual for a non-existent key
        {
            Literal literal(static_cast<int32_t>(-999));
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
            auto actual_ids = CollectRowIds(result);
            ASSERT_TRUE(actual_ids.empty());
        }

        int32_t mid_key = -1;
        {
            std::vector<int32_t> non_null_keys;
            for (const auto& rec : records) {
                if (!rec.is_null) {
                    non_null_keys.push_back(std::stoi(rec.key));
                }
            }
            ASSERT_FALSE(non_null_keys.empty());
            std::sort(non_null_keys.begin(), non_null_keys.end());
            mid_key = non_null_keys[non_null_keys.size() / 2];
        }
        // VisitLessThan for a mid-range key
        {
            Literal literal(mid_key);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal));
            auto actual_ids = CollectRowIds(result);
            auto expected_ids = CollectMatchingRows(records, [mid_key](const CsvRecord& r) {
                return !r.is_null && std::stoi(r.key) < mid_key;
            });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitGreaterOrEqual for a mid-range key
        {
            Literal literal(mid_key);
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(literal));
            auto actual_ids = CollectRowIds(result);
            auto expected_ids = CollectMatchingRows(records, [mid_key](const CsvRecord& r) {
                return !r.is_null && std::stoi(r.key) >= mid_key;
            });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitIn for multiple keys
        {
            std::set<int32_t> unique_keys;
            for (const auto& rec : records) {
                if (!rec.is_null) {
                    unique_keys.insert(std::stoi(rec.key));
                }
            }
            ASSERT_GE(unique_keys.size(), 3);
            auto it = unique_keys.begin();
            int32_t k1 = *it++;
            int32_t k2 = *it++;
            int32_t k3 = *it++;
            std::vector<Literal> in_literals = {Literal(k1), Literal(k2), Literal(k3)};
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
            auto actual_ids = CollectRowIds(result);
            auto expected_ids = CollectMatchingRows(records, [k1, k2, k3](const CsvRecord& r) {
                if (r.is_null) {
                    return false;
                }
                int32_t v = std::stoi(r.key);
                return v == k1 || v == k2 || v == k3;
            });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitNotEqual for the first non-null key
        for (const auto& rec : records) {
            if (!rec.is_null) {
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitNotEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(records, [key_val](const CsvRecord& r) {
                    return !r.is_null && std::stoi(r.key) != key_val;
                });
                ASSERT_EQ(actual_ids, expected_ids);
                break;
            }
        }
    }

    // Run string-type queries against a reader with CSV records as ground truth
    void RunStringQueries(const std::shared_ptr<GlobalIndexReader>& reader,
                          const std::vector<CsvRecord>& records) const {
        // VisitIsNull
        {
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
            auto actual_ids = CollectRowIds(result);
            auto expected_ids =
                CollectMatchingRows(records, [](const CsvRecord& r) { return r.is_null; });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitIsNotNull
        {
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
            auto actual_ids = CollectRowIds(result);
            auto expected_ids =
                CollectMatchingRows(records, [](const CsvRecord& r) { return !r.is_null; });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitEqual for the first non-null key
        for (const auto& rec : records) {
            if (!rec.is_null) {
                Literal literal(FieldType::STRING, rec.key.c_str(),
                                static_cast<int32_t>(rec.key.size()));
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(
                    records, [&rec](const CsvRecord& r) { return !r.is_null && r.key == rec.key; });
                ASSERT_EQ(actual_ids, expected_ids);
                break;
            }
        }

        // VisitEqual for the last non-null key
        for (auto it = records.rbegin(); it != records.rend(); ++it) {
            if (!it->is_null) {
                Literal literal(FieldType::STRING, it->key.c_str(),
                                static_cast<int32_t>(it->key.size()));
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(
                    records, [&it](const CsvRecord& r) { return !r.is_null && r.key == it->key; });
                ASSERT_EQ(actual_ids, expected_ids);
                break;
            }
        }

        // VisitEqual for a non-existent key
        {
            std::string non_existent = "zzz_non_existent_key";
            Literal literal(FieldType::STRING, non_existent.c_str(),
                            static_cast<int32_t>(non_existent.size()));
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
            auto actual_ids = CollectRowIds(result);
            ASSERT_TRUE(actual_ids.empty());
        }

        std::string mid_key;
        {
            std::vector<std::string> non_null_keys;
            for (const auto& rec : records) {
                if (!rec.is_null) {
                    non_null_keys.push_back(rec.key);
                }
            }
            ASSERT_FALSE(non_null_keys.empty());
            std::sort(non_null_keys.begin(), non_null_keys.end());
            mid_key = non_null_keys[non_null_keys.size() / 2];
        }
        // VisitLessThan for a mid-range key
        {
            Literal literal(FieldType::STRING, mid_key.c_str(),
                            static_cast<int32_t>(mid_key.size()));
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessThan(literal));
            auto actual_ids = CollectRowIds(result);
            auto expected_ids = CollectMatchingRows(
                records, [&mid_key](const CsvRecord& r) { return !r.is_null && r.key < mid_key; });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitGreaterOrEqual for a mid-range key
        {
            Literal literal(FieldType::STRING, mid_key.c_str(),
                            static_cast<int32_t>(mid_key.size()));
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterOrEqual(literal));
            auto actual_ids = CollectRowIds(result);
            auto expected_ids = CollectMatchingRows(
                records, [&mid_key](const CsvRecord& r) { return !r.is_null && r.key >= mid_key; });
            ASSERT_EQ(actual_ids, expected_ids);
        }

        // VisitStartsWith
        {
            std::string prefix_str = "test_000";
            Literal literal(FieldType::STRING, prefix_str.c_str(),
                            static_cast<int32_t>(prefix_str.size()));
            ASSERT_OK_AND_ASSIGN(auto result, reader->VisitStartsWith(literal));
            auto actual_ids = CollectRowIds(result);
            auto expected_ids = CollectMatchingRows(records, [&prefix_str](const CsvRecord& r) {
                return !r.is_null && StringUtils::StartsWith(r.key, prefix_str);
            });
            ASSERT_EQ(actual_ids, expected_ids);
        }
    }

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

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::string data_dir_;
};

TEST_F(BTreeCompatibilityTest, ReadAndQueryIntData) {
    for (int32_t count : {50, 100, 500, 1000, 5000}) {
        std::string prefix = "btree_test_int_" + std::to_string(count);
        std::string bin_path = data_dir_ + "/" + prefix + ".bin";
        std::string meta_path = bin_path + ".meta";
        std::string csv_path = data_dir_ + "/" + prefix + ".csv";

        auto records = ParseCsvFile(csv_path);
        ASSERT_EQ(static_cast<int32_t>(records.size()), count);

        ASSERT_OK_AND_ASSIGN(auto reader,
                             CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count));
        RunIntQueries(reader, records);
    }
}

TEST_F(BTreeCompatibilityTest, ReadAndQueryStringData) {
    for (int32_t count : {50, 100, 500, 1000, 5000}) {
        std::string prefix = "btree_test_string_" + std::to_string(count);
        std::string bin_path = data_dir_ + "/" + prefix + ".bin";
        std::string meta_path = bin_path + ".meta";
        std::string csv_path = data_dir_ + "/" + prefix + ".csv";

        auto records = ParseCsvFile(csv_path);
        ASSERT_EQ(static_cast<int32_t>(records.size()), count);

        ASSERT_OK_AND_ASSIGN(auto reader,
                             CreateReaderFromFiles(bin_path, meta_path, arrow::utf8(), count));
        RunStringQueries(reader, records);
    }
}

TEST_F(BTreeCompatibilityTest, AllNulls) {
    std::string prefix = "btree_test_int_all_nulls";
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    auto records = ParseCsvFile(csv_path);
    ASSERT_FALSE(records.empty());
    auto count = static_cast<int32_t>(records.size());

    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count));

    // All rows should be null
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        auto actual_ids = CollectRowIds(result);
        ASSERT_EQ(static_cast<int32_t>(actual_ids.size()), count);
        for (int32_t i = 0; i < count; ++i) {
            ASSERT_TRUE(actual_ids.count(i));
        }
    }

    // No rows should be non-null
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        auto actual_ids = CollectRowIds(result);
        ASSERT_TRUE(actual_ids.empty());
    }

    // VisitEqual should return empty for any key
    {
        Literal literal(static_cast<int32_t>(42));
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
        auto actual_ids = CollectRowIds(result);
        ASSERT_TRUE(actual_ids.empty());
    }
}

TEST_F(BTreeCompatibilityTest, NoNulls) {
    std::string prefix = "btree_test_int_no_nulls";
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    auto records = ParseCsvFile(csv_path);
    ASSERT_FALSE(records.empty());
    auto count = static_cast<int32_t>(records.size());

    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count));

    // No rows should be null
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        auto actual_ids = CollectRowIds(result);
        ASSERT_TRUE(actual_ids.empty());
    }

    // All rows should be non-null
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        auto actual_ids = CollectRowIds(result);
        ASSERT_EQ(static_cast<int32_t>(actual_ids.size()), count);
    }

    // VisitEqual for each unique key
    {
        std::set<std::string> tested_keys;
        for (const auto& rec : records) {
            if (!rec.is_null && tested_keys.find(rec.key) == tested_keys.end()) {
                tested_keys.insert(rec.key);
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(records, [key_val](const CsvRecord& r) {
                    return !r.is_null && std::stoi(r.key) == key_val;
                });
                ASSERT_EQ(actual_ids, expected_ids);
            }
        }
    }

    int32_t max_key = 0;
    for (const auto& rec : records) {
        if (!rec.is_null) {
            max_key = std::max(max_key, std::stoi(rec.key));
        }
    }

    // VisitLessOrEqual for the max key should return all rows
    {
        Literal literal(max_key);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitLessOrEqual(literal));
        auto actual_ids = CollectRowIds(result);
        ASSERT_EQ(static_cast<int32_t>(actual_ids.size()), count);
    }

    // VisitGreaterThan for the max key should return empty
    {
        Literal literal(max_key);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitGreaterThan(literal));
        auto actual_ids = CollectRowIds(result);
        ASSERT_TRUE(actual_ids.empty());
    }
}

TEST_F(BTreeCompatibilityTest, DuplicateKeys) {
    std::string prefix = "btree_test_int_duplicates";
    std::string bin_path = data_dir_ + "/" + prefix + ".bin";
    std::string meta_path = bin_path + ".meta";
    std::string csv_path = data_dir_ + "/" + prefix + ".csv";

    auto records = ParseCsvFile(csv_path);
    ASSERT_FALSE(records.empty());
    auto count = static_cast<int32_t>(records.size());

    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateReaderFromFiles(bin_path, meta_path, arrow::int32(), count));

    // VisitIsNull
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNull());
        auto actual_ids = CollectRowIds(result);
        auto expected_ids =
            CollectMatchingRows(records, [](const CsvRecord& r) { return r.is_null; });
        ASSERT_EQ(actual_ids, expected_ids);
    }

    // VisitIsNotNull
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIsNotNull());
        auto actual_ids = CollectRowIds(result);
        auto expected_ids =
            CollectMatchingRows(records, [](const CsvRecord& r) { return !r.is_null; });
        ASSERT_EQ(actual_ids, expected_ids);
    }

    // VisitEqual for each unique key
    {
        std::set<std::string> tested_keys;
        for (const auto& rec : records) {
            if (!rec.is_null && tested_keys.find(rec.key) == tested_keys.end()) {
                tested_keys.insert(rec.key);
                int32_t key_val = std::stoi(rec.key);
                Literal literal(key_val);
                ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
                auto actual_ids = CollectRowIds(result);
                auto expected_ids = CollectMatchingRows(records, [key_val](const CsvRecord& r) {
                    return !r.is_null && std::stoi(r.key) == key_val;
                });
                ASSERT_EQ(actual_ids, expected_ids);
            }
        }
    }

    // VisitIn for keys 0, 5, 9
    {
        std::vector<Literal> in_literals = {Literal(static_cast<int32_t>(0)),
                                            Literal(static_cast<int32_t>(5)),
                                            Literal(static_cast<int32_t>(9))};
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitIn(in_literals));
        auto actual_ids = CollectRowIds(result);
        auto expected_ids = CollectMatchingRows(records, [](const CsvRecord& r) {
            if (r.is_null) {
                return false;
            }
            int32_t v = std::stoi(r.key);
            return v == 0 || v == 5 || v == 9;
        });
        ASSERT_EQ(actual_ids, expected_ids);
    }
}

TEST_F(BTreeCompatibilityTest, MetaDeserialization) {
    // Test int_50 meta
    {
        std::string meta_path = data_dir_ + "/btree_test_int_50.bin.meta";
        auto meta_str = ReadFileAsString(meta_path);
        std::shared_ptr<Bytes> meta_bytes = Bytes::AllocateBytes(meta_str, pool_.get());

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_TRUE(meta);

        ASSERT_TRUE(meta->HasNulls());
        ASSERT_FALSE(meta->OnlyNulls());

        ASSERT_TRUE(meta->FirstKey());
        ASSERT_OK_AND_ASSIGN(auto min_key,
                             KeySerializer::DeserializeKey(MemorySlice::Wrap(meta->FirstKey()),
                                                           arrow::int32(), pool_.get()));
        ASSERT_EQ(min_key, Literal(3));

        ASSERT_TRUE(meta->LastKey());
        ASSERT_OK_AND_ASSIGN(auto max_key,
                             KeySerializer::DeserializeKey(MemorySlice::Wrap(meta->LastKey()),
                                                           arrow::int32(), pool_.get()));
        ASSERT_EQ(max_key, Literal(143));
    }

    // Test all_nulls meta
    {
        std::string meta_path = data_dir_ + "/btree_test_int_all_nulls.bin.meta";
        auto meta_str = ReadFileAsString(meta_path);
        std::shared_ptr<Bytes> meta_bytes = Bytes::AllocateBytes(meta_str, pool_.get());

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_TRUE(meta);

        ASSERT_TRUE(meta->HasNulls());
        ASSERT_TRUE(meta->OnlyNulls());
        ASSERT_FALSE(meta->FirstKey());
        ASSERT_FALSE(meta->LastKey());
    }

    // Test no_nulls meta
    {
        std::string meta_path = data_dir_ + "/btree_test_int_no_nulls.bin.meta";
        auto meta_str = ReadFileAsString(meta_path);
        std::shared_ptr<Bytes> meta_bytes = Bytes::AllocateBytes(meta_str, pool_.get());

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_TRUE(meta);

        ASSERT_TRUE(meta->FirstKey());
        ASSERT_OK_AND_ASSIGN(auto min_key,
                             KeySerializer::DeserializeKey(MemorySlice::Wrap(meta->FirstKey()),
                                                           arrow::int32(), pool_.get()));
        ASSERT_EQ(min_key, Literal(4));

        ASSERT_TRUE(meta->LastKey());
        ASSERT_OK_AND_ASSIGN(auto max_key,
                             KeySerializer::DeserializeKey(MemorySlice::Wrap(meta->LastKey()),
                                                           arrow::int32(), pool_.get()));
        ASSERT_EQ(max_key, Literal(158));
    }

    // Test string_50 meta
    {
        std::string meta_path = data_dir_ + "/btree_test_string_50.bin.meta";
        auto meta_str = ReadFileAsString(meta_path);
        std::shared_ptr<Bytes> meta_bytes = Bytes::AllocateBytes(meta_str, pool_.get());

        auto meta = BTreeIndexMeta::Deserialize(meta_bytes, pool_.get());
        ASSERT_TRUE(meta);

        ASSERT_TRUE(meta->HasNulls());
        ASSERT_FALSE(meta->OnlyNulls());

        ASSERT_TRUE(meta->FirstKey());
        ASSERT_OK_AND_ASSIGN(auto min_key,
                             KeySerializer::DeserializeKey(MemorySlice::Wrap(meta->FirstKey()),
                                                           arrow::utf8(), pool_.get()));
        std::string min_key_str = "test_00000";
        ASSERT_EQ(min_key, Literal(FieldType::STRING, min_key_str.data(), min_key_str.size()));

        ASSERT_TRUE(meta->LastKey());
        ASSERT_OK_AND_ASSIGN(auto max_key,
                             KeySerializer::DeserializeKey(MemorySlice::Wrap(meta->LastKey()),
                                                           arrow::utf8(), pool_.get()));
        std::string max_key_str = "test_00049";
        ASSERT_EQ(max_key, Literal(FieldType::STRING, max_key_str.data(), max_key_str.size()));
    }
}

TEST_F(BTreeCompatibilityTest, RowCountConsistency) {
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> test_cases = {
        {"btree_test_int_50", arrow::int32()},       {"btree_test_int_100", arrow::int32()},
        {"btree_test_int_500", arrow::int32()},      {"btree_test_string_50", arrow::utf8()},
        {"btree_test_string_100", arrow::utf8()},    {"btree_test_int_all_nulls", arrow::int32()},
        {"btree_test_int_no_nulls", arrow::int32()}, {"btree_test_int_duplicates", arrow::int32()},
    };

    for (const auto& [prefix, arrow_type] : test_cases) {
        std::string bin_path = data_dir_ + "/" + prefix + ".bin";
        std::string meta_path = bin_path + ".meta";
        std::string csv_path = data_dir_ + "/" + prefix + ".csv";

        auto records = ParseCsvFile(csv_path);
        ASSERT_FALSE(records.empty());
        auto count = static_cast<int32_t>(records.size());

        ASSERT_OK_AND_ASSIGN(auto reader,
                             CreateReaderFromFiles(bin_path, meta_path, arrow_type, count));

        ASSERT_OK_AND_ASSIGN(auto null_result, reader->VisitIsNull());
        auto null_ids = CollectRowIds(null_result);

        ASSERT_OK_AND_ASSIGN(auto non_null_result, reader->VisitIsNotNull());
        auto non_null_ids = CollectRowIds(non_null_result);

        // Null and non-null should be disjoint
        for (auto id : null_ids) {
            ASSERT_EQ(non_null_ids.count(id), 0u);
        }

        // Total should equal record count
        ASSERT_EQ(static_cast<int32_t>(null_ids.size() + non_null_ids.size()), count);
    }
}

}  // namespace paimon::test
