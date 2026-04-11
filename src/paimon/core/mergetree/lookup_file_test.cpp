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

#include "paimon/core/mergetree/lookup_file.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(LookupFileTest, TestSimple) {
    class FakeLookupStoreReader : public LookupStoreReader {
     public:
        FakeLookupStoreReader(const std::map<std::string, std::string>& kvs,
                              std::shared_ptr<MemoryPool>& pool)
            : pool_(pool), kvs_(kvs) {}
        Result<std::shared_ptr<Bytes>> Lookup(const std::shared_ptr<Bytes>& key) const override {
            auto iter = kvs_.find(std::string(key->data(), key->size()));
            if (iter == kvs_.end()) {
                return std::shared_ptr<Bytes>();
            }
            return std::make_shared<Bytes>(iter->second, pool_.get());
        }
        Status Close() override {
            return Status::OK();
        }

     private:
        std::shared_ptr<MemoryPool> pool_;
        std::map<std::string, std::string> kvs_;
    };
    auto pool = GetDefaultPool();
    auto tmp_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(tmp_dir);
    auto fs = tmp_dir->GetFileSystem();
    std::string local_file = tmp_dir->Str() + "/test.file";
    ASSERT_OK(fs->WriteFile(local_file, "testdata", /*overwrite=*/false));
    ASSERT_TRUE(fs->Exists(local_file).value());

    std::map<std::string, std::string> kvs = {{"aa", "aa1"}, {"bb", "bb1"}};
    auto lookup_file = std::make_shared<LookupFile>(
        fs, local_file, /*file_size_bytes=*/0, /*level=*/3, /*schema_id=*/1,
        /*ser_version=*/"v1", std::make_unique<FakeLookupStoreReader>(kvs, pool),
        /*callback=*/nullptr);
    ASSERT_EQ(lookup_file->LocalFile(), local_file);
    ASSERT_EQ(lookup_file->Level(), 3);
    ASSERT_EQ(lookup_file->SchemaId(), 1);
    ASSERT_EQ(lookup_file->SerVersion(), "v1");
    {
        ASSERT_OK_AND_ASSIGN(auto value,
                             lookup_file->GetResult(std::make_shared<Bytes>("aa", pool.get())));
        ASSERT_TRUE(value);
        ASSERT_EQ(std::string(value->data(), value->size()), "aa1");
    }
    {
        ASSERT_OK_AND_ASSIGN(auto value,
                             lookup_file->GetResult(std::make_shared<Bytes>("bb", pool.get())));
        ASSERT_TRUE(value);
        ASSERT_EQ(std::string(value->data(), value->size()), "bb1");
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto value, lookup_file->GetResult(std::make_shared<Bytes>("non-exist", pool.get())));
        ASSERT_FALSE(value);
    }
    ASSERT_FALSE(lookup_file->IsClosed());
    ASSERT_EQ(lookup_file->request_count_, 3);
    ASSERT_EQ(lookup_file->hit_count_, 2);

    ASSERT_OK(lookup_file->Close());
    ASSERT_TRUE(lookup_file->IsClosed());
    ASSERT_FALSE(fs->Exists(local_file).value());
}

TEST(LookupFileTest, TestLocalFilePrefix) {
    auto pool = GetDefaultPool();
    {
        auto schema = arrow::schema({
            arrow::field("f0", arrow::utf8()),
            arrow::field("f1", arrow::int32()),
        });
        auto partition = BinaryRowGenerator::GenerateRow({std::string("20240731"), 10}, pool.get());
        ASSERT_OK_AND_ASSIGN(std::string ret, LookupFile::LocalFilePrefix(
                                                  schema, partition, /*bucket=*/3, "test.orc"));
        ASSERT_EQ(ret, "20240731-10-3-test.orc");
    }
    {
        auto schema = arrow::schema({});
        auto partition = BinaryRow::EmptyRow();
        ASSERT_OK_AND_ASSIGN(std::string ret, LookupFile::LocalFilePrefix(
                                                  schema, partition, /*bucket=*/3, "test.orc"));
        ASSERT_EQ(ret, "3-test.orc");
    }
}

TEST(LookupFileTest, TestLookupFileCacheLifecycle) {
    // This test covers: cache creation, put multiple entries, replacement,
    // invalidation, weight-based eviction, and verifying local files are deleted.
    auto pool = GetDefaultPool();
    auto tmp_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(tmp_dir);
    auto fs = tmp_dir->GetFileSystem();

    class FakeLookupStoreReader : public LookupStoreReader {
     public:
        Result<std::shared_ptr<Bytes>> Lookup(
            const std::shared_ptr<Bytes>& /*key*/) const override {
            return std::shared_ptr<Bytes>();
        }
        Status Close() override {
            return Status::OK();
        }
    };

    std::vector<std::string> call_back_files;
    // Helper to create a local file with given size and return a LookupFile
    auto make_lookup_file = [&](const std::string& name,
                                int64_t size) -> std::shared_ptr<LookupFile> {
        std::string path = tmp_dir->Str() + "/" + name;
        std::string data(size, 'x');
        EXPECT_OK(fs->WriteFile(path, data, /*overwrite=*/false));
        LookupFile::Callback callback = [&call_back_files, name = name]() {
            call_back_files.push_back(name);
        };
        return std::make_shared<LookupFile>(
            fs, path, size, /*level=*/1, /*schema_id=*/0,
            /*ser_version=*/"v1", std::make_unique<FakeLookupStoreReader>(), std::move(callback));
    };

    // Create a cache: max_weight = 300 bytes, no expiration
    auto cache = LookupFile::CreateLookupFileCache(/*file_retention_ms=*/-1, /*max_disk_size=*/300);
    ASSERT_EQ(cache->Size(), 0);
    ASSERT_EQ(cache->GetCurrentWeight(), 0);

    // --- Phase 1: Put multiple entries ---
    auto file_a = make_lookup_file("a.sst", 100);
    auto file_b = make_lookup_file("b.sst", 100);
    auto file_c = make_lookup_file("c.sst", 100);
    std::string path_a = file_a->LocalFile();
    std::string path_b = file_b->LocalFile();
    std::string path_c = file_c->LocalFile();

    ASSERT_OK(cache->Put("a", file_a));
    ASSERT_OK(cache->Put("b", file_b));
    ASSERT_OK(cache->Put("c", file_c));
    ASSERT_EQ(cache->Size(), 3);
    ASSERT_EQ(cache->GetCurrentWeight(), 300);

    // All local files should exist
    ASSERT_TRUE(fs->Exists(path_a).value());
    ASSERT_TRUE(fs->Exists(path_b).value());
    ASSERT_TRUE(fs->Exists(path_c).value());

    // --- Phase 2: Replace an entry ---
    // Replace "b" with a new file; old file_b should be closed and deleted
    auto file_b2 = make_lookup_file("b2.sst", 80);
    std::string path_b2 = file_b2->LocalFile();
    ASSERT_OK(cache->Put("b", file_b2));
    ASSERT_EQ(cache->Size(), 3);
    ASSERT_EQ(cache->GetCurrentWeight(), 280);  // 100 + 80 + 100

    // Old b.sst should be deleted by RemovalCallback (REPLACED cause)
    ASSERT_FALSE(fs->Exists(path_b).value());
    ASSERT_EQ(call_back_files, std::vector<std::string>({"b.sst"}));
    // New b2.sst should exist
    ASSERT_TRUE(fs->Exists(path_b2).value());

    // --- Phase 3: Weight-based eviction ---
    // Add a large file that pushes total over 300 bytes
    auto file_d = make_lookup_file("d.sst", 150);
    std::string path_d = file_d->LocalFile();
    ASSERT_OK(cache->Put("d", file_d));
    // Total would be 100 + 80 + 100 + 150 = 430 > 300
    // LRU eviction should remove "a" first (least recently used), then "c"
    // After eviction: weight should be 230
    ASSERT_EQ(cache->GetCurrentWeight(), 230);

    // "a" and "c" were LRU (inserted first, never accessed again), should be evicted and file
    // deleted
    ASSERT_FALSE(cache->GetIfPresent("a").has_value());
    ASSERT_FALSE(cache->GetIfPresent("c").has_value());
    ASSERT_EQ(call_back_files, std::vector<std::string>({"b.sst", "a.sst", "c.sst"}));
    ASSERT_FALSE(fs->Exists(path_a).value());
    ASSERT_FALSE(fs->Exists(path_c).value());

    // "d" should be in cache
    ASSERT_TRUE(cache->GetIfPresent("d").has_value());
    ASSERT_TRUE(fs->Exists(path_d).value());

    // --- Phase 4: Explicit invalidation ---
    // Invalidate "b" (the replaced entry)
    cache->Invalidate("b");
    ASSERT_FALSE(cache->GetIfPresent("b").has_value());
    // b2.sst should be deleted
    ASSERT_FALSE(fs->Exists(path_b2).value());
    ASSERT_EQ(call_back_files, std::vector<std::string>({"b.sst", "a.sst", "c.sst", "b2.sst"}));

    // --- Phase 5: InvalidateAll ---
    cache->InvalidateAll();
    ASSERT_EQ(cache->Size(), 0);
    ASSERT_EQ(cache->GetCurrentWeight(), 0);
    // d.sst should be deleted
    ASSERT_FALSE(fs->Exists(path_d).value());
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(fs->ListDir(tmp_dir->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());
    ASSERT_EQ(call_back_files,
              std::vector<std::string>({"b.sst", "a.sst", "c.sst", "b2.sst", "d.sst"}));
}
}  // namespace paimon::test
