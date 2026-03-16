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
        fs, local_file, /*level=*/3, /*schema_id=*/1,
        /*ser_version=*/"v1", std::make_unique<FakeLookupStoreReader>(kvs, pool));
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
        auto partition = BinaryRowGenerator::GenerateRow({"20240731", 10}, pool.get());
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
}  // namespace paimon::test
