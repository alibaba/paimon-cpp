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

#include "paimon/common/global_index/btree/btree_file_meta_selector.h"

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BTreeFileMetaSelectorTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        key_type_ = arrow::int32();

        // file1: keys [1, 10], has_nulls=true
        // file2: keys [15, 20], has_nulls=false
        // file3: keys [21, 30], has_nulls=true
        // file4: keys [1, 5], has_nulls=false
        // file5: keys [19, 25], has_nulls=true
        auto meta1 = std::make_shared<BTreeIndexMeta>(SerializeInt(1), SerializeInt(10), true);
        auto meta2 = std::make_shared<BTreeIndexMeta>(SerializeInt(15), SerializeInt(20), false);
        auto meta3 = std::make_shared<BTreeIndexMeta>(SerializeInt(21), SerializeInt(30), true);
        auto meta4 = std::make_shared<BTreeIndexMeta>(SerializeInt(1), SerializeInt(5), false);
        auto meta5 = std::make_shared<BTreeIndexMeta>(SerializeInt(19), SerializeInt(25), true);

        // file6: only-nulls file (no keys, has_nulls=true)
        auto meta6 = std::make_shared<BTreeIndexMeta>(nullptr, nullptr, true);
        files_ = {
            GlobalIndexIOMeta("file1", 1, meta1->Serialize(pool_.get())),
            GlobalIndexIOMeta("file2", 1, meta2->Serialize(pool_.get())),
            GlobalIndexIOMeta("file3", 1, meta3->Serialize(pool_.get())),
            GlobalIndexIOMeta("file4", 1, meta4->Serialize(pool_.get())),
            GlobalIndexIOMeta("file5", 1, meta5->Serialize(pool_.get())),
            GlobalIndexIOMeta("file6", 1, meta6->Serialize(pool_.get())),
        };
    }

    std::shared_ptr<Bytes> SerializeInt(int32_t value) const {
        Literal literal(value);
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<Bytes> result,
                             KeySerializer::SerializeKey(literal, key_type_, pool_.get()));
        return result;
    }

    std::set<std::string> FileNames(const std::vector<GlobalIndexIOMeta>& metas) const {
        std::set<std::string> names;
        for (const auto& meta : metas) {
            names.insert(meta.file_path);
        }
        return names;
    }

    void CheckResult(const std::vector<GlobalIndexIOMeta>& actual,
                     const std::set<std::string>& expected) const {
        ASSERT_EQ(FileNames(actual), expected);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::DataType> key_type_;
    std::vector<GlobalIndexIOMeta> files_;
};

TEST_F(BTreeFileMetaSelectorTest, TestVisitLessThan) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // minKey < 8: file1(1), file4(1)
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitLessThan(Literal(8)));
    CheckResult(result, {"file1", "file4"});

    // minKey < 15: file1(1), file4(1)  (file2 minKey=15, not < 15)
    ASSERT_OK_AND_ASSIGN(result, selector.VisitLessThan(Literal(15)));
    CheckResult(result, {"file1", "file4"});

    // minKey < 1: no file has minKey < 1
    ASSERT_OK_AND_ASSIGN(result, selector.VisitLessThan(Literal(1)));
    ASSERT_TRUE(result.empty());
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitLessOrEqual) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // minKey <= 20: file1(1), file2(15), file4(1), file5(19)
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitLessOrEqual(Literal(20)));
    CheckResult(result, {"file1", "file2", "file4", "file5"});

    // minKey <= 15: file1(1), file2(15), file4(1)
    ASSERT_OK_AND_ASSIGN(result, selector.VisitLessOrEqual(Literal(15)));
    CheckResult(result, {"file1", "file2", "file4"});
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitGreaterThan) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // maxKey > 20: file3(30), file5(25)
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitGreaterThan(Literal(20)));
    CheckResult(result, {"file3", "file5"});

    // maxKey > 30: no file
    ASSERT_OK_AND_ASSIGN(result, selector.VisitGreaterThan(Literal(30)));
    ASSERT_TRUE(result.empty());
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitGreaterOrEqual) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // maxKey >= 5: all non-null files (file1..file5)
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitGreaterOrEqual(Literal(5)));
    CheckResult(result, {"file1", "file2", "file3", "file4", "file5"});

    // maxKey >= 20: file2(20), file3(30), file5(25)
    ASSERT_OK_AND_ASSIGN(result, selector.VisitGreaterOrEqual(Literal(20)));
    CheckResult(result, {"file2", "file3", "file5"});
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitEqual) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // 22 in [21,30] and [19,25]
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitEqual(Literal(22)));
    CheckResult(result, {"file3", "file5"});

    // 30 in [21,30] only
    ASSERT_OK_AND_ASSIGN(result, selector.VisitEqual(Literal(30)));
    CheckResult(result, {"file3"});

    // 100 out of all ranges
    ASSERT_OK_AND_ASSIGN(result, selector.VisitEqual(Literal(100)));
    ASSERT_TRUE(result.empty());
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitNotEqual) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // NotEqual cannot prune any file, returns all
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitNotEqual(Literal(22)));
    CheckResult(result, {"file1", "file2", "file3", "file4", "file5", "file6"});
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitIsNull) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // has_nulls: file1, file3, file5, file6
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitIsNull());
    CheckResult(result, {"file1", "file3", "file5", "file6"});
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitIsNotNull) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // !onlyNulls: file1..file5 (file6 is only-nulls)
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitIsNotNull());
    CheckResult(result, {"file1", "file2", "file3", "file4", "file5"});
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitIn) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // IN(1, 2, 3, 26, 27, 28):
    //   1 in [1,10]=file1, [1,5]=file4
    //   2 in [1,10]=file1, [1,5]=file4
    //   3 in [1,10]=file1, [1,5]=file4
    //   26 in [21,30]=file3
    //   27 in [21,30]=file3
    //   28 in [21,30]=file3
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitIn({Literal(1), Literal(2), Literal(3),
                                                        Literal(26), Literal(27), Literal(28)}));
    CheckResult(result, {"file1", "file3", "file4"});

    // IN(100): no match
    ASSERT_OK_AND_ASSIGN(result, selector.VisitIn({Literal(100)}));
    ASSERT_TRUE(result.empty());
}

TEST_F(BTreeFileMetaSelectorTest, TestVisitNotIn) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // NotIn cannot prune any file
    ASSERT_OK_AND_ASSIGN(auto result,
                         selector.VisitNotIn({Literal(1), Literal(7), Literal(19), Literal(30)}));
    CheckResult(result, {"file1", "file2", "file3", "file4", "file5", "file6"});
}

TEST_F(BTreeFileMetaSelectorTest, TestOnlyNullsFileExcludedFromRangeQueries) {
    BTreeFileMetaSelector selector(files_, key_type_, pool_);

    // file6 is only-nulls, should be excluded from all range/equality queries
    ASSERT_OK_AND_ASSIGN(auto result, selector.VisitEqual(Literal(1)));
    auto names = FileNames(result);
    ASSERT_EQ(names.count("file6"), 0u);

    ASSERT_OK_AND_ASSIGN(result, selector.VisitLessThan(Literal(100)));
    names = FileNames(result);
    ASSERT_EQ(names.count("file6"), 0u);

    ASSERT_OK_AND_ASSIGN(result, selector.VisitGreaterThan(Literal(0)));
    names = FileNames(result);
    ASSERT_EQ(names.count("file6"), 0u);

    // But IsNull should include file6
    ASSERT_OK_AND_ASSIGN(result, selector.VisitIsNull());
    names = FileNames(result);
    ASSERT_EQ(names.count("file6"), 1u);
}

}  // namespace paimon::test
