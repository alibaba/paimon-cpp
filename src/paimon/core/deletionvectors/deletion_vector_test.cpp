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

#include "paimon/core/deletionvectors/deletion_vector.h"

#include <cstdlib>
#include <cstring>
#include <set>

#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DeletionVectorTest, TestSimple) {
    std::set<int32_t> to_deleted;
    for (int32_t i = 0; i < 10000; i++) {
        to_deleted.insert(paimon::test::RandomNumber(0, 100000000l));
    }
    std::set<int32_t> not_deleted;
    for (int32_t i = 0; i < 10000; i++) {
        if (to_deleted.find(i) == to_deleted.end()) {
            not_deleted.insert(i);
        }
    }
    RoaringBitmap32 roaring;
    auto deletion_vector = std::make_unique<BitmapDeletionVector>(roaring);
    ASSERT_TRUE(deletion_vector->IsEmpty());
    for (auto i : to_deleted) {
        if (i % 2 == 0) {
            ASSERT_OK(deletion_vector->Delete(i));
        } else {
            ASSERT_TRUE(deletion_vector->CheckedDelete(i).value());
            ASSERT_FALSE(deletion_vector->CheckedDelete(i).value());
        }
    }
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto bytes, deletion_vector->SerializeToBytes(pool));
    ASSERT_OK_AND_ASSIGN(auto de_deletion_vector,
                         DeletionVector::DeserializeFromBytes(bytes.get(), pool.get()));

    ASSERT_FALSE(deletion_vector->IsEmpty());
    ASSERT_FALSE(de_deletion_vector->IsEmpty());

    for (auto i : to_deleted) {
        ASSERT_TRUE(deletion_vector->IsDeleted(i).value());
        ASSERT_TRUE(de_deletion_vector->IsDeleted(i).value());
    }
    for (auto i : not_deleted) {
        ASSERT_FALSE(deletion_vector->IsDeleted(i).value());
        ASSERT_FALSE(de_deletion_vector->IsDeleted(i).value());
    }
}
TEST(DeletionVectorTest, TestCompatibleWithJava) {
    // generated from java, with magic_num and bitmap, deleted row is {1, 2, 4}
    std::vector<uint8_t> data = {94, 67, 242, 208, 58, 48, 0, 0, 1, 0, 0, 0, 0,
                                 0,  2,  0,   16,  0,  0,  0, 1, 0, 2, 0, 4, 0};
    auto pool = GetDefaultPool();
    auto serialize_bytes = std::make_shared<Bytes>(data.size(), pool.get());
    memcpy(serialize_bytes->data(), data.data(), data.size());

    // test deserialize
    ASSERT_OK_AND_ASSIGN(auto deletion_vector,
                         DeletionVector::DeserializeFromBytes(serialize_bytes.get(), pool.get()));
    std::vector<bool> expected = {false, true, true, false, true, false};
    std::vector<bool> result;
    for (size_t i = 0; i < expected.size(); i++) {
        result.emplace_back(deletion_vector->IsDeleted(i).value());
    }
    ASSERT_EQ(expected, result);

    // test serialize
    ASSERT_OK_AND_ASSIGN(auto serialized_dv, deletion_vector->SerializeToBytes(pool));
    ASSERT_EQ(*serialized_dv, *serialize_bytes);
}
}  // namespace paimon::test
