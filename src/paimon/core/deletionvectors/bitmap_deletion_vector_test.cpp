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

#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"

#include <set>

#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(BitmapDeletionVectorTest, BasicOperations) {
    RoaringBitmap32 roaring;
    BitmapDeletionVector dv(roaring);
    ASSERT_TRUE(dv.IsEmpty());
    for (int32_t i = 0; i < 2000; i += 2) {
        ASSERT_OK(dv.Delete(i));
    }
    ASSERT_EQ(dv.GetCardinality(), 1000);
    for (int32_t i = 0; i < 2000; ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(dv.IsDeleted(i).value());
        } else {
            ASSERT_FALSE(dv.IsDeleted(i).value());
        }
    }
}

TEST(BitmapDeletionVectorTest, CheckedDelete) {
    RoaringBitmap32 roaring;
    BitmapDeletionVector dv(roaring);
    ASSERT_TRUE(dv.CheckedDelete(42).value());
    ASSERT_FALSE(dv.CheckedDelete(42).value());
    ASSERT_TRUE(dv.IsDeleted(42).value());
}

TEST(BitmapDeletionVectorTest, SerializeAndDeserialize) {
    RoaringBitmap32 roaring;
    for (int32_t i = 0; i < 100; i += 3) {
        roaring.Add(i);
    }
    BitmapDeletionVector dv(roaring);
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto bytes, dv.SerializeToBytes(pool));
    ASSERT_OK_AND_ASSIGN(
        auto dv2, BitmapDeletionVector::Deserialize(bytes->data(), bytes->size(), pool.get()));
    for (int32_t i = 0; i < 100; ++i) {
        ASSERT_EQ(dv.IsDeleted(i).value(), dv2->IsDeleted(i).value());
    }
}

TEST(BitmapDeletionVectorTest, SerializeToOutputStream) {
    RoaringBitmap32 roaring;
    for (int32_t i = 0; i < 50; i += 2) {
        roaring.Add(i);
    }
    BitmapDeletionVector dv(roaring);
    auto pool = GetDefaultPool();
    auto dir = UniqueTestDirectory::Create();
    auto path = PathUtil::JoinPath(dir->Str(), "dv");
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", path, {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output_stream,
                         fs->Create(path, /*overwrite=*/false));
    DataOutputStream out(output_stream);
    ASSERT_OK_AND_ASSIGN(int64_t size, dv.SerializeTo(pool, &out));
    ASSERT_OK(output_stream->Flush());
    ASSERT_OK(output_stream->Close());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs->Open(path));
    DataInputStream in(input_stream);
    ASSERT_OK_AND_ASSIGN(int32_t byte_len, in.ReadValue<int32_t>());
    auto bytes = Bytes::AllocateBytes(byte_len, pool.get());
    ASSERT_OK(in.Read(bytes->data(), bytes->size()));
    const char* data = bytes->data();
    ASSERT_EQ(bytes->size(), size);
    ASSERT_OK_AND_ASSIGN(auto dv2,
                         BitmapDeletionVector::Deserialize(data, bytes->size(), pool.get()));
    for (int32_t i = 0; i < 50; ++i) {
        ASSERT_EQ(dv.IsDeleted(i).value(), dv2->IsDeleted(i).value());
    }
}

TEST(BitmapDeletionVectorTest, GetCardinality) {
    RoaringBitmap32 empty;
    BitmapDeletionVector dv_empty(empty);
    ASSERT_EQ(dv_empty.GetCardinality(), 0);

    RoaringBitmap32 cont;
    for (int32_t i = 0; i < 100; ++i) cont.Add(i);
    BitmapDeletionVector dv_cont(cont);
    ASSERT_EQ(dv_cont.GetCardinality(), 100);

    RoaringBitmap32 gap;
    for (int32_t i = 0; i < 1000; i += 10) gap.Add(i);
    BitmapDeletionVector dv_gap(gap);
    ASSERT_EQ(dv_gap.GetCardinality(), 100);

    RoaringBitmap32 del;
    for (int32_t i = 0; i < 10; ++i) del.Add(i);
    BitmapDeletionVector dv_del(del);
    ASSERT_EQ(dv_del.GetCardinality(), 10);
    ASSERT_OK(dv_del.Delete(100));
    ASSERT_EQ(dv_del.GetCardinality(), 11);
}

TEST(BitmapDeletionVectorTest, PositionOutOfRangeShouldFail) {
    RoaringBitmap32 roaring;
    BitmapDeletionVector dv(roaring);
    int64_t invalid_position = static_cast<int64_t>(RoaringBitmap32::MAX_VALUE) + 1;

    ASSERT_NOK_WITH_MSG(dv.Delete(invalid_position), "too many rows");
    ASSERT_NOK_WITH_MSG(dv.CheckedDelete(invalid_position), "too many rows");
    ASSERT_NOK_WITH_MSG(dv.IsDeleted(invalid_position), "too many rows");
}

TEST(BitmapDeletionVectorTest, DeserializeShouldRejectInvalidMagicNumber) {
    auto pool = GetDefaultPool();
    MemorySegmentOutputStream output(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    output.WriteValue<int32_t>(BitmapDeletionVector::MAGIC_NUMBER + 1);

    auto payload = MemorySegmentUtils::CopyToBytes(output.Segments(), /*offset=*/0,
                                                   output.CurrentSize(), pool.get());

    ASSERT_NOK_WITH_MSG(
        BitmapDeletionVector::Deserialize(payload->data(), payload->size(), pool.get()),
        fmt::format("invalid magic number: {}", BitmapDeletionVector::MAGIC_NUMBER + 1));
}

TEST(BitmapDeletionVectorTest, DeserializeWithoutMagicNumberShouldRoundTrip) {
    auto pool = GetDefaultPool();

    RoaringBitmap32 roaring;
    roaring.Add(1);
    roaring.Add(7);
    roaring.Add(1024);
    BitmapDeletionVector dv(roaring);

    ASSERT_OK_AND_ASSIGN(auto bytes_with_magic, dv.SerializeToBytes(pool));
    ASSERT_OK_AND_ASSIGN(
        auto deserialized,
        BitmapDeletionVector::DeserializeWithoutMagicNumber(
            bytes_with_magic->data() + BitmapDeletionVector::MAGIC_NUMBER_SIZE_BYTES,
            bytes_with_magic->size() - BitmapDeletionVector::MAGIC_NUMBER_SIZE_BYTES, pool.get()));

    ASSERT_TRUE(deserialized->IsDeleted(1).value());
    ASSERT_TRUE(deserialized->IsDeleted(7).value());
    ASSERT_TRUE(deserialized->IsDeleted(1024).value());
    ASSERT_FALSE(deserialized->IsDeleted(8).value());
}

}  // namespace paimon::test
