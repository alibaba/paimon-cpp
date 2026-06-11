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

#include "paimon/common/io/offset_input_stream.h"

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(OffsetInputStreamTest, TestBasicConstruction) {
    auto inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_OK_AND_ASSIGN(auto offset_stream, OffsetInputStream::Create(std::move(inner_stream),
                                                                       /*length=*/6, /*offset=*/2));

    ASSERT_OK_AND_ASSIGN(auto length, offset_stream->Length());
    ASSERT_EQ(6, length);

    ASSERT_OK_AND_ASSIGN(auto pos, offset_stream->GetPos());
    ASSERT_EQ(0, pos);

    ASSERT_OK_AND_ASSIGN(auto uri, offset_stream->GetUri());
    ASSERT_EQ("", uri);
}

TEST(OffsetInputStreamTest, TestSeekOperations) {
    auto inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_OK_AND_ASSIGN(auto offset_stream, OffsetInputStream::Create(std::move(inner_stream),
                                                                       /*length=*/6, /*offset=*/2));

    // Test FS_SEEK_SET
    ASSERT_OK(offset_stream->Seek(3, SeekOrigin::FS_SEEK_SET));
    ASSERT_OK_AND_ASSIGN(auto pos, offset_stream->GetPos());
    ASSERT_EQ(3, pos);

    // Test FS_SEEK_CUR
    ASSERT_OK(offset_stream->Seek(1, SeekOrigin::FS_SEEK_CUR));
    ASSERT_OK_AND_ASSIGN(pos, offset_stream->GetPos());
    ASSERT_EQ(4, pos);

    // Test FS_SEEK_END
    ASSERT_OK(offset_stream->Seek(-2, SeekOrigin::FS_SEEK_END));
    ASSERT_OK_AND_ASSIGN(pos, offset_stream->GetPos());
    ASSERT_EQ(4, pos);

    // Test boundary conditions
    ASSERT_NOK(offset_stream->Seek(-10, SeekOrigin::FS_SEEK_SET));
    ASSERT_NOK(offset_stream->Seek(10, SeekOrigin::FS_SEEK_SET));
    ASSERT_NOK(offset_stream->Seek(10, SeekOrigin::FS_SEEK_CUR));
    ASSERT_NOK(offset_stream->Seek(10, SeekOrigin::FS_SEEK_END));
}

TEST(OffsetInputStreamTest, TestReadOperations) {
    auto inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_OK_AND_ASSIGN(auto offset_stream, OffsetInputStream::Create(std::move(inner_stream),
                                                                       /*length=*/6, /*offset=*/2));

    // Test sequential read
    std::string buffer(4, '\0');
    ASSERT_OK_AND_ASSIGN(auto bytes_read, offset_stream->Read(buffer.data(), /*size=*/4));
    ASSERT_EQ(4, bytes_read);
    ASSERT_EQ("cdef", buffer);

    ASSERT_OK_AND_ASSIGN(auto pos, offset_stream->GetPos());
    ASSERT_EQ(4, pos);

    // Test read with offset
    std::string buffer2(2, '\0');
    ASSERT_OK_AND_ASSIGN(bytes_read, offset_stream->Read(buffer2.data(), /*size=*/2, /*offset=*/1));
    ASSERT_EQ(2, bytes_read);
    ASSERT_EQ("de", buffer2);

    // Position should not change after offset read
    ASSERT_OK_AND_ASSIGN(pos, offset_stream->GetPos());
    ASSERT_EQ(4, pos);

    // Test close
    ASSERT_OK(offset_stream->Close());
}

TEST(OffsetInputStreamTest, TestReadAsync) {
    auto inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_OK_AND_ASSIGN(auto offset_stream, OffsetInputStream::Create(std::move(inner_stream),
                                                                       /*length=*/6, /*offset=*/2));

    std::string buffer1(3, '\0'), buffer2(2, '\0');
    bool callback_called1 = false, callback_called2 = false;
    Status callback_status1, callback_status2;

    auto callback1 = [&](Status status) {
        callback_called1 = true;
        callback_status1 = status;
    };
    auto callback2 = [&](Status status) {
        callback_called2 = true;
        callback_status2 = status;
    };

    offset_stream->ReadAsync(buffer1.data(), 3, 1, std::move(callback1));
    offset_stream->ReadAsync(buffer2.data(), 2, 3, std::move(callback2));

    ASSERT_TRUE(callback_called1);
    ASSERT_OK(callback_status1);
    ASSERT_EQ("def", buffer1);
    ASSERT_TRUE(callback_called2);
    ASSERT_OK(callback_status2);
    ASSERT_EQ("fg", buffer2);

    // Position should not change after offset read
    ASSERT_OK_AND_ASSIGN(auto pos, offset_stream->GetPos());
    ASSERT_EQ(0, pos);
}

TEST(OffsetInputStreamTest, TestBoundaryValidation) {
    auto inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_OK_AND_ASSIGN(auto offset_stream, OffsetInputStream::Create(std::move(inner_stream),
                                                                       /*length=*/6, /*offset=*/2));

    // Test read beyond boundary
    std::string buffer(10, '\0');
    ASSERT_NOK_WITH_MSG(offset_stream->Read(buffer.data(), /*size=*/10),
                        "assert boundary failed: inner pos 10 exceed length 6");

    // Test offset read beyond boundary
    ASSERT_NOK_WITH_MSG(offset_stream->Read(buffer.data(), /*size=*/4, /*offset=*/5),
                        "assert boundary failed: inner pos 9 exceed length 6");
}

TEST(OffsetInputStreamTest, TestInvalidParameters) {
    // Test null wrapped stream
    ASSERT_NOK_WITH_MSG(OffsetInputStream::Create(nullptr, /*length=*/6, /*offset=*/2),
                        "input stream is null pointer");

    // Test negative offset
    auto inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_NOK_WITH_MSG(
        OffsetInputStream::Create(std::move(inner_stream), /*length=*/6, /*offset=*/-1),
        "offset -1 is less than 0");

    // Test negative length
    inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_NOK_WITH_MSG(
        OffsetInputStream::Create(std::move(inner_stream), /*length=*/-2, /*offset=*/2),
        "length -2 is less than 0");

    // Test length + offset beyond wrapped stream length
    inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_NOK_WITH_MSG(
        OffsetInputStream::Create(std::move(inner_stream), /*length=*/8, /*offset=*/7),
        "offset 7 + length 8 exceed total length 10");

    // Test offset beyond wrapped stream length
    inner_stream = std::make_unique<ByteArrayInputStream>("abcdefghij", /*length=*/10);
    ASSERT_NOK_WITH_MSG(
        OffsetInputStream::Create(std::move(inner_stream), /*length=*/1, /*offset=*/15),
        "offset 15 exceed total length 10");
}

}  // namespace paimon::test
