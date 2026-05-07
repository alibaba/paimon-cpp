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

#include "paimon/io/buffered_input_stream.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(BufferedInputStreamTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto output_stream = std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/8, pool);
    std::string str = "abcdef";
    auto bytes = std::make_shared<Bytes>(str, pool.get());
    output_stream->WriteBytes(bytes);
    auto out_bytes = MemorySegmentUtils::CopyToBytes(output_stream->Segments(), 0,
                                                     output_stream->CurrentSize(), pool.get());
    auto in = std::make_shared<ByteArrayInputStream>(out_bytes->data(), out_bytes->size());
    auto input_stream = std::make_shared<BufferedInputStream>(in, /*buffer_size=*/4, pool.get());
    ASSERT_EQ(6, input_stream->Length().value());
    ASSERT_TRUE(input_stream->GetUri().value().empty());

    // read from pos 0
    std::string value(3, '\0');
    ASSERT_EQ(3, input_stream->Read(value.data(), value.size()).value());
    ASSERT_EQ("abc", value);
    ASSERT_EQ(3, input_stream->GetPos().value());
    ASSERT_EQ(3, input_stream->Read(value.data(), value.size()).value());
    ASSERT_EQ("def", value);
    ASSERT_EQ(6, input_stream->GetPos().value());

    // read from pos 1
    ASSERT_NOK_WITH_MSG(input_stream->Read(value.data(), value.size(), /*offset=*/1),
                        "BufferedInputStream does not support Read from offset");
    ASSERT_EQ(6, input_stream->GetPos().value());

    // seek to pos 3
    ASSERT_OK(input_stream->Seek(3, SeekOrigin::FS_SEEK_SET));
    ASSERT_EQ(3, input_stream->Read(value.data(), value.size()).value());
    ASSERT_EQ("def", value);
    ASSERT_EQ(6, input_stream->GetPos().value());

    // seek to pos 2
    ASSERT_OK(input_stream->Seek(-4, SeekOrigin::FS_SEEK_END));
    ASSERT_EQ(3, input_stream->Read(value.data(), value.size()).value());
    ASSERT_EQ("cde", value);
    ASSERT_EQ(5, input_stream->GetPos().value());

    // seek to pos 1
    ASSERT_OK(input_stream->Seek(-4, SeekOrigin::FS_SEEK_CUR));
    ASSERT_EQ(3, input_stream->Read(value.data(), value.size()).value());
    ASSERT_EQ("bcd", value);
    ASSERT_EQ(4, input_stream->GetPos().value());

    // test exceed eof, seek to pos 4, want to read 4 bytes
    ASSERT_OK(input_stream->Seek(-2, SeekOrigin::FS_SEEK_END));
    ASSERT_NOK_WITH_MSG(input_stream->Read(value.data(), value.size()),
                        "InnerRead failed, after Fill(), still no bytes available (may read eof), "
                        "but expect read 1 bytes");

    // test invalid seek
    ASSERT_NOK_WITH_MSG(input_stream->Seek(100, SeekOrigin::FS_SEEK_CUR),
                        "invalid seek, after seek, current pos 106, length 6");

    // test ReadAsync not implemented
    bool read_finished = false;
    auto callback = [&](Status status) {
        ASSERT_TRUE(status.IsNotImplemented());
        read_finished = true;
    };
    input_stream->ReadAsync(value.data(), value.size(), /*offset=*/0, callback);
    ASSERT_TRUE(read_finished);

    ASSERT_OK(input_stream->Close());
}

TEST(BufferedInputStreamTest, TestSeek) {
    // Data: "0123456789abcdef" (16 bytes), buffer_size = 8
    auto pool = GetDefaultPool();
    std::string data = "0123456789abcdef";
    auto in = std::make_shared<ByteArrayInputStream>(reinterpret_cast<const char*>(data.data()),
                                                     data.size());
    auto stream = std::make_shared<BufferedInputStream>(in, /*buffer_size=*/8, pool.get());

    // Helper: verify buffer_ content matches expected substring of data
    auto check_buffer = [&](const BufferedInputStream& s, const std::string& expected_content,
                            int32_t expected_pos, int32_t expected_count) {
        ASSERT_EQ(s.pos_, expected_pos);
        ASSERT_EQ(s.count_, expected_count);
        std::string actual(s.buffer_->data(), s.buffer_->data() + expected_count);
        ASSERT_EQ(actual, expected_content) << "buffer content mismatch: actual=\"" << actual
                                            << "\", expected=\"" << expected_content << "\"";
    };

    std::string buf(4, '\0');

    // Initial state: buffer empty
    ASSERT_EQ(stream->pos_, 0);
    ASSERT_EQ(stream->count_, 0);

    // FS_SEEK_SET slow path (buffer empty, count_==0): seek to pos 4, read "4567"
    // First Read calls Fill: reads 8 bytes from pos 4 -> buffer = "456789ab", count_=8.
    // Then consumes 4 bytes -> pos_=4.
    {
        ASSERT_OK(stream->Seek(4, SeekOrigin::FS_SEEK_SET));
        // After seek: slow path, buffer invalidated
        ASSERT_EQ(stream->pos_, 0);
        ASSERT_EQ(stream->count_, 0);
        ASSERT_EQ(4, stream->GetPos().value());

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("4567", buf);
        check_buffer(*stream, "456789ab", 4, 8);
    }

    // FS_SEEK_CUR buffer hit: pos=8, seek -4 -> target=4, inside buffer [4..12)
    // Fast path: only adjusts pos_ to 0, count_ stays 8, buffer unchanged.
    {
        ASSERT_OK(stream->Seek(-4, SeekOrigin::FS_SEEK_CUR));
        check_buffer(*stream, "456789ab", 0, 8);
        ASSERT_EQ(4, stream->GetPos().value());

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("4567", buf);
        check_buffer(*stream, "456789ab", 4, 8);
    }

    // FS_SEEK_SET buffer hit: pos=8, seek to 5, inside buffer [4..12)
    // Fast path: pos_ = 5 - 4 = 1, count_ stays 8, buffer unchanged.
    {
        ASSERT_OK(stream->Seek(5, SeekOrigin::FS_SEEK_SET));
        check_buffer(*stream, "456789ab", 1, 8);
        ASSERT_EQ(5, stream->GetPos().value());

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("5678", buf);
        check_buffer(*stream, "456789ab", 5, 8);
    }

    // FS_SEEK_END buffer miss: target = 16 + (-6) = 10
    // Current buffer covers [4..12). pos 10 is inside [4..12) -> actually buffer hit!
    // Fast path: pos_ = 10 - 4 = 6, count_ stays 8.
    {
        ASSERT_OK(stream->Seek(-6, SeekOrigin::FS_SEEK_END));
        check_buffer(*stream, "456789ab", 6, 8);
        ASSERT_EQ(10, stream->GetPos().value());

        // Read 4 bytes from pos 10: 2 left in buffer ("ab"), then refill.
        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("abcd", buf);
        // After consuming "ab" (pos_==count_==8), InnerRead calls Fill from in_ pos 12,
        // reads [12..16) -> buffer = "cdef", count_=4, then consumes 2 -> pos_=2.
        check_buffer(*stream, "cdef", 2, 4);
    }

    // Buffer miss slow path: seek to pos 0, current buffer covers [12..16).
    // Target 0 is outside [12..16) -> slow path, buffer invalidated.
    {
        ASSERT_OK(stream->Seek(0, SeekOrigin::FS_SEEK_SET));
        ASSERT_EQ(stream->pos_, 0);
        ASSERT_EQ(stream->count_, 0);
        ASSERT_EQ(0, stream->GetPos().value());

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("0123", buf);
        // Fill reads [0..8) -> buffer = "01234567", count_=8, pos_=4
        check_buffer(*stream, "01234567", 4, 8);
    }

    // Buffer hit at exact boundary start: seek to pos 0 (= buf_start_abs=0)
    // Fast path: pos_ = 0, count_ stays 8, buffer unchanged.
    {
        ASSERT_OK(stream->Seek(0, SeekOrigin::FS_SEEK_SET));
        check_buffer(*stream, "01234567", 0, 8);

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("0123", buf);
        check_buffer(*stream, "01234567", 4, 8);
    }

    // Buffer hit at exact boundary end: seek to pos 8 (= buf_end_abs=8)
    // Fast path: pos_ = 8 == count_, buffer unchanged but next Read triggers refill.
    {
        ASSERT_OK(stream->Seek(8, SeekOrigin::FS_SEEK_SET));
        check_buffer(*stream, "01234567", 8, 8);
        ASSERT_EQ(8, stream->GetPos().value());

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("89ab", buf);
        // pos_==count_ triggered Fill: buffer = "89abcdef", count_=8, pos_=4
        check_buffer(*stream, "89abcdef", 4, 8);
    }

    // FS_SEEK_CUR buffer miss: pos=12, seek -12 -> target=0, outside [8..16)
    {
        ASSERT_OK(stream->Seek(-12, SeekOrigin::FS_SEEK_CUR));
        ASSERT_EQ(stream->pos_, 0);
        ASSERT_EQ(stream->count_, 0);

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("0123", buf);
        check_buffer(*stream, "01234567", 4, 8);
    }

    // FS_SEEK_END buffer hit: fill buffer near file end, then seek within via FS_SEEK_END.
    {
        ASSERT_OK(stream->Seek(12, SeekOrigin::FS_SEEK_SET));
        // pos 12 is outside current buffer [0..8) -> slow path
        ASSERT_EQ(stream->pos_, 0);
        ASSERT_EQ(stream->count_, 0);

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("cdef", buf);
        // Fill from 12: buffer = "cdef", count_=4, pos_=4
        check_buffer(*stream, "cdef", 4, 4);

        // Seek -4 from end -> target = 12, buffer covers [12..16), 12 is inside.
        // Fast path: pos_ = 12 - 12 = 0
        ASSERT_OK(stream->Seek(-4, SeekOrigin::FS_SEEK_END));
        check_buffer(*stream, "cdef", 0, 4);
        ASSERT_EQ(12, stream->GetPos().value());

        ASSERT_EQ(4, stream->Read(buf.data(), 4).value());
        ASSERT_EQ("cdef", buf);
        check_buffer(*stream, "cdef", 4, 4);
    }

    // Empty buffer (count_==0): fresh stream, seek always takes slow path.
    {
        auto in2 = std::make_shared<ByteArrayInputStream>(
            reinterpret_cast<const char*>(data.data()), data.size());
        auto fresh = std::make_shared<BufferedInputStream>(in2, /*buffer_size=*/8, pool.get());
        ASSERT_EQ(fresh->pos_, 0);
        ASSERT_EQ(fresh->count_, 0);

        ASSERT_OK(fresh->Seek(10, SeekOrigin::FS_SEEK_SET));
        ASSERT_EQ(fresh->pos_, 0);
        ASSERT_EQ(fresh->count_, 0);
        ASSERT_EQ(10, fresh->GetPos().value());

        ASSERT_EQ(4, fresh->Read(buf.data(), 4).value());
        ASSERT_EQ("abcd", buf);
        // Fill from 10: buffer = "abcdef", count_=6, pos_=4
        check_buffer(*fresh, "abcdef", 4, 6);
    }
}
}  // namespace paimon::test
