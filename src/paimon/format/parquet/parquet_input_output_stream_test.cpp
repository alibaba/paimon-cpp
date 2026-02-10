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

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/io/type_fwd.h"
#include "arrow/util/future.h"
#include "gtest/gtest.h"
#include "paimon/format/parquet/parquet_input_stream_impl.h"
#include "paimon/format/parquet/parquet_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {

TEST(ParquetInputOutputStreamTest, TestInOutStream) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->Mkdirs(test_root));
    std::string file_name = test_root + "/test.parquet";

    // out stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    auto out_stream = std::make_unique<ParquetOutputStreamImpl>(out);
    int64_t out_tell1 = out_stream->Tell().ValueOr(-1);
    ASSERT_EQ(out_tell1, 0);
    ASSERT_FALSE(out_stream->closed());

    std::string data = "hello";
    ASSERT_TRUE(out_stream->Write(data.data(), data.length()).ok());
    ASSERT_TRUE(out_stream->Flush().ok());
    int64_t out_tell2 = out_stream->Tell().ValueOr(-1);
    ASSERT_EQ(out_tell2, 5);
    // noted that ParquetOutputStreamImpl::close() api do nothing except set the closed_ flag.
    ASSERT_TRUE(out_stream->Close().ok());
    ASSERT_TRUE(out_stream->closed());
    ASSERT_OK(out_stream->out_->Close());

    // in stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(uint64_t length, in->Length());
    auto in_stream = std::make_unique<ParquetInputStreamImpl>(in, GetDefaultPool(), length);
    int64_t file_length = in_stream->GetSize().ValueOr(-1);
    ASSERT_EQ(file_length, data.length());
    int64_t in_tell1 = in_stream->Tell().ValueOr(-1);
    ASSERT_EQ(in_tell1, 0);
    ASSERT_FALSE(in_stream->closed());

    char ret[10];
    int64_t read_len = in_stream->Read(data.length(), ret).ValueOr(-1);
    ASSERT_EQ(read_len, data.length());
    ASSERT_EQ(data, std::string(ret, data.length()));
    int64_t in_tell2 = in_stream->Tell().ValueOr(-1);
    ASSERT_EQ(in_tell2, 5);

    ASSERT_TRUE(in_stream->Seek(/*position=*/3).ok());
    std::shared_ptr<arrow::Buffer> buffer = in_stream->Read(/*nbytes=*/2).ValueOr(nullptr);
    ASSERT_TRUE(buffer);
    ASSERT_EQ(buffer->ToString(), "lo");

    auto fut = in_stream->ReadAsync(arrow::io::default_io_context(), /*position=*/0, /*nbytes=*/5);
    auto buffer2 = fut.result().ValueOrDie();
    ASSERT_EQ(data, buffer2->ToString());

    auto buffer3 = in_stream->ReadAt(/*position=*/1, /*nbytes=*/2).ValueOr(nullptr);
    ASSERT_TRUE(buffer3);
    ASSERT_EQ(buffer3->ToString(), "el");
    int64_t read_len2 = in_stream->ReadAt(/*position=*/4, /*nbytes=*/1, ret).ValueOr(-1);
    ASSERT_EQ(read_len2, 1);
    ASSERT_EQ(std::string(ret, read_len2), "o");

    ASSERT_TRUE(in_stream->Close().ok());
    ASSERT_TRUE(in_stream->closed());
}

}  // namespace paimon::parquet::test
