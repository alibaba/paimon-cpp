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

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/io/type_fwd.h"
#include "arrow/util/future.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ArrowStreamAdapterTest, TestInputAndOutputStream) {
    auto test_root_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->Mkdirs(test_root));
    std::string file_name = test_root + "/stream.arrow";

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    auto out_stream = std::make_unique<ArrowOutputStreamAdapter>(out);
    ASSERT_EQ(out_stream->Tell().ValueOrDie(), 0);
    ASSERT_FALSE(out_stream->closed());

    std::string data = "hello";
    ASSERT_TRUE(out_stream->Write(data.data(), data.length()).ok());
    ASSERT_TRUE(out_stream->Flush().ok());
    ASSERT_EQ(out_stream->Tell().ValueOrDie(), 5);
    // noted that ArrowOutputStreamAdapter::Close() api do nothing except set the closed_ flag.
    ASSERT_TRUE(out_stream->Close().ok());
    ASSERT_TRUE(out_stream->closed());
    ASSERT_OK(out_stream->out_->Close());

    // in stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(uint64_t length, in->Length());
    auto in_stream =
        std::make_unique<ArrowInputStreamAdapter>(in, GetArrowPool(GetDefaultPool()), length);
    ASSERT_EQ(in_stream->GetSize().ValueOrDie(), static_cast<int64_t>(data.length()));
    ASSERT_EQ(in_stream->Tell().ValueOrDie(), 0);
    ASSERT_FALSE(in_stream->closed());

    char ret[10] = {};
    int64_t read_len = in_stream->Read(data.length(), ret).ValueOrDie();
    ASSERT_EQ(read_len, data.length());
    ASSERT_EQ(std::string(ret, read_len), data);
    ASSERT_EQ(in_stream->Tell().ValueOrDie(), 5);

    ASSERT_TRUE(in_stream->Seek(/*position=*/3).ok());
    std::shared_ptr<arrow::Buffer> buffer = in_stream->Read(/*nbytes=*/2).ValueOrDie();
    ASSERT_EQ(buffer->ToString(), "lo");

    auto fut = in_stream->ReadAsync(arrow::io::default_io_context(), /*position=*/0, /*nbytes=*/5);
    auto buffer2 = fut.result().ValueOrDie();
    ASSERT_EQ(data, buffer2->ToString());

    auto buffer3 = in_stream->ReadAt(/*position=*/1, /*nbytes=*/2).ValueOrDie();
    ASSERT_EQ(buffer3->ToString(), "el");
    int64_t read_len2 = in_stream->ReadAt(/*position=*/4, /*nbytes=*/1, ret).ValueOrDie();
    ASSERT_EQ(read_len2, 1);
    ASSERT_EQ(std::string(ret, read_len2), "o");

    ASSERT_TRUE(in_stream->Close().ok());
    ASSERT_TRUE(in_stream->closed());
}

}  // namespace paimon::test
