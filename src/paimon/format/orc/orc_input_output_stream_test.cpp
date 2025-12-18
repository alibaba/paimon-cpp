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

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {
TEST(OrcInputOutputStreamTest, TestInOutStream) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->Mkdirs(test_root));
    std::string file_name = test_root + "/test.orc";

    // out stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                         OrcOutputStreamImpl::Create(out));
    ASSERT_EQ(out_stream->getName(), file_name);
    ASSERT_EQ(out_stream->getNaturalWriteSize(), 128 * 1024);
    ASSERT_EQ(out_stream->getLength(), 0);

    std::string data = "hello";
    out_stream->write(data.data(), data.length());
    // noted that OrcOutputStreamImpl::close() api do nothing
    ASSERT_OK(out_stream->output_stream_->Close());

    // in stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto in_stream, OrcInputStreamImpl::Create(in, DEFAULT_NATURAL_READ_SIZE));

    char ret[10];
    in_stream->read(ret, data.length(), /*offset=*/0);
    ASSERT_EQ(data, std::string(ret, data.length()));
}

TEST(OrcInputOutputStreamTest, TestSimple) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->Mkdirs(test_root));
    std::string file_name = test_root + "/test.orc";

    // write process
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         file_system->Create(file_name, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> out_stream,
                         OrcOutputStreamImpl::Create(out));
    ASSERT_EQ(out_stream->getName(), file_name);
    ASSERT_EQ(out_stream->getNaturalWriteSize(), 128 * 1024);
    ASSERT_EQ(out_stream->getLength(), 0);

    std::string orc_schema = "struct<Col3:int,Col2:double,Col1:string>";
    std::unique_ptr<::orc::Type> type = ::orc::Type::buildTypeFromString(orc_schema);
    ::orc::WriterOptions writer_options;
    std::unique_ptr<::orc::Writer> writer =
        ::orc::createWriter(*type, out_stream.get(), writer_options);
    size_t batch_size = 11;
    std::unique_ptr<::orc::ColumnVectorBatch> batch = writer->createRowBatch(batch_size);
    auto* struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
    ASSERT_TRUE(struct_batch);
    auto* int_batch = dynamic_cast<::orc::LongVectorBatch*>(struct_batch->fields[0]);
    ASSERT_TRUE(int_batch);
    auto* double_batch = dynamic_cast<::orc::DoubleVectorBatch*>(struct_batch->fields[1]);
    ASSERT_TRUE(double_batch);
    auto* string_batch = dynamic_cast<::orc::StringVectorBatch*>(struct_batch->fields[2]);
    ASSERT_TRUE(string_batch);

    std::vector<std::tuple<int32_t, double, std::string>> raw_data;
    raw_data.reserve(batch_size);
    for (size_t i = 0; i < batch_size; i++) {
        raw_data.emplace_back(i, 0.1 + i, "str_" + std::to_string(i));
    }
    for (size_t i = 0; i < batch_size; i++) {
        const auto& [ivalue, dvalue, svalue] = raw_data[i];
        int_batch->data[i] = ivalue;
        double_batch->data[i] = dvalue;
        string_batch->data[i] = const_cast<char*>(svalue.c_str());
        string_batch->length[i] = static_cast<int32_t>(svalue.length());
    }
    struct_batch->numElements = batch_size;
    struct_batch->fields[0]->numElements = batch_size;
    struct_batch->fields[1]->numElements = batch_size;
    struct_batch->fields[2]->numElements = batch_size;
    writer->add(*batch);

    writer->close();
    ASSERT_OK(out->Close());
    ASSERT_TRUE(file_system->Exists(file_name).value());

    // read process
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto in_stream,
                         OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    auto length = file_system->GetFileStatus(file_name).value()->GetLen();
    ASSERT_EQ(in_stream->getName(), file_name);
    ASSERT_EQ(in_stream->getLength(), length);
    ASSERT_EQ(in_stream->getNaturalReadSize(), 1024 * 1024);

    ::orc::ReaderOptions reader_options;
    std::unique_ptr<::orc::Reader> reader =
        ::orc::createReader(std::move(in_stream), reader_options);
    ASSERT_EQ("struct<Col3:int,Col2:double,Col1:string>", reader->getType().toString());
    ::orc::RowReaderOptions options;
    // read with reversed field sequence
    options.include(std::list<std::string>({"Col1", "Col2", "Col3"}));
    std::unique_ptr<::orc::RowReader> row_reader = reader->createRowReader(options);
    auto sub_type_count = row_reader->getSelectedType().getSubtypeCount();
    ASSERT_EQ(3, sub_type_count);
    std::list<std::string> expect_selected_type = {"Col3", "Col2", "Col1"};
    std::list<std::string> result_selected_type;
    for (size_t i = 0; i < sub_type_count; i++) {
        result_selected_type.push_back(row_reader->getSelectedType().getFieldName(i));
    }
    ASSERT_EQ(expect_selected_type, result_selected_type);

    batch_size = 10;
    batch = row_reader->createRowBatch(batch_size);
    ASSERT_TRUE(row_reader->next(*batch));
    ASSERT_EQ(10, batch->numElements);

    struct_batch = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
    ASSERT_TRUE(struct_batch);
    int_batch = dynamic_cast<::orc::LongVectorBatch*>(struct_batch->fields[0]);
    ASSERT_TRUE(int_batch);
    double_batch = dynamic_cast<::orc::DoubleVectorBatch*>(struct_batch->fields[1]);
    ASSERT_TRUE(double_batch);
    string_batch = dynamic_cast<::orc::StringVectorBatch*>(struct_batch->fields[2]);
    ASSERT_TRUE(string_batch);

    for (size_t i = 0; i < batch_size; i++) {
        const auto& [ivalue, dvalue, svalue] = raw_data[i];
        ASSERT_EQ(ivalue, int_batch->data[i]);
        ASSERT_NEAR(dvalue, double_batch->data[i], 0.001);
        ASSERT_EQ(svalue, std::string(string_batch->data[i], string_batch->length[i]));
    }
}
}  // namespace paimon::orc::test
