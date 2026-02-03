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
#pragma once

#include "lucene++/BufferedIndexInput.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/lucene/lucene_utils.h"

namespace paimon::lucene {
class LuceneSyncInput : public Lucene::LuceneObject {
 public:
    explicit LuceneSyncInput(const std::shared_ptr<InputStream>& in_stream) : in_(in_stream) {}
    const std::shared_ptr<InputStream>& GetInput() const {
        return in_;
    }

 private:
    std::shared_ptr<InputStream> in_;
};

class LuceneIndexInput : public Lucene::BufferedIndexInput {
 public:
    LuceneIndexInput(const boost::shared_ptr<LuceneSyncInput>& in_stream, int32_t buffer_size)
        : Lucene::BufferedIndexInput(buffer_size),
          input_buffer_size_(buffer_size),
          in_stream_(in_stream) {}

 public:
    int64_t length() override {
        auto result = in_stream_->GetInput()->Length();
        if (!result.ok()) {
            throw Lucene::IOException(LuceneUtils::StringToWstring(result.status().ToString()));
        }
        return static_cast<int64_t>(result.value());
    }
    void close() override {
        if (is_clone_) {
            return;
        }
        if (in_stream_) {
            in_stream_.reset();
        }
    }

 private:
    void readInternal(uint8_t* b, int32_t offset, int32_t length) override {
        Lucene::SyncLock lock(in_stream_);
        int64_t position = getFilePointer();
        auto read_result =
            in_stream_->GetInput()->Read(reinterpret_cast<char*>(b + offset), length, position);
        if (!read_result.ok()) {
            throw Lucene::IOException(
                LuceneUtils::StringToWstring(read_result.status().ToString()));
        }
        if (read_result.value() != length) {
            throw Lucene::IOException(L"actual read len and expect read len mismatch");
        }
    }
    void seekInternal(int64_t pos) override {}

    Lucene::LuceneObjectPtr clone(const Lucene::LuceneObjectPtr& other) override {
        Lucene::LuceneObjectPtr clone = Lucene::BufferedIndexInput::clone(
            other ? other : Lucene::newLucene<LuceneIndexInput>(in_stream_, input_buffer_size_));
        boost::shared_ptr<LuceneIndexInput> clone_index_input(
            boost::dynamic_pointer_cast<LuceneIndexInput>(clone));
        clone_index_input->in_stream_ = in_stream_;
        clone_index_input->is_clone_ = true;
        return clone_index_input;
    }

 private:
    bool is_clone_ = false;
    int32_t input_buffer_size_;
    boost::shared_ptr<LuceneSyncInput> in_stream_;
};
}  // namespace paimon::lucene
