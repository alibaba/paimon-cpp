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
#include "paimon/global_index/lucene/lucene_directory.h"

#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/lucene/lucene_input.h"
#include "paimon/global_index/lucene/lucene_utils.h"

namespace paimon::lucene {
LuceneDirectory::LuceneDirectory(
    const std::string& path,
    const std::map<std::string, std::pair<int64_t, int64_t>>& file_name_to_offset_and_length,
    const std::shared_ptr<InputStream>& paimon_input)
    : LuceneDirectory::LuceneDirectory(path, file_name_to_offset_and_length, paimon_input,
                                       kDefaultReadBufferSize) {}

LuceneDirectory::LuceneDirectory(
    const std::string& path,
    const std::map<std::string, std::pair<int64_t, int64_t>>& file_name_to_offset_and_length,
    const std::shared_ptr<InputStream>& paimon_input, int32_t input_buffer_size)
    : Lucene::Directory(),
      input_buffer_size_(input_buffer_size),
      path_(path),
      file_name_to_offset_and_length_(file_name_to_offset_and_length),
      paimon_input_(paimon_input) {
    Lucene::Directory::setLockFactory(Lucene::NoLockFactory::getNoLockFactory());
}

Lucene::HashSet<Lucene::String> LuceneDirectory::listAll() {
    ensureOpen();
    Lucene::HashSet<Lucene::String> result_file_list(
        Lucene::HashSet<Lucene::String>::newInstance());
    for (const auto& [file_name, _] : file_name_to_offset_and_length_) {
        result_file_list.add(LuceneUtils::StringToWstring(file_name));
    }
    return result_file_list;
}

bool LuceneDirectory::fileExists(const Lucene::String& name) {
    ensureOpen();
    auto iter = file_name_to_offset_and_length_.find(LuceneUtils::WstringToString(name));
    return iter != file_name_to_offset_and_length_.end();
}

uint64_t LuceneDirectory::fileModified(const Lucene::String& name) {
    throw Lucene::IOException(L"LuceneDirectory not support fileModified()");
}

void LuceneDirectory::touchFile(const Lucene::String& name) {
    throw Lucene::IOException(L"LuceneDirectory not support touchFile()");
}

void LuceneDirectory::deleteFile(const Lucene::String& name) {
    throw Lucene::IOException(L"LuceneDirectory not support deleteFile()");
}

int64_t LuceneDirectory::fileLength(const Lucene::String& name) {
    ensureOpen();
    auto iter = file_name_to_offset_and_length_.find(LuceneUtils::WstringToString(name));
    if (iter == file_name_to_offset_and_length_.end()) {
        throw Lucene::IOException(L"file not exist in fileLength");
    }
    return iter->second.second;
}

Lucene::IndexOutputPtr LuceneDirectory::createOutput(const Lucene::String& name) {
    throw Lucene::IOException(L"LuceneDirectory not support createOutput()");
}

Lucene::IndexInputPtr LuceneDirectory::openInput(const Lucene::String& name) {
    ensureOpen();
    auto file_iter = file_name_to_offset_and_length_.find(LuceneUtils::WstringToString(name));
    if (file_iter == file_name_to_offset_and_length_.end()) {
        throw Lucene::IOException(L"file not exist in openInput");
    }
    const auto& [offset, length] = file_iter->second;
    auto offset_input_result = OffsetInputStream::Create(paimon_input_, length, offset);
    if (!offset_input_result.ok()) {
        throw Lucene::IOException(
            LuceneUtils::StringToWstring(offset_input_result.status().ToString()));
    }
    std::shared_ptr<InputStream> offset_input = std::move(offset_input_result).value();
    return Lucene::newLucene<LuceneIndexInput>(Lucene::newLucene<LuceneSyncInput>(offset_input),
                                               input_buffer_size_);
}

void LuceneDirectory::close() {
    Lucene::SyncLock sync_lock(this);
    isOpen = false;
}

}  // namespace paimon::lucene
