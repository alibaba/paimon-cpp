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
#include "lucene++/LuceneHeaders.h"
#include "lucene++/NoLockFactory.h"
#include "paimon/fs/file_system.h"

namespace paimon::lucene {
/// This class wraps a Paimon FileSystem instance to enable Lucene index reading capabilities,
/// but only supports read operations (e.g., openInput, fileLength). Write operations
/// (e.g., createOutput, delete) will throw an exception or be unsupported.
class LuceneDirectory : public Lucene::Directory {
 public:
    LuceneDirectory(
        const std::string& path,
        const std::map<std::string, std::pair<int64_t, int64_t>>& file_name_to_offset_and_length,
        const std::shared_ptr<InputStream>& paimon_input);

    LuceneDirectory(
        const std::string& path,
        const std::map<std::string, std::pair<int64_t, int64_t>>& file_name_to_offset_and_length,
        const std::shared_ptr<InputStream>& paimon_input, int32_t input_buffer_size);

    Lucene::HashSet<Lucene::String> listAll() override;

    bool fileExists(const Lucene::String& name) override;

    uint64_t fileModified(const Lucene::String& name) override;

    void touchFile(const Lucene::String& name) override;

    void deleteFile(const Lucene::String& name) override;

    int64_t fileLength(const Lucene::String& name) override;

    Lucene::IndexOutputPtr createOutput(const Lucene::String& name) override;

    Lucene::IndexInputPtr openInput(const Lucene::String& name) override;

    void close() override;

 private:
    int32_t input_buffer_size_;
    std::string path_;
    /// @note All files are concatenated into a single physical file for the Paimon global index.
    ///       Use `file_name_to_offset_and_length_` and `paimon_input_` to obtain the actual
    ///       offset and length of each logical file within the merged file, which are used
    ///       to create Lucene index inputs for `Lucene::Directory`.
    std::map<std::string, std::pair<int64_t, int64_t>> file_name_to_offset_and_length_;
    std::shared_ptr<InputStream> paimon_input_;
};
}  // namespace paimon::lucene
