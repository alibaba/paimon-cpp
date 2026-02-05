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
#include "arrow/type.h"
#include "cppjieba/Jieba.hpp"
#include "lucene++/LuceneHeaders.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/global_index/lucene/lucene_defs.h"

namespace paimon::lucene {
class LuceneGlobalIndexWriter : public GlobalIndexWriter {
 public:
    struct LuceneWriteContext {
        LuceneWriteContext(const std::string& _tmp_index_path,
                           const Lucene::FSDirectoryPtr& _lucene_dir,
                           const Lucene::IndexWriterPtr& _index_writer,
                           const Lucene::DocumentPtr& _doc, const Lucene::FieldPtr& _field,
                           const Lucene::FieldPtr& _row_id_field);

        LuceneWriteContext(LuceneWriteContext&&) = default;
        LuceneWriteContext& operator=(LuceneWriteContext&&) = default;

        std::string tmp_index_path;
        Lucene::FSDirectoryPtr lucene_dir;
        Lucene::IndexWriterPtr index_writer;
        Lucene::DocumentPtr doc;
        Lucene::FieldPtr field;
        Lucene::FieldPtr row_id_field;
    };

    static Result<std::shared_ptr<LuceneGlobalIndexWriter>> Create(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    ~LuceneGlobalIndexWriter() override;

    Status AddBatch(::ArrowArray* c_arrow_array) override;

    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    LuceneGlobalIndexWriter(const std::string& field_name,
                            const std::shared_ptr<arrow::DataType>& arrow_type,
                            LuceneWriteContext&& write_context,
                            const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
                            const std::map<std::string, std::string>& options,
                            const std::shared_ptr<MemoryPool>& pool);

    Result<std::string> FlushIndexToFinal() const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t row_id_ = 0;
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    LuceneWriteContext write_context_;
    std::shared_ptr<GlobalIndexFileWriter> file_writer_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::lucene
