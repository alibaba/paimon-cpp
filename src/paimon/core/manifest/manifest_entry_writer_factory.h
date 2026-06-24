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

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "arrow/c/abi.h"
#include "paimon/core/io/single_file_writer_factory.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_entry_writer.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class FileSystem;
class ManifestEntryWriter;
class MemoryPool;
class PathFactory;
class WriterBuilder;

class ManifestEntryWriterFactory
    : public SingleFileWriterFactory<const ManifestEntry&, ManifestFileMeta> {
 public:
    ManifestEntryWriterFactory(const std::string& compression,
                               std::function<Status(const ManifestEntry&, ::ArrowArray*)> converter,
                               const std::shared_ptr<MemoryPool>& pool,
                               const std::shared_ptr<arrow::Schema>& partition_type,
                               const std::shared_ptr<FileSystem>& file_system,
                               const std::shared_ptr<PathFactory>& path_factory,
                               const std::shared_ptr<WriterBuilder>& writer_builder)
        : compression_(compression),
          converter_(std::move(converter)),
          pool_(pool),
          partition_type_(partition_type),
          file_system_(file_system),
          path_factory_(path_factory),
          writer_builder_(writer_builder) {}

    Result<std::unique_ptr<SingleFileWriter<const ManifestEntry&, ManifestFileMeta>>> CreateWriter()
        const override {
        auto writer =
            std::make_unique<ManifestEntryWriter>(compression_, converter_, pool_, partition_type_);
        PAIMON_RETURN_NOT_OK(writer->Init(file_system_, path_factory_->NewPath(), writer_builder_));
        return std::unique_ptr<SingleFileWriter<const ManifestEntry&, ManifestFileMeta>>(
            std::move(writer));
    }

 private:
    std::string compression_;
    std::function<Status(const ManifestEntry&, ::ArrowArray*)> converter_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> partition_type_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<PathFactory> path_factory_;
    std::shared_ptr<WriterBuilder> writer_builder_;
};

}  // namespace paimon
