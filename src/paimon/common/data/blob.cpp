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

#include "paimon/data/blob.h"

#include <memory>
#include <utility>

#include "arrow/c/bridge.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/stream_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {

class MemoryPool;

class Blob::Impl {
 public:
    Impl(std::unique_ptr<BlobDescriptor>&& descriptor, const std::string& uri)
        : descriptor_(std::move(descriptor)), uri_(uri) {}

    PAIMON_UNIQUE_PTR<Bytes> SerializeDescriptor(const std::shared_ptr<MemoryPool>& pool) const {
        return descriptor_->Serialize(pool);
    }

    const BlobDescriptor* GetDescriptor() const {
        return descriptor_.get();
    }

    const std::string& Uri() const {
        return uri_;
    }

 private:
    std::unique_ptr<BlobDescriptor> descriptor_;
    std::string uri_;
};

Result<std::unique_ptr<Blob>> Blob::FromPath(const std::string& path) {
    return FromPath(path, /*offset=*/0, /*length=*/-1);
}

Result<std::unique_ptr<Blob>> Blob::FromPath(const std::string& path, int64_t offset,
                                             int64_t length) {
    PAIMON_ASSIGN_OR_RAISE(std::string normalized_path, PathUtil::NormalizePath(path));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobDescriptor> descriptor,
                           BlobDescriptor::Create(normalized_path, offset, length));
    auto impl = std::make_unique<Blob::Impl>(std::move(descriptor), descriptor->Uri());
    return std::unique_ptr<Blob>(new Blob(std::move(impl)));
}

Blob::Blob(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}
Blob::~Blob() = default;

Result<std::unique_ptr<Blob>> Blob::FromDescriptor(const char* buffer, uint64_t length) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobDescriptor> descriptor,
                           BlobDescriptor::Deserialize(buffer, length));

    auto impl = std::make_unique<Impl>(std::move(descriptor), descriptor->Uri());
    return std::unique_ptr<Blob>(new Blob(std::move(impl)));
}

PAIMON_UNIQUE_PTR<Bytes> Blob::ToDescriptor(const std::shared_ptr<MemoryPool>& pool) const {
    return impl_->SerializeDescriptor(pool);
}

const std::string& Blob::Uri() const {
    return impl_->Uri();
}

Result<std::unique_ptr<InputStream>> Blob::NewInputStream(
    const std::shared_ptr<FileSystem>& fs) const {
    if (fs == nullptr) {
        return Status::Invalid("file system is nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> file,
                           fs->Open(impl_->GetDescriptor()->Uri()));

    return OffsetInputStream::Create(std::move(file), impl_->GetDescriptor()->Length(),
                                     impl_->GetDescriptor()->Offset());
}

Result<PAIMON_UNIQUE_PTR<Bytes>> Blob::ToData(const std::shared_ptr<FileSystem>& fs,
                                              const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input_stream, NewInputStream(fs));
    return StreamUtils::ReadAsyncFully(std::move(input_stream), pool);
}

Result<std::unique_ptr<ArrowSchema>> Blob::ArrowField(
    const std::string& field_name, std::unordered_map<std::string, std::string> metadata) {
    auto blob_field = BlobUtils::ToArrowField(field_name, /*nullable=*/false, metadata);
    auto field = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportField(*blob_field, field.get()));
    return field;
}

}  // namespace paimon
