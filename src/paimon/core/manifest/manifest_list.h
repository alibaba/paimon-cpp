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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/objects_file.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class Cache;
class FileFormat;
class FileSystem;
class FileStorePathFactory;
class ManifestFileMeta;
class MemoryPool;
class PathFactory;
class ReaderBuilder;
class WriterBuilder;

/// This file includes several `ManifestFileMeta`, representing all data of the whole
/// table at the corresponding snapshot.
class ManifestList : public ObjectsFile<ManifestFileMeta> {
 public:
    static Result<std::unique_ptr<ManifestList>> Create(
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<FileFormat>& file_format, const std::string& compression,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<Cache>& cache, const std::shared_ptr<MemoryPool>& pool);

    /// Write several `ManifestFileMeta`s into a manifest list.
    ///
    /// @note This method is atomic.
    Result<std::pair<std::string, int64_t>> Write(const std::vector<ManifestFileMeta>& metas);

    /// Return all `ManifestFileMeta` instances for either data or changelog manifests in this
    /// snapshot.
    ///
    /// @param snapshot The input snapshot.
    /// @param manifests The vector to store the manifest file metadata.
    /// @return Status indicating whether the operation was successful or not.
    Status ReadAllManifests(const Snapshot& snapshot,
                            std::vector<ManifestFileMeta>* manifests) const {
        PAIMON_RETURN_NOT_OK(ReadDataManifests(snapshot, manifests));
        return ReadChangelogManifests(snapshot, manifests);
    }

    /// Return a `ManifestFileMeta` for each data manifest in this snapshot.
    ///
    /// @param snapshot The input snapshot.
    /// @param manifests The vector to store the manifest file metadata.
    /// @return Status indicating whether the operation was successful or not.
    Status ReadDataManifests(const Snapshot& snapshot,
                             std::vector<ManifestFileMeta>* manifests) const {
        PAIMON_RETURN_NOT_OK(ReadBaseManifests(snapshot, manifests));
        return ReadDeltaManifests(snapshot, manifests);
    }

    /// Return a `ManifestFileMeta` for each base manifest in this snapshot.
    ///
    /// @param snapshot The input snapshot.
    /// @param manifests The vector to store the manifest file metadata.
    /// @return Status indicating whether the operation was successful or not.
    Status ReadBaseManifests(const Snapshot& snapshot,
                             std::vector<ManifestFileMeta>* manifests) const {
        return Read(snapshot.BaseManifestList(), /*filter=*/nullptr, manifests);
    }

    /// Return a `ManifestFileMeta` for each delta manifest in this snapshot.
    ///
    /// @param snapshot The input snapshot.
    /// @param manifests The vector to store the manifest file metadata.
    /// @return Status indicating whether the operation was successful or not.
    Status ReadDeltaManifests(const Snapshot& snapshot,
                              std::vector<ManifestFileMeta>* manifests) const {
        return Read(snapshot.DeltaManifestList(), /*filter=*/nullptr, manifests);
    }

    /// Return a `ManifestFileMeta` for each changelog manifest in this snapshot.
    ///
    /// @param snapshot The input snapshot.
    /// @param manifests The vector to store the manifest file metadata.
    /// @return Status indicating whether the operation was successful or not.
    Status ReadChangelogManifests(const Snapshot& snapshot,
                                  std::vector<ManifestFileMeta>* manifests) const {
        const std::optional<std::string>& changelog_manifest_list =
            snapshot.ChangelogManifestList();
        if (changelog_manifest_list) {
            return Status::NotImplemented("do not support read changelog manifest list");
            // return Read(changelog_manifest_list.value(), /*filter=*/nullptr, manifests);
        } else {
            return Status::OK();
        }
    }

 private:
    ManifestList(const std::shared_ptr<FileSystem>& file_system,
                 const std::shared_ptr<ReaderBuilder>& reader_builder,
                 const std::shared_ptr<WriterBuilder>& writer_builder,
                 const std::string& compression, const std::shared_ptr<PathFactory>& path_factory,
                 const std::shared_ptr<Cache>& cache, const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
