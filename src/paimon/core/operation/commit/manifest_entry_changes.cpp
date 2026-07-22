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

#include "paimon/core/operation/commit/manifest_entry_changes.h"

#include <unordered_set>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"

namespace paimon {

ManifestEntryChanges::ManifestEntryChanges(int32_t default_num_bucket)
    : default_num_bucket_(default_num_bucket) {}

Status ManifestEntryChanges::Collect(const std::shared_ptr<CommitMessage>& message) {
    auto commit_message = std::dynamic_pointer_cast<CommitMessageImpl>(message);
    if (!commit_message) {
        return Status::Invalid("fail to cast commit message to commit message impl");
    }

    DataIncrement new_files_increment = commit_message->GetNewFilesIncrement();
    for (const std::shared_ptr<DataFileMeta>& file : new_files_increment.NewFiles()) {
        append_table_files.push_back(MakeEntry(FileKind::Add(), commit_message, file));
    }
    for (const std::shared_ptr<DataFileMeta>& file : new_files_increment.DeletedFiles()) {
        append_table_files.push_back(MakeEntry(FileKind::Delete(), commit_message, file));
    }
    for (const std::shared_ptr<DataFileMeta>& file : new_files_increment.ChangelogFiles()) {
        append_changelog.push_back(MakeEntry(FileKind::Add(), commit_message, file));
    }
    for (const std::shared_ptr<IndexFileMeta>& file : new_files_increment.DeletedIndexFiles()) {
        append_index_files.emplace_back(FileKind::Delete(), commit_message->Partition(),
                                        commit_message->Bucket(), file);
    }
    for (const std::shared_ptr<IndexFileMeta>& file : new_files_increment.NewIndexFiles()) {
        append_index_files.emplace_back(FileKind::Add(), commit_message->Partition(),
                                        commit_message->Bucket(), file);
    }

    CompactIncrement compact_increment = commit_message->GetCompactIncrement();
    for (const std::shared_ptr<DataFileMeta>& file : compact_increment.CompactBefore()) {
        compact_table_files.push_back(MakeEntry(FileKind::Delete(), commit_message, file));
    }
    for (const std::shared_ptr<DataFileMeta>& file : compact_increment.CompactAfter()) {
        compact_table_files.push_back(MakeEntry(FileKind::Add(), commit_message, file));
    }
    for (const std::shared_ptr<DataFileMeta>& file : compact_increment.ChangelogFiles()) {
        compact_changelog.push_back(MakeEntry(FileKind::Add(), commit_message, file));
    }
    for (const std::shared_ptr<IndexFileMeta>& file : compact_increment.DeletedIndexFiles()) {
        compact_index_files.emplace_back(FileKind::Delete(), commit_message->Partition(),
                                         commit_message->Bucket(), file);
    }
    for (const std::shared_ptr<IndexFileMeta>& file : compact_increment.NewIndexFiles()) {
        compact_index_files.emplace_back(FileKind::Add(), commit_message->Partition(),
                                         commit_message->Bucket(), file);
    }

    return Status::OK();
}

bool ManifestEntryChanges::HasAppendChanges() const {
    return !append_table_files.empty() || !append_changelog.empty() || !append_index_files.empty();
}

bool ManifestEntryChanges::HasGlobalIndexFileAdditions() const {
    for (const IndexManifestEntry& index_entry : append_index_files) {
        if (index_entry.kind == FileKind::Add() && index_entry.index_file->GetGlobalIndexMeta()) {
            return true;
        }
    }
    return false;
}

bool ManifestEntryChanges::HasCompactChanges() const {
    return !compact_table_files.empty() || !compact_changelog.empty() ||
           !compact_index_files.empty();
}

std::string ManifestEntryChanges::ToString() const {
    std::vector<std::string> parts;
    if (!append_table_files.empty()) {
        parts.push_back(fmt::format("{} append table files", append_table_files.size()));
    }
    if (!append_changelog.empty()) {
        parts.push_back(fmt::format("{} append Changelogs", append_changelog.size()));
    }
    if (!append_index_files.empty()) {
        parts.push_back(fmt::format("{} append index files", append_index_files.size()));
    }
    if (!compact_table_files.empty()) {
        parts.push_back(fmt::format("{} compact table files", compact_table_files.size()));
    }
    if (!compact_changelog.empty()) {
        parts.push_back(fmt::format("{} compact Changelogs", compact_changelog.size()));
    }
    if (!compact_index_files.empty()) {
        parts.push_back(fmt::format("{} compact index files", compact_index_files.size()));
    }
    return fmt::format("{}", fmt::join(parts, ", "));
}

std::vector<BinaryRow> ManifestEntryChanges::ChangedPartitions(
    const std::vector<ManifestEntry>& data_file_changes,
    const std::vector<IndexManifestEntry>& index_file_changes) {
    std::unordered_set<BinaryRow> changed_partitions;
    for (const ManifestEntry& file : data_file_changes) {
        changed_partitions.insert(file.Partition());
    }
    for (const IndexManifestEntry& file : index_file_changes) {
        if (file.index_file->IndexType() == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX ||
            file.index_file->GetGlobalIndexMeta()) {
            changed_partitions.insert(file.partition);
        }
    }
    return std::vector<BinaryRow>(changed_partitions.begin(), changed_partitions.end());
}

ManifestEntry ManifestEntryChanges::MakeEntry(
    const FileKind& kind, const std::shared_ptr<CommitMessageImpl>& commit_message,
    const std::shared_ptr<DataFileMeta>& file) const {
    int32_t total_buckets = commit_message->TotalBuckets() == std::nullopt
                                ? default_num_bucket_
                                : commit_message->TotalBuckets().value();
    return ManifestEntry(kind, commit_message->Partition(), commit_message->Bucket(), total_buckets,
                         file);
}

}  // namespace paimon
