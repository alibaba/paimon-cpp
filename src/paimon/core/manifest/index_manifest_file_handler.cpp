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

#include "paimon/core/manifest/index_manifest_file_handler.h"

#include <set>
#include <utility>

#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
namespace paimon {
std::vector<IndexManifestEntry> IndexManifestFileHandler::GlobalFileNameCombiner::Combine(
    const std::vector<IndexManifestEntry>& prev_index_files,
    const std::vector<IndexManifestEntry>& new_index_files) const {
    // TODO(liancheng.lsz): support dv
    std::map<std::string, IndexManifestEntry> index_entries;
    for (const auto& entry : prev_index_files) {
        index_entries.emplace(entry.index_file->FileName(), entry);
    }

    std::vector<IndexManifestEntry> removed;
    removed.reserve(new_index_files.size());
    std::vector<IndexManifestEntry> added;
    added.reserve(new_index_files.size());

    for (const auto& entry : new_index_files) {
        if (entry.kind == FileKind::Delete()) {
            removed.push_back(entry);
        } else if (entry.kind == FileKind::Add()) {
            added.push_back(entry);
        }
    }

    // The deleted entry is processed first to avoid overwriting a new entry.
    for (const auto& entry : removed) {
        index_entries.erase(entry.index_file->FileName());
    }
    for (const auto& entry : added) {
        index_entries.emplace(entry.index_file->FileName(), entry);
    }

    std::vector<IndexManifestEntry> result_entries;
    result_entries.reserve(index_entries.size());
    for (const auto& [_, entry] : index_entries) {
        result_entries.push_back(entry);
    }
    return result_entries;
}

Result<std::string> IndexManifestFileHandler::Write(
    const std::optional<std::string>& previous_index_manifest,
    const std::vector<IndexManifestEntry>& new_index_entries,
    IndexManifestFile* index_manifest_file) {
    std::vector<IndexManifestEntry> entries;
    if (previous_index_manifest != std::nullopt) {
        PAIMON_RETURN_NOT_OK(index_manifest_file->Read(previous_index_manifest.value(),
                                                       /*filter=*/nullptr, &entries));
    }
    for (const auto& entry : entries) {
        if (!(entry.kind == FileKind::Add())) {
            return Status::Invalid("Invalid entry, file kind is not add.");
        }
    }
    std::map<std::string, std::vector<IndexManifestEntry>> previous = SeparateIndexEntries(entries);
    std::map<std::string, std::vector<IndexManifestEntry>> current =
        SeparateIndexEntries(new_index_entries);

    std::set<std::string> index_types;
    for (const auto& [index_type, _] : previous) {
        index_types.insert(index_type);
    }
    for (const auto& [index_type, _] : current) {
        index_types.insert(index_type);
    }

    std::vector<IndexManifestEntry> index_entries;
    index_entries.reserve(previous.size() + current.size());
    for (const auto& index_type : index_types) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<IndexManifestFileHandler::IndexManifestFileCombiner> combiner,
            GetIndexManifestFileCombine(index_type));
        std::vector<IndexManifestEntry> typed_previous_entries = previous[index_type];
        std::vector<IndexManifestEntry> typed_current_entries = current[index_type];
        std::vector<IndexManifestEntry> combined_entries =
            combiner->Combine(typed_previous_entries, typed_current_entries);

        index_entries.insert(index_entries.end(), combined_entries.begin(), combined_entries.end());
    }

    std::pair<std::string, int64_t> file_path_and_length;
    PAIMON_ASSIGN_OR_RAISE(file_path_and_length,
                           index_manifest_file->WriteWithoutRolling(index_entries));
    return file_path_and_length.first;
}

std::map<std::string, std::vector<IndexManifestEntry>>
IndexManifestFileHandler::SeparateIndexEntries(
    const std::vector<IndexManifestEntry>& index_entries) {
    std::map<std::string, std::vector<IndexManifestEntry>> result;
    for (const auto& index_entry : index_entries) {
        std::string index_type = index_entry.index_file->IndexType();
        result[index_type].push_back(index_entry);
    }
    return result;
}

Result<std::unique_ptr<IndexManifestFileHandler::IndexManifestFileCombiner>>
IndexManifestFileHandler::GetIndexManifestFileCombine(const std::string& index_type) {
    if (index_type != DeletionVectorsIndexFile::DELETION_VECTORS_INDEX && index_type != "HASH") {
        return std::make_unique<GlobalFileNameCombiner>();
    }
    return Status::NotImplemented(
        "Do not support handle dv index or hash index in commit process.");
}

}  // namespace paimon
