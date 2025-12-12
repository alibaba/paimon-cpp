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

#include <filesystem>
#include <iostream>
#include <map>
#include <set>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "paimon/api.h"
#include "paimon/catalog/catalog.h"
#include "paimon/orphan_files_cleaner.h"

namespace fs = std::filesystem;
namespace paimon {
Status CleanOrphanFiles(const std::string& table_path, int64_t older_than_ms) {
    CleanContextBuilder clean_context_builder(table_path);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CleanContext> clean_context,
                           clean_context_builder.WithOlderThanMs(older_than_ms).Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OrphanFilesCleaner> orphan_files_cleaner,
                           OrphanFilesCleaner::Create(std::move(clean_context)));
    PAIMON_ASSIGN_OR_RAISE(std::set<std::string> cleaned_paths, orphan_files_cleaner->Clean());

    for (const auto& clean_file : cleaned_paths) {
        std::cout << "clean_file_path : " << clean_file << std::endl;
    }

    return Status::OK();
}

Status DropPartition(const std::string& table_path,
                     const std::vector<std::map<std::string, std::string>>& partitions) {
    CommitContextBuilder commit_context_builder(table_path, /*commit_user=*/"commit_user_1");
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                           commit_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> committer,
                           FileStoreCommit::Create(std::move(commit_context)));
    PAIMON_RETURN_NOT_OK(committer->DropPartition(partitions, /*commit_identifier=*/10));

    return Status::OK();
}

Status ExpireSnapshot(const std::string& table_path) {
    CommitContextBuilder commit_context_builder(table_path, /*commit_user=*/"commit_user_1");
    std::map<std::string, std::string> commit_options = {
        {Options::SNAPSHOT_NUM_RETAINED_MAX, "2"},
        {Options::SNAPSHOT_NUM_RETAINED_MIN, "1"},
        {Options::SNAPSHOT_TIME_RETAINED, "1ms"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"}};
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                           commit_context_builder.SetOptions(commit_options).Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> committer,
                           FileStoreCommit::Create(std::move(commit_context)));
    PAIMON_RETURN_NOT_OK(committer->Expire());

    return Status::OK();
}

}  // namespace paimon

bool CopyToTempDirectory(const fs::path& src, const fs::path& dst) {
    try {
        if (!fs::exists(dst)) {
            fs::create_directories(dst);
        }
        for (const auto& entry : fs::recursive_directory_iterator(src)) {
            const auto& relativePath = fs::relative(entry.path(), src);
            const auto& targetPath = dst / relativePath;
            if (entry.is_directory()) {
                fs::create_directories(targetPath);
            } else {
                fs::copy_file(entry.path(), targetPath, fs::copy_options::overwrite_existing);
            }
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "filesystem error: " << e.what() << std::endl;
        return false;
    }
    return true;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " <origin_table_path> <temp_table_path> <clean_mode>"
                  << std::endl;
        return -1;
    }
    std::string origin_table_path = std::string(argv[1]);
    std::string temp_table_path = std::string(argv[2]);
    std::string clean_mode = std::string(argv[3]);

    if (!CopyToTempDirectory(origin_table_path, temp_table_path)) {
        return -1;
    }

    std::map<std::string, std::string> clean_options;
    paimon::Status status;
    if (clean_mode == "orphan_file") {
        std::cout << "enter the timestamp (ms) before which orphan files will be deleted"
                  << std::endl;
        int64_t older_than_ms;
        std::cin >> older_than_ms;
        status = paimon::CleanOrphanFiles(temp_table_path, older_than_ms);
    } else if (clean_mode == "drop_partition") {
        std::cout << "enter partition key-value pairs to drop. type 'EOF EOF' to finish"
                  << std::endl;
        std::string partition_key, value;
        std::vector<std::map<std::string, std::string>> partitions;
        while (std::cin >> partition_key >> value) {
            if (partition_key == "EOF" && value == "EOF") break;
            partitions.push_back({{partition_key, value}});
        }
        status = paimon::DropPartition(temp_table_path, partitions);
    } else if (clean_mode == "expire_snapshot") {
        status = paimon::ExpireSnapshot(temp_table_path);
    }

    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return -1;
    }

    return 0;
}
