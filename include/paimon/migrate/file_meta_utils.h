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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// Utility class for handling file metadata operations during data migration.
///
/// This class provides static utility functions for migrating external data files into Paimon
/// tables. It handles the generation of commit messages from source data files, enabling seamless
/// integration of existing data into Paimon's file store architecture.
///
/// @note This utility currently does not support:
/// - object store file systems
/// - primary-key tables
///
/// @warning This utility will move/rename source data files to destination paths during migration.
/// It is recommended to back up your data in advance before using this utility to avoid data loss.
class PAIMON_EXPORT FileMetaUtils {
 public:
    FileMetaUtils() = delete;
    ~FileMetaUtils() = delete;

    /// Generate a commit message for migrating external data files into a Paimon table.
    ///
    /// This method analyzes the provided source data files and generates a commit message that can
    /// be used to incorporate these files into the target Paimon table.
    ///
    /// @param src_data_files Vector of paths to source data files to be migrated.
    ///                       **These files must have the same schema as the target Paimon table**.
    /// @param dst_table_path Path to the destination Paimon table directory.
    /// @param partition_values Map of partition column names to their values for partitioned
    ///                         tables. Use empty map for non-partitioned tables.
    /// @param options Set a configuration options map to set some option entries which are not
    ///                defined in the table schema or whose values you want to overwrite.
    /// @param file_system  Specifies the file system for file operations.
    ///                     If `nullptr`, use default file system.
    /// @return Result containing a unique pointer to the generated `CommitMessage`,
    ///         or an error status if the migration cannot be performed.
    static Result<std::unique_ptr<CommitMessage>> GenerateCommitMessage(
        const std::vector<std::string>& src_data_files, const std::string& dst_table_path,
        const std::map<std::string, std::string>& partition_values,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system = nullptr);
};

}  // namespace paimon
