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

#include <memory>
#include <vector>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/sorted_run.h"

namespace paimon {

/// Algorithm to partition several data files into the minimum number of `SortedRun`s.
class IntervalPartition {
 public:
    IntervalPartition(const std::vector<std::shared_ptr<DataFileMeta>>& input_files,
                      const std::shared_ptr<FieldsComparator>& key_comparator);

    /// Returns a two-dimensional list of `SortedRun`s.
    ///
    /// The elements of the outer list are sections. Key intervals between sections do not
    /// overlap. This extra layer is to minimize the number of `SortedRun`s dealt at the same
    /// time.
    ///
    /// The elements of the inner list are `SortedRun`s within a section.
    ///
    /// Users are expected to use the results by this way:
    ///
    /// @code
    /// for (List<SortedRun> section : algorithm.partition()) {
    ///     // do some merge sorting within section
    /// }
    /// @endcode
    std::vector<std::vector<SortedRun>> Partition() const;

 private:
    std::vector<SortedRun> Partition(const std::vector<std::shared_ptr<DataFileMeta>>& metas) const;

 private:
    std::vector<std::shared_ptr<DataFileMeta>> files_;
    std::shared_ptr<FieldsComparator> key_comparator_;
};

}  // namespace paimon
