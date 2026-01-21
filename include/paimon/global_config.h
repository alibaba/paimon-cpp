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

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// Get the capacity of the arrow's global thread pool
/// This is a simple wrapper of arrow::GetCpuThreadPoolCapacity()
///
/// Return the number of worker threads in the thread pool to which
/// Arrow dispatches various CPU-bound tasks.  This is an ideal number,
/// not necessarily the exact number of threads at a given point in time.
///
/// You can change this number using SetArrowCpuThreadPoolCapacity().
PAIMON_EXPORT int GetArrowCpuThreadPoolCapacity();

/// Set the capacity of the arrow's global thread pool
/// This is a simple wrapper of arrow::SetCpuThreadPoolCapacity()
///
/// Set the number of worker threads in the thread pool to which
/// Arrow dispatches various CPU-bound tasks.
///
/// The current number is returned by GetArrowCpuThreadPoolCapacity().
/// Currently, this capacity will significantly affect the performance
/// of parquet file batch read.
PAIMON_EXPORT Status SetArrowCpuThreadPoolCapacity(int threads);

}  // namespace paimon
