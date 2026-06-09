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

#include <cstdint>
#include <functional>
#include <string>

#include "paimon/result.h"

namespace benchmark {
class State;
}

namespace paimon::benchmark {

class BenchmarkHelpers {
 public:
    using ReadOnceFn = std::function<Result<int64_t>()>;
    using SkipFn = void (*)(::benchmark::State&, const std::string&);

    static bool ValidateFileFormatOrSkip(::benchmark::State& state, const std::string& file_format,
                                         bool is_supported, SkipFn skip);

    static bool ValidateSourcePresenceOrSkip(::benchmark::State& state,
                                             const std::string& source_path,
                                             const std::string& message, SkipFn skip);

    static bool ValidateSourceSupportOrSkip(::benchmark::State& state,
                                            const std::string& source_format, bool is_supported,
                                            SkipFn skip);

    static bool ValidatePrefetchParallelOrSkip(::benchmark::State& state,
                                               int32_t prefetch_parallel_num, SkipFn skip);

    static Result<int64_t> RunReadIterations(::benchmark::State& state,
                                             const ReadOnceFn& read_once);

    static Result<bool> TryRunSourceTableReadMode(::benchmark::State& state,
                                                  const std::string& benchmark_name,
                                                  const std::string& source_table_path,
                                                  const ReadOnceFn& read_once);
};

}  // namespace paimon::benchmark
