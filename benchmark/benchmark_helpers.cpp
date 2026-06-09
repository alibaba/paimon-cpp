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

#include "benchmark/benchmark_helpers.h"

#include <iostream>

#include "benchmark/benchmark.h"
#include "fmt/format.h"

namespace paimon::benchmark {

bool BenchmarkHelpers::ValidateFileFormatOrSkip(::benchmark::State& state,
                                                const std::string& file_format, bool is_supported,
                                                SkipFn skip) {
    if (!is_supported) {
        skip(state, fmt::format("file format is not supported in this build: {}", file_format));
        return false;
    }
    return true;
}

bool BenchmarkHelpers::ValidateSourcePresenceOrSkip(::benchmark::State& state,
                                                    const std::string& source_path,
                                                    const std::string& message, SkipFn skip) {
    if (source_path.empty()) {
        skip(state, message);
        return false;
    }
    return true;
}

bool BenchmarkHelpers::ValidateSourceSupportOrSkip(::benchmark::State& state,
                                                   const std::string& source_format,
                                                   bool is_supported, SkipFn skip) {
    if (!is_supported) {
        skip(state,
             fmt::format("source data mode requires reader support in this build for format: {}",
                         source_format));
        return false;
    }
    return true;
}

bool BenchmarkHelpers::ValidatePrefetchParallelOrSkip(::benchmark::State& state,
                                                      int32_t prefetch_parallel_num, SkipFn skip) {
    if (prefetch_parallel_num <= 0) {
        skip(state, "prefetch_parallel must be greater than 0");
        return false;
    }
    return true;
}

Result<int64_t> BenchmarkHelpers::RunReadIterations(::benchmark::State& state,
                                                    const ReadOnceFn& read_once) {
    int64_t rows_read = 0;
    for (auto _ : state) {
        PAIMON_ASSIGN_OR_RAISE(rows_read, read_once());
    }
    return rows_read;
}

Result<bool> BenchmarkHelpers::TryRunSourceTableReadMode(::benchmark::State& state,
                                                         const std::string& benchmark_name,
                                                         const std::string& source_table_path,
                                                         const ReadOnceFn& read_once) {
    if (source_table_path.empty()) {
        return false;
    }

    std::cout << "[benchmark][" << benchmark_name << "] source_table_path=" << source_table_path
              << std::endl;
    PAIMON_ASSIGN_OR_RAISE(const int64_t rows_read, RunReadIterations(state, read_once));
    state.SetItemsProcessed(state.iterations() * rows_read);
    return true;
}

}  // namespace paimon::benchmark
