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

#include <cstdint>
#include <exception>
#include <iostream>

#include "benchmark/benchmark.h"
#include "benchmark/benchmark_suite.h"

int main(int argc, char** argv) {
    try {
        paimon::benchmark::ParsePaimonBenchmarkCliArgs(&argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "paimon-read-write-benchmark: " << e.what() << std::endl;
        std::cerr << "Try 'paimon-read-write-benchmark --help' for more information." << std::endl;
        return 1;
    }

    if (paimon::benchmark::HasHelpFlag(static_cast<int32_t>(argc), argv)) {
        paimon::benchmark::PrintPaimonBenchmarkCliHelp();
        return 0;
    }

    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
