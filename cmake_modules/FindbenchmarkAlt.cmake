# Copyright 2026-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(_PAIMON_BENCHMARK_ROOTS ${benchmark_ROOT} ${BENCHMARK_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_BENCHMARK_ROOTS "")
if(_PAIMON_BENCHMARK_ROOTS)
    set(_PAIMON_BENCHMARK_FIND_ARGS HINTS ${_PAIMON_BENCHMARK_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(benchmark CONFIG QUIET ${_PAIMON_BENCHMARK_FIND_ARGS})

if(NOT TARGET benchmark::benchmark)
    find_path(BENCHMARK_INCLUDE_DIR
              NAMES benchmark/benchmark.h ${_PAIMON_BENCHMARK_FIND_ARGS}
              PATH_SUFFIXES include)
    find_library(BENCHMARK_LIBRARY
                 NAMES benchmark ${_PAIMON_BENCHMARK_FIND_ARGS}
                 PATH_SUFFIXES lib lib64)
    find_library(BENCHMARK_MAIN_LIBRARY
                 NAMES benchmark_main ${_PAIMON_BENCHMARK_FIND_ARGS}
                 PATH_SUFFIXES lib lib64)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(benchmarkAlt REQUIRED_VARS BENCHMARK_INCLUDE_DIR
                                                                 BENCHMARK_LIBRARY)

    if(benchmarkAlt_FOUND)
        if(NOT TARGET benchmark::benchmark)
            add_library(benchmark::benchmark UNKNOWN IMPORTED)
            set_target_properties(benchmark::benchmark
                                  PROPERTIES IMPORTED_LOCATION "${BENCHMARK_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${BENCHMARK_INCLUDE_DIR}")
        endif()

        if(BENCHMARK_MAIN_LIBRARY AND NOT TARGET benchmark::benchmark_main)
            add_library(benchmark::benchmark_main UNKNOWN IMPORTED)
            set_target_properties(benchmark::benchmark_main
                                  PROPERTIES IMPORTED_LOCATION "${BENCHMARK_MAIN_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${BENCHMARK_INCLUDE_DIR}")
        endif()
    endif()
else()
    set(benchmarkAlt_FOUND TRUE)
endif()

unset(_PAIMON_BENCHMARK_ROOTS)
unset(_PAIMON_BENCHMARK_FIND_ARGS)
