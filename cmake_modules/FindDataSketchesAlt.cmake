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

# Headers are located directly: the installed config exports unnamespaced targets like `hll`.

set(_PAIMON_DATASKETCHES_ROOTS ${DataSketches_ROOT} ${DATASKETCHES_ROOT}
                               ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_DATASKETCHES_ROOTS "")
if(_PAIMON_DATASKETCHES_ROOTS)
    set(_PAIMON_DATASKETCHES_FIND_ARGS HINTS ${_PAIMON_DATASKETCHES_ROOTS}
                                       NO_DEFAULT_PATH)
endif()

find_path(DATASKETCHES_INCLUDE_DIR
          NAMES DataSketches/hll.hpp ${_PAIMON_DATASKETCHES_FIND_ARGS}
          PATH_SUFFIXES include)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(DataSketchesAlt REQUIRED_VARS DATASKETCHES_INCLUDE_DIR)

if(DataSketchesAlt_FOUND AND NOT TARGET DataSketches)
    add_library(DataSketches INTERFACE IMPORTED)
    target_include_directories(DataSketches SYSTEM
                               INTERFACE "${DATASKETCHES_INCLUDE_DIR}")
endif()

unset(_PAIMON_DATASKETCHES_FIND_ARGS)
unset(_PAIMON_DATASKETCHES_ROOTS)
