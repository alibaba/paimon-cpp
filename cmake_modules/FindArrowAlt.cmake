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

set(_PAIMON_ARROW_ROOTS ${Arrow_ROOT} ${ARROW_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_ARROW_ROOTS "")
if(_PAIMON_ARROW_ROOTS)
    set(_PAIMON_ARROW_FIND_ARGS HINTS ${_PAIMON_ARROW_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(Arrow CONFIG QUIET ${_PAIMON_ARROW_FIND_ARGS})
find_package(Parquet CONFIG QUIET ${_PAIMON_ARROW_FIND_ARGS})
find_package(ArrowDataset CONFIG QUIET ${_PAIMON_ARROW_FIND_ARGS})
find_package(ArrowAcero CONFIG QUIET ${_PAIMON_ARROW_FIND_ARGS})

function(_paimon_select_first_target OUT_VAR)
    foreach(_target IN LISTS ARGN)
        if(TARGET ${_target})
            set(${OUT_VAR}
                ${_target}
                PARENT_SCOPE)
            return()
        endif()
    endforeach()
endfunction()

if(PAIMON_DEPENDENCY_USE_SHARED)
    _paimon_select_first_target(_PAIMON_ARROW_TARGET Arrow::arrow_shared Arrow::arrow)
    _paimon_select_first_target(_PAIMON_PARQUET_TARGET Parquet::parquet_shared
                                Parquet::parquet)
    _paimon_select_first_target(_PAIMON_ARROW_DATASET_TARGET
                                ArrowDataset::arrow_dataset_shared
                                Arrow::arrow_dataset_shared ArrowDataset::arrow_dataset)
    _paimon_select_first_target(_PAIMON_ARROW_ACERO_TARGET ArrowAcero::arrow_acero_shared
                                Arrow::arrow_acero_shared ArrowAcero::arrow_acero)
else()
    _paimon_select_first_target(_PAIMON_ARROW_TARGET Arrow::arrow_static Arrow::arrow)
    _paimon_select_first_target(_PAIMON_PARQUET_TARGET Parquet::parquet_static
                                Parquet::parquet)
    _paimon_select_first_target(_PAIMON_ARROW_DATASET_TARGET
                                ArrowDataset::arrow_dataset_static
                                Arrow::arrow_dataset_static ArrowDataset::arrow_dataset)
    _paimon_select_first_target(_PAIMON_ARROW_ACERO_TARGET ArrowAcero::arrow_acero_static
                                Arrow::arrow_acero_static ArrowAcero::arrow_acero)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    ArrowAlt REQUIRED_VARS _PAIMON_ARROW_TARGET _PAIMON_PARQUET_TARGET
                           _PAIMON_ARROW_DATASET_TARGET _PAIMON_ARROW_ACERO_TARGET)

if(ArrowAlt_FOUND)
    get_target_property(ARROW_INCLUDE_DIR ${_PAIMON_ARROW_TARGET}
                        INTERFACE_INCLUDE_DIRECTORIES)

    if(NOT TARGET arrow)
        add_library(arrow INTERFACE IMPORTED)
        if(ARROW_INCLUDE_DIR)
            set_target_properties(arrow PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                   "${ARROW_INCLUDE_DIR}")
        endif()
        target_link_libraries(arrow INTERFACE ${_PAIMON_ARROW_TARGET})
    endif()

    if(NOT TARGET arrow_acero)
        add_library(arrow_acero INTERFACE IMPORTED)
        target_link_libraries(arrow_acero INTERFACE ${_PAIMON_ARROW_ACERO_TARGET} arrow)
    endif()

    if(NOT TARGET arrow_dataset)
        add_library(arrow_dataset INTERFACE IMPORTED)
        target_link_libraries(arrow_dataset INTERFACE ${_PAIMON_ARROW_DATASET_TARGET}
                                                      arrow_acero)
    endif()

    if(NOT TARGET parquet)
        add_library(parquet INTERFACE IMPORTED)
        target_link_libraries(parquet INTERFACE ${_PAIMON_PARQUET_TARGET} arrow_dataset)
    endif()
endif()

unset(_PAIMON_ARROW_ACERO_TARGET)
unset(_PAIMON_ARROW_DATASET_TARGET)
unset(_PAIMON_ARROW_FIND_ARGS)
unset(_PAIMON_ARROW_ROOTS)
unset(_PAIMON_ARROW_TARGET)
unset(_PAIMON_PARQUET_TARGET)
