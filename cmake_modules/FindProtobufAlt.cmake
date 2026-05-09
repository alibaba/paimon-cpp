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

set(_PAIMON_PROTOBUF_ROOTS ${Protobuf_ROOT} ${PROTOBUF_ROOT} ${PAIMON_PACKAGE_PREFIX})
list(REMOVE_ITEM _PAIMON_PROTOBUF_ROOTS "")
if(_PAIMON_PROTOBUF_ROOTS)
    set(_PAIMON_PROTOBUF_FIND_ARGS HINTS ${_PAIMON_PROTOBUF_ROOTS} NO_DEFAULT_PATH)
endif()

find_package(Protobuf CONFIG QUIET ${_PAIMON_PROTOBUF_FIND_ARGS})

set(_PAIMON_PROTOBUF_LIBRARY_TARGETS protobuf::libprotobuf Protobuf::libprotobuf)
foreach(_target IN LISTS _PAIMON_PROTOBUF_LIBRARY_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_PROTOBUF_LIBRARY_TARGET ${_target})
        break()
    endif()
endforeach()

set(_PAIMON_PROTOC_LIBRARY_TARGETS protobuf::libprotoc Protobuf::libprotoc)
foreach(_target IN LISTS _PAIMON_PROTOC_LIBRARY_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_PROTOC_LIBRARY_TARGET ${_target})
        break()
    endif()
endforeach()

set(_PAIMON_PROTOC_TARGETS protobuf::protoc Protobuf::protoc)
foreach(_target IN LISTS _PAIMON_PROTOC_TARGETS)
    if(TARGET ${_target})
        set(_PAIMON_PROTOC_TARGET ${_target})
        get_target_property(PROTOBUF_COMPILER ${_PAIMON_PROTOC_TARGET} IMPORTED_LOCATION)
        break()
    endif()
endforeach()

if(_PAIMON_PROTOBUF_LIBRARY_TARGET AND NOT PROTOBUF_COMPILER)
    find_program(PROTOBUF_COMPILER
                 NAMES protoc ${_PAIMON_PROTOBUF_FIND_ARGS}
                 PATH_SUFFIXES bin)
endif()

if(_PAIMON_PROTOBUF_LIBRARY_TARGET)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(
        ProtobufAlt REQUIRED_VARS _PAIMON_PROTOBUF_LIBRARY_TARGET PROTOBUF_COMPILER)
    if(ProtobufAlt_FOUND)
        get_target_property(PROTOBUF_INCLUDE_DIR ${_PAIMON_PROTOBUF_LIBRARY_TARGET}
                            INTERFACE_INCLUDE_DIRECTORIES)

        if(NOT TARGET libprotobuf)
            add_library(libprotobuf INTERFACE IMPORTED)
            if(PROTOBUF_INCLUDE_DIR)
                set_target_properties(libprotobuf PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                             "${PROTOBUF_INCLUDE_DIR}")
            endif()
            target_link_libraries(libprotobuf
                                  INTERFACE ${_PAIMON_PROTOBUF_LIBRARY_TARGET})
        endif()

        if(_PAIMON_PROTOC_LIBRARY_TARGET AND NOT TARGET libprotoc)
            add_library(libprotoc INTERFACE IMPORTED)
            if(PROTOBUF_INCLUDE_DIR)
                set_target_properties(libprotoc PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                           "${PROTOBUF_INCLUDE_DIR}")
            endif()
            target_link_libraries(libprotoc INTERFACE ${_PAIMON_PROTOC_LIBRARY_TARGET})
        endif()

        if(NOT TARGET protoc)
            add_executable(protoc IMPORTED)
            set_target_properties(protoc PROPERTIES IMPORTED_LOCATION
                                                    "${PROTOBUF_COMPILER}")
        endif()

        set(PROTOBUF_LIBRARIES ${_PAIMON_PROTOBUF_LIBRARY_TARGET})
    endif()
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PC_Protobuf QUIET protobuf)
    endif()

    find_path(PROTOBUF_INCLUDE_DIR
              NAMES google/protobuf/message.h ${_PAIMON_PROTOBUF_FIND_ARGS}
              HINTS ${PC_Protobuf_INCLUDE_DIRS}
              PATH_SUFFIXES include)
    find_library(PROTOBUF_LIBRARY
                 NAMES protobuf ${_PAIMON_PROTOBUF_FIND_ARGS}
                 HINTS ${PC_Protobuf_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)
    find_library(PROTOC_LIBRARY
                 NAMES protoc ${_PAIMON_PROTOBUF_FIND_ARGS}
                 HINTS ${PC_Protobuf_LIBRARY_DIRS}
                 PATH_SUFFIXES lib lib64)
    find_program(PROTOBUF_COMPILER
                 NAMES protoc ${_PAIMON_PROTOBUF_FIND_ARGS}
                 PATH_SUFFIXES bin)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(
        ProtobufAlt REQUIRED_VARS PROTOBUF_LIBRARY PROTOBUF_INCLUDE_DIR PROTOBUF_COMPILER)

    if(ProtobufAlt_FOUND)
        if(NOT TARGET libprotobuf)
            add_library(libprotobuf UNKNOWN IMPORTED)
            set_target_properties(libprotobuf
                                  PROPERTIES IMPORTED_LOCATION "${PROTOBUF_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${PROTOBUF_INCLUDE_DIR}")
            if(TARGET zlib)
                target_link_libraries(libprotobuf INTERFACE zlib)
            endif()
        endif()

        if(PROTOC_LIBRARY AND NOT TARGET libprotoc)
            add_library(libprotoc UNKNOWN IMPORTED)
            set_target_properties(libprotoc
                                  PROPERTIES IMPORTED_LOCATION "${PROTOC_LIBRARY}"
                                             INTERFACE_INCLUDE_DIRECTORIES
                                             "${PROTOBUF_INCLUDE_DIR}")
            target_link_libraries(libprotoc INTERFACE libprotobuf)
        endif()

        if(NOT TARGET protoc)
            add_executable(protoc IMPORTED)
            set_target_properties(protoc PROPERTIES IMPORTED_LOCATION
                                                    "${PROTOBUF_COMPILER}")
        endif()

        set(PROTOBUF_LIBRARIES "${PROTOBUF_LIBRARY}")
    endif()
endif()

unset(_PAIMON_PROTOBUF_FIND_ARGS)
unset(_PAIMON_PROTOBUF_LIBRARY_TARGET)
unset(_PAIMON_PROTOBUF_LIBRARY_TARGETS)
unset(_PAIMON_PROTOBUF_ROOTS)
unset(_PAIMON_PROTOC_LIBRARY_TARGET)
unset(_PAIMON_PROTOC_LIBRARY_TARGETS)
unset(_PAIMON_PROTOC_TARGET)
unset(_PAIMON_PROTOC_TARGETS)
