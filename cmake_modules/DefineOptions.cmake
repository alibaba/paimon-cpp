# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Borrowed the file from Apache Arrow:
# https://github.com/apache/arrow/blob/main/cpp/cmake_modules/DefineOptions.cmake

macro(set_option_category name)
    set(PAIMON_OPTION_CATEGORY ${name})
    list(APPEND "PAIMON_OPTION_CATEGORIES" ${name})
endmacro()

function(check_description_length name description)
    foreach(description_line ${description})
        string(LENGTH ${description_line} line_length)
        if(${line_length} GREATER 80)
            message(FATAL_ERROR "description for ${name} contained a\n\
        line ${line_length} characters long!\n\
        (max is 80). Split it into more lines with semicolons")
        endif()
    endforeach()
endfunction()

function(list_join lst glue out)
    if("${${lst}}" STREQUAL "")
        set(${out}
            ""
            PARENT_SCOPE)
        return()
    endif()

    list(GET ${lst} 0 joined)
    list(REMOVE_AT ${lst} 0)
    foreach(item ${${lst}})
        set(joined "${joined}${glue}${item}")
    endforeach()
    set(${out}
        ${joined}
        PARENT_SCOPE)
endfunction()

macro(define_option name description default)
    check_description_length(${name} ${description})
    list_join(description "\n" multiline_description)

    option(${name} "${multiline_description}" ${default})

    list(APPEND "PAIMON_${PAIMON_OPTION_CATEGORY}_OPTION_NAMES" ${name})
    set("${name}_OPTION_DESCRIPTION" ${description})
    set("${name}_OPTION_DEFAULT" ${default})
    set("${name}_OPTION_TYPE" "bool")
endmacro()

macro(define_option_string name description default)
    check_description_length(${name} ${description})
    list_join(description "\n" multiline_description)

    set(${name}
        ${default}
        CACHE STRING "${multiline_description}")

    list(APPEND "PAIMON_${PAIMON_OPTION_CATEGORY}_OPTION_NAMES" ${name})
    set("${name}_OPTION_DESCRIPTION" ${description})
    set("${name}_OPTION_DEFAULT" "\"${default}\"")
    set("${name}_OPTION_TYPE" "string")
    set("${name}_OPTION_POSSIBLE_VALUES" ${ARGN})

    list_join("${name}_OPTION_POSSIBLE_VALUES" "|" "${name}_OPTION_ENUM")
    if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
        set_property(CACHE ${name} PROPERTY STRINGS "${name}_OPTION_POSSIBLE_VALUES")
    endif()
endmacro()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
    #----------------------------------------------------------------------
    set_option_category("Compile and link")

    define_option_string(PAIMON_CXXFLAGS "Compiler flags to append when compiling Paimon"
                         "")

    define_option(PAIMON_BUILD_STATIC "Build static libraries" ON)

    define_option(PAIMON_BUILD_SHARED "Build shared libraries" ON)
    #----------------------------------------------------------------------
    set_option_category("Test")

    define_option(PAIMON_BUILD_TESTS "Build the Paimon googletest unit tests" OFF)

    if(PAIMON_BUILD_SHARED)
        set(PAIMON_TEST_LINKAGE_DEFAULT "shared")
    else()
        set(PAIMON_TEST_LINKAGE_DEFAULT "static")
    endif()

    #----------------------------------------------------------------------
    set_option_category("Lint")

    define_option(PAIMON_VERBOSE_LINT
                  "If off, 'quiet' flags will be passed to linting tools" OFF)

    define_option(PAIMON_LINT_GIT_DIFF_MODE
                  "If on, only git-diff files will be passed to linting tools" ON)

    define_option_string(PAIMON_LINT_GIT_TARGET_COMMIT
                         "target commit/branch for comparison in git diff" "origin/main")

    define_option(PAIMON_GENERATE_COVERAGE "Build with C++ code coverage enabled" OFF)

    #----------------------------------------------------------------------
    set_option_category("Checks")

    define_option(PAIMON_USE_ASAN "Enable Address Sanitizer checks" OFF)
    define_option(PAIMON_USE_TSAN "Enable Thread Sanitizer checks" OFF)
    define_option(PAIMON_USE_UBSAN "Enable Undefined Behaviour Sanitizer checks" OFF)

    #----------------------------------------------------------------------
    set_option_category("Advanced developer")

    define_option(PAIMON_EXTRA_ERROR_CONTEXT
                  "Compile with extra error context (line numbers, code)" OFF)

    option(PAIMON_BUILD_CONFIG_SUMMARY_JSON
           "Summarize build configuration in a JSON file" ON)
endif()

macro(validate_config)
    foreach(category ${PAIMON_OPTION_CATEGORIES})
        set(option_names ${PAIMON_${category}_OPTION_NAMES})

        foreach(name ${option_names})
            set(possible_values ${${name}_OPTION_POSSIBLE_VALUES})
            set(value "${${name}}")
            if(possible_values)
                if(NOT CMAKE_VERSION VERSION_LESS "3.3")
                    if(NOT "${value}" IN_LIST possible_values)
                        message(FATAL_ERROR "Configuration option ${name} got invalid value '${value}'. "
                                            "Allowed values: ${${name}_OPTION_ENUM}.")
                    endif()
                endif()
            endif()
        endforeach()

    endforeach()
endmacro()

macro(config_summary_message)
    message(STATUS "---------------------------------------------------------------------"
    )
    message(STATUS "Paimon version:                                 ${PAIMON_VERSION}")
    message(STATUS)
    message(STATUS "Build configuration summary:")

    message(STATUS "  Generator: ${CMAKE_GENERATOR}")
    message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
    message(STATUS "  Source directory: ${CMAKE_CURRENT_SOURCE_DIR}")
    message(STATUS "  Install prefix: ${CMAKE_INSTALL_PREFIX}")
    message(STATUS "  Install libdir: ${CMAKE_INSTALL_LIBDIR}")

    if(${CMAKE_EXPORT_COMPILE_COMMANDS})
        message(STATUS "  Compile commands: ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json"
        )
    endif()

    foreach(category ${PAIMON_OPTION_CATEGORIES})

        message(STATUS)
        message(STATUS "${category} options:")
        message(STATUS)

        set(option_names ${PAIMON_${category}_OPTION_NAMES})

        foreach(name ${option_names})
            set(value "${${name}}")
            if("${value}" STREQUAL "")
                set(value "\"\"")
            endif()

            set(description ${${name}_OPTION_DESCRIPTION})

            if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
                set(summary "=${value} [default=${${name}_OPTION_ENUM}]")
            else()
                set(summary "=${value} [default=${${name}_OPTION_DEFAULT}]")
            endif()

            message(STATUS "  ${name}${summary}")
            foreach(description_line ${description})
                message(STATUS "      ${description_line}")
            endforeach()
        endforeach()

    endforeach()

endmacro()
