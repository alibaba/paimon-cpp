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
# https://github.com/apache/arrow/blob/main/cpp/cmake_modules/BuildUtils.cmake

function(add_paimon_lib LIB_NAME)
    set(options BUILD_SHARED BUILD_STATIC)
    set(one_value_args SHARED_LINK_FLAGS)
    set(multi_value_args
        SOURCES
        STATIC_LINK_LIBS
        SHARED_LINK_LIBS
        EXTRA_INCLUDES
        PRIVATE_INCLUDES
        DEPENDENCIES)
    cmake_parse_arguments(ARG
                          "${options}"
                          "${one_value_args}"
                          "${multi_value_args}"
                          ${ARGN})
    if(ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif()

    # Allow overriding PAIMON_BUILD_SHARED and PAIMON_BUILD_STATIC
    if(ARG_BUILD_SHARED)
        set(BUILD_SHARED ${ARG_BUILD_SHARED})
    else()
        set(BUILD_SHARED ${PAIMON_BUILD_SHARED})
    endif()
    if(ARG_BUILD_STATIC)
        set(BUILD_STATIC ${ARG_BUILD_STATIC})
    else()
        set(BUILD_STATIC ${PAIMON_BUILD_STATIC})
    endif()

    # Generate a single "objlib" from all C++ modules and link
    # that "objlib" into each library kind, to avoid compiling twice
    add_library(${LIB_NAME}_objlib OBJECT ${ARG_SOURCES})
    # Necessary to make static linking into other shared libraries work properly
    set_property(TARGET ${LIB_NAME}_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
    if(ARG_DEPENDENCIES)
        # In static-only builds, some dependency names are still declared as
        # *_shared. Map them to *_static when the shared target is unavailable.
        set(_paimon_objlib_deps)
        foreach(_paimon_dep IN LISTS ARG_DEPENDENCIES)
            set(_paimon_mapped_dep "${_paimon_dep}")
            if(NOT TARGET ${_paimon_mapped_dep} AND _paimon_dep MATCHES "_shared$")
                string(REGEX REPLACE "_shared$" "_static" _paimon_mapped_dep
                                     "${_paimon_dep}")
            endif()
            if(TARGET ${_paimon_mapped_dep})
                list(APPEND _paimon_objlib_deps ${_paimon_mapped_dep})
            endif()
            unset(_paimon_mapped_dep)
        endforeach()
        if(_paimon_objlib_deps)
            add_dependencies(${LIB_NAME}_objlib ${_paimon_objlib_deps})
        endif()
        unset(_paimon_objlib_deps)
        unset(_paimon_dep)
    endif()
    set(LIB_DEPS $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    set(LIB_INCLUDES)
    set(EXTRA_DEPS)

    if(ARG_EXTRA_INCLUDES)
        target_include_directories(${LIB_NAME}_objlib SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()
    if(ARG_PRIVATE_INCLUDES)
        target_include_directories(${LIB_NAME}_objlib PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    set(RUNTIME_INSTALL_DIR bin)

    if(BUILD_SHARED)
        add_library(${LIB_NAME}_shared SHARED ${LIB_DEPS})
        if(EXTRA_DEPS)
            add_dependencies(${LIB_NAME}_shared ${EXTRA_DEPS})
        endif()

        if(LIB_INCLUDES)
            target_include_directories(${LIB_NAME}_shared SYSTEM
                                       PUBLIC ${ARG_EXTRA_INCLUDES})
        endif()

        if(ARG_PRIVATE_INCLUDES)
            target_include_directories(${LIB_NAME}_shared PRIVATE ${ARG_PRIVATE_INCLUDES})
        endif()

        set_target_properties(${LIB_NAME}_shared
                              PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                         "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}"
                                         RUNTIME_OUTPUT_DIRECTORY
                                         "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
                                         PDB_OUTPUT_DIRECTORY
                                         "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}"
                                         LINK_FLAGS "${ARG_SHARED_LINK_FLAGS}"
                                         OUTPUT_NAME ${LIB_NAME})

        target_link_libraries(${LIB_NAME}_shared
                              LINK_PUBLIC
                              "$<BUILD_INTERFACE:${ARG_SHARED_LINK_LIBS}>"
                              "$<INSTALL_INTERFACE:${ARG_SHARED_INSTALL_INTERFACE_LIBS}>"
                              LINK_PRIVATE
                              "$<BUILD_INTERFACE:${ARG_STATIC_LINK_LIBS}>"
                              ${ARG_SHARED_PRIVATE_LINK_LIBS})

        target_link_libraries(${LIB_NAME}_shared
                              PUBLIC "$<BUILD_INTERFACE:paimon_sanitizer_flags>")

        target_link_options(${LIB_NAME}_shared
                            PRIVATE
                            -Wl,--exclude-libs,ALL
                            -Wl,-Bsymbolic
                            -Wl,-z,defs
                            -Wl,--gc-sections)

        install(TARGETS ${LIB_NAME}_shared ${INSTALL_IS_OPTIONAL}
                EXPORT ${LIB_NAME}_targets
                RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
                LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
                ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
                INCLUDES
                DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    endif()

    if(BUILD_STATIC)
        add_library(${LIB_NAME}_static STATIC ${LIB_DEPS})
        if(EXTRA_DEPS)
            add_dependencies(${LIB_NAME}_static ${EXTRA_DEPS})
        endif()

        if(LIB_INCLUDES)
            target_include_directories(${LIB_NAME}_static SYSTEM
                                       PUBLIC ${ARG_EXTRA_INCLUDES})
        endif()

        if(ARG_PRIVATE_INCLUDES)
            target_include_directories(${LIB_NAME}_static PRIVATE ${ARG_PRIVATE_INCLUDES})
        endif()

        set(LIB_NAME_STATIC ${LIB_NAME})

        set_target_properties(${LIB_NAME}_static
                              PROPERTIES ARCHIVE_OUTPUT_DIRECTORY
                                         "${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}"
                                         OUTPUT_NAME ${LIB_NAME_STATIC})

        if(ARG_STATIC_INSTALL_INTERFACE_LIBS)
            target_link_libraries(${LIB_NAME}_static
                                  LINK_PUBLIC
                                  "$<INSTALL_INTERFACE:${ARG_STATIC_INSTALL_INTERFACE_LIBS}>"
            )
        endif()

        if(ARG_STATIC_LINK_LIBS)
            target_link_libraries(${LIB_NAME}_static LINK_PRIVATE
                                  "$<BUILD_INTERFACE:${ARG_STATIC_LINK_LIBS}>")
        endif()

        target_link_libraries(${LIB_NAME}_static
                              PUBLIC "$<BUILD_INTERFACE:paimon_sanitizer_flags>")

        install(TARGETS ${LIB_NAME}_static ${INSTALL_IS_OPTIONAL}
                EXPORT ${LIB_NAME}_targets
                RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
                LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
                ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
                INCLUDES
                DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    endif()
endfunction()

#
# Testing
#
# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# If given, SOURCES is the list of C++ source files to compile into the test
# executable.  Otherwise, "REL_TEST_NAME.cpp" is used.
#
# The unit test is added with a label of "unittest" to support filtering with
# ctest.
#
# Arguments after the test name will be passed to set_tests_properties().
#
# \arg ENABLED if passed, add this unit test even if PAIMON_BUILD_TESTS is off
# \arg PREFIX a string to append to the name of the test executable. For
# example, if you have src/paimon/foo/bar-test.cpp, then PREFIX "foo" will create
# test executable foo-bar-test
# \arg LABELS the unit test label or labels to assign the unit tests
# to. By default, unit tests will go in the "unittest" group, but if we have
# multiple unit tests in some subgroup, you can assign a test to multiple
# groups use the syntax unittest;GROUP2;GROUP3. Custom targets for the group
# names must exist
function(add_test_case REL_TEST_NAME)
    set(options NO_VALGRIND ENABLED)
    set(one_value_args PRECOMPILED_HEADER_LIB)
    set(multi_value_args
        SOURCES
        PRECOMPILED_HEADERS
        STATIC_LINK_LIBS
        EXTRA_LINK_LIBS
        EXTRA_INCLUDES
        EXTRA_DEPENDENCIES
        LABELS
        EXTRA_LABELS
        PREFIX)
    cmake_parse_arguments(ARG
                          "${options}"
                          "${one_value_args}"
                          "${multi_value_args}"
                          ${ARGN})
    if(ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif()

    if(NO_TESTS AND NOT ARG_ENABLED)
        return()
    endif()
    get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

    if(ARG_PREFIX)
        set(TEST_NAME "${ARG_PREFIX}-${TEST_NAME}")
    endif()

    if(ARG_SOURCES)
        set(SOURCES ${ARG_SOURCES})
    else()
        set(SOURCES "${REL_TEST_NAME}.cpp")
    endif()

    # Make sure the executable name contains only hyphens, not underscores
    string(REPLACE "_" "-" TEST_NAME ${TEST_NAME})
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    message(STATUS ${TEST_NAME})
    add_executable(${TEST_NAME} ${SOURCES})

    if(ARG_STATIC_LINK_LIBS)
        target_link_libraries(${TEST_NAME} PRIVATE ${ARG_STATIC_LINK_LIBS})
    else()
        target_link_libraries(${TEST_NAME} PRIVATE ${PAIMON_TEST_LINK_LIBS})
    endif()

    if(ARG_EXTRA_LINK_LIBS)
        target_link_libraries(${TEST_NAME} PRIVATE ${ARG_EXTRA_LINK_LIBS})
    endif()

    if(ARG_EXTRA_INCLUDES)
        target_include_directories(${TEST_NAME} SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_EXTRA_DEPENDENCIES)
        add_dependencies(${TEST_NAME} ${ARG_EXTRA_DEPENDENCIES})
    endif()

    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        target_compile_options(${TEST_NAME} PRIVATE -Wno-global-constructors)
    endif()
    target_compile_options(${TEST_NAME} PRIVATE -fno-access-control)

    add_test(${TEST_NAME}
             ${BUILD_SUPPORT_DIR}/run-test.sh
             ${CMAKE_BINARY_DIR}
             test
             ${TEST_PATH})

    # Add test as dependency of relevant targets
    foreach(TARGET ${ARG_LABELS})
        add_dependencies(${TARGET} ${TEST_NAME})
    endforeach()

    set(LABELS)
    list(APPEND LABELS "unittest")
    if(ARG_LABELS)
        list(APPEND LABELS ${ARG_LABELS})
    endif()
    # EXTRA_LABELS don't create their own dependencies, they are only used
    # to ease running certain test categories.
    if(ARG_EXTRA_LABELS)
        list(APPEND LABELS ${ARG_EXTRA_LABELS})
    endif()

    foreach(LABEL ${ARG_LABELS})
        # ensure there is a cmake target which exercises tests with this LABEL
        set(LABEL_TEST_NAME "test-${LABEL}")
        if(NOT TARGET ${LABEL_TEST_NAME})
            add_custom_target(${LABEL_TEST_NAME}
                              ctest -L "${LABEL}" --output-on-failure
                              USES_TERMINAL)
        endif()
        # ensure the test is (re)built before the LABEL test runs
        add_dependencies(${LABEL_TEST_NAME} ${TEST_NAME})
    endforeach()

    set_property(TEST ${TEST_NAME}
                 APPEND
                 PROPERTY LABELS ${LABELS})
endfunction()

# Adding unit tests part of the "paimon" portion of the test suite
function(add_paimon_test REL_TEST_NAME)
    set(options)
    set(one_value_args PREFIX)
    set(multi_value_args LABELS PRECOMPILED_HEADERS)
    cmake_parse_arguments(ARG
                          "${options}"
                          "${one_value_args}"
                          "${multi_value_args}"
                          ${ARGN})
    if(ARG_PREFIX)
        set(PREFIX ${ARG_PREFIX})
    else()
        set(PREFIX "paimon")
    endif()

    if(ARG_LABELS)
        set(LABELS ${ARG_LABELS})
    else()
        set(LABELS "paimon-tests")
    endif()

    add_test_case(${REL_TEST_NAME}
                  PREFIX
                  ${PREFIX}
                  LABELS
                  ${LABELS}
                  ${PCH_ARGS}
                  ${ARG_UNPARSED_ARGUMENTS})
endfunction()
