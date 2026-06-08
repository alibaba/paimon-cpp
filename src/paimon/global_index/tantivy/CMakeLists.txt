# Copyright 2026-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0.
#
# tantivy-fulltext global index (Rust FFI). See docs/dev/tantivy_fts_migration_plan.md.
# Stage 4 grows the support lib with the C++ writer wrapper + writer test.

if(NOT PAIMON_ENABLE_TANTIVY)
    return()
endif()

set(PAIMON_TANTIVY_SUPPORT_SRCS
    tantivy_ffi_log.cpp
    tantivy_archive_layout.cpp
    tantivy_stream_ctx.cpp
    tantivy_global_index_writer.cpp
    tantivy_global_index_reader.cpp
    tantivy_global_index.cpp
    tantivy_global_index_factory.cpp
)

add_paimon_lib(paimon_tantivy_support
               SOURCES
               ${PAIMON_TANTIVY_SUPPORT_SRCS}
               DEPENDENCIES
               paimon_shared
               paimon_tantivy_ffi
               STATIC_LINK_LIBS
               paimon_tantivy_ffi
               arrow
               glog
               fmt
               SHARED_LINK_LIBS
               paimon_shared
               SHARED_LINK_FLAGS
               ${PAIMON_VERSION_SCRIPT_FLAGS})
# Corrosion's paimon_tantivy_ffi target carries INTERFACE_INCLUDE_DIRECTORIES
# (cbindgen-generated header path). The objlib in add_paimon_lib doesn't link
# against deps,so its compile step misses include dirs.Wire them explicitly.
target_link_libraries(paimon_tantivy_support_objlib PUBLIC paimon_tantivy_ffi)

if(PAIMON_BUILD_TESTS)
    add_paimon_test(tantivy_smoke_test
                    SOURCES
                    tantivy_smoke_test.cpp
                    STATIC_LINK_LIBS
                    paimon_tantivy_ffi
                    ${GTEST_LINK_TOOLCHAIN})

    add_paimon_test(tantivy_ffi_test
                    SOURCES
                    tantivy_ffi_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    "-Wl,--whole-archive"
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})

    # Golden-sample tokenizer diff (cppjieba vs jieba-rs). Links against the
    # lucene index module to reuse JiebaTokenizer::CutWithMode + Normalize, so it
    # can only be built when lucene-fts is enabled (the C++ JiebaTokenizer lives
    # in the lucene module). Guarded so the default LUCENE=OFF / TANTIVY=ON build
    # doesn't try to link the non-existent paimon_lucene_index_static.
    # Note: we mirror the lucene-fts test's link line (see lucene/CMakeLists.txt)
    # rather than using the `jieba` imported target, whose INTERFACE_INCLUDE
    # concatenates two paths in one string (upstream quirk).
    if(PAIMON_ENABLE_LUCENE)
        add_paimon_test(tantivy_tokenizer_test
                        SOURCES
                        tantivy_tokenizer_test.cpp
                        EXTRA_INCLUDES
                        ${LUCENE_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_lucene_index_static
                        "-Wl,--no-whole-archive"
                        paimon_tantivy_ffi
                        ${GTEST_LINK_TOOLCHAIN})
        target_compile_definitions(paimon-tantivy-tokenizer-test
                                   PRIVATE
                                   JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}"
                                   PAIMON_TANTIVY_GOLDEN_DIR="${CMAKE_SOURCE_DIR}/test/test_data/tokenizer_golden")
        target_include_directories(paimon-tantivy-tokenizer-test
                                   SYSTEM PRIVATE
                                   ${JIEBA_INCLUDE_DIR}
                                   ${JIEBA_DICT_DIR})
    endif()

    # Stage 4 — Writer test. Builds an Arrow batch, runs the writer through
    # GlobalIndexFileManager + LocalFileSystem, then validates the packed
    # on-disk format. Reader round-trip lives in Stage 6.
    add_paimon_test(tantivy_writer_test
                    SOURCES
                    tantivy_writer_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    arrow
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})
    target_compile_definitions(paimon-tantivy-writer-test
                               PRIVATE
                               JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

    # Stage 6 — Reader + 5 query types end-to-end.
    add_paimon_test(tantivy_reader_test
                    SOURCES
                    tantivy_reader_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    arrow
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})
    target_compile_definitions(paimon-tantivy-reader-test
                               PRIVATE
                               JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

    # Stage 7 — limit + pre_filter + scoring.
    add_paimon_test(tantivy_filter_limit_test
                    SOURCES
                    tantivy_filter_limit_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    arrow
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})
    target_compile_definitions(paimon-tantivy-filter-limit-test
                               PRIVATE
                               JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

    # Java → C++ cross-read test. Fixture produced by paimon-java's
    # `TantivyIndexFixtureGen` (see docs/dev/tantivy_java_cross_read_plan.md)
    # and checked in under test/test_data/java_tantivy_fixtures/.
    add_paimon_test(tantivy_java_compat_test
                    SOURCES
                    tantivy_java_compat_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    arrow
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})
    target_compile_definitions(paimon-tantivy-java-compat-test
                               PRIVATE
                               JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}"
                               PAIMON_TANTIVY_JAVA_FIXTURE_DIR="${CMAKE_SOURCE_DIR}/test/test_data/java_tantivy_fixtures"
                               PAIMON_TANTIVY_CPP_FIXTURE_DIR="${CMAKE_SOURCE_DIR}/test/test_data/cpp_tantivy_fixtures")

    # K4 — V3 streaming reader + W1 streaming writer integration coverage:
    # ParseArchiveHeader fuzz, concurrent query on shared reader, concurrent
    # reader create+drop lifecycle, streaming benchmark log.
    add_paimon_test(tantivy_streaming_test
                    SOURCES
                    tantivy_streaming_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    arrow
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})
    target_compile_definitions(paimon-tantivy-streaming-test
                               PRIVATE
                               JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

    # Stage 8 — TantivyGlobalIndex + factory + end-to-end integration test.
    # `--whole-archive` is required so the static REGISTER_PAIMON_FACTORY
    # symbols are not stripped out of the test binary.
    add_paimon_test(tantivy_index_test
                    SOURCES
                    tantivy_index_test.cpp
                    STATIC_LINK_LIBS
                    paimon_shared
                    test_utils_static
                    "-Wl,--whole-archive"
                    paimon_local_file_system_static
                    paimon_tantivy_support_static
                    "-Wl,--no-whole-archive"
                    paimon_tantivy_ffi
                    arrow
                    glog
                    fmt
                    ${GTEST_LINK_TOOLCHAIN})
    target_compile_definitions(paimon-tantivy-index-test
                               PRIVATE
                               JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

    # Stage 9 — Cross-implementation coexistence. Links against BOTH the
    # lucene and tantivy support static libs to verify they resolve their
    # `REGISTER_PAIMON_FACTORY` registrations side by side and don't
    # collide on shared symbols. Only built when lucene-fts is enabled.
    if(PAIMON_ENABLE_LUCENE)
        add_paimon_test(tantivy_lucene_coexist_test
                        SOURCES
                        tantivy_lucene_coexist_test.cpp
                        EXTRA_INCLUDES
                        ${LUCENE_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_lucene_index_static
                        paimon_tantivy_support_static
                        "-Wl,--no-whole-archive"
                        paimon_tantivy_ffi
                        arrow
                        glog
                        fmt
                        ${GTEST_LINK_TOOLCHAIN})
        target_compile_definitions(paimon-tantivy-lucene-coexist-test
                                   PRIVATE
                                   JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")

        # Stage 10 — Equivalence + benchmark. Same link line as the coexist
        # test (needs both impls); benchmark output goes to stderr.
        add_paimon_test(tantivy_equivalence_test
                        SOURCES
                        tantivy_equivalence_test.cpp
                        EXTRA_INCLUDES
                        ${LUCENE_INCLUDE_DIR}
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        "-Wl,--whole-archive"
                        paimon_local_file_system_static
                        paimon_lucene_index_static
                        paimon_tantivy_support_static
                        "-Wl,--no-whole-archive"
                        paimon_tantivy_ffi
                        arrow
                        glog
                        fmt
                        ${GTEST_LINK_TOOLCHAIN})
        target_compile_definitions(paimon-tantivy-equivalence-test
                                   PRIVATE
                                   JIEBA_TEST_DICT_DIR="${JIEBA_DICT_DIR}")
    endif()
endif()
