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
 *
 * FFI common layer tests — error/buffer/log behave as documented.
 * Does NOT build on a real index.
 */

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/global_index/tantivy/tantivy_ffi_handle.h"
#include "paimon/global_index/tantivy/tantivy_ffi_log.h"
#include "paimon/global_index/tantivy/tantivy_ffi_status.h"

extern "C" {
#include "paimon_tantivy_ffi.h"  // NOLINT(build/include_subdir)
}

namespace paimon::tantivy {

// ------------------------- last_error contract -------------------------

TEST(TantivyFfiError, LastErrorIsNeverNull) {
    // Before anything, last_error should be a valid non-null pointer to ""
    const char* ptr = paimon_tantivy_last_error();
    ASSERT_NE(ptr, nullptr);
    // Content is thread-local; for freshly-spawned thread it must be empty
    std::atomic<bool> child_ok{false};
    std::thread t([&]() {
        const char* p = paimon_tantivy_last_error();
        child_ok.store(p != nullptr && p[0] == '\0');
    });
    t.join();
    ASSERT_TRUE(child_ok.load());
}

// ------------------------- status translation -------------------------

TEST(TantivyFfiStatus, OkTranslates) {
    Status s = FfiStatusToStatus(PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_OK);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST(TantivyFfiStatus, ErrorCodeNamesShowUp) {
    // Translate a few codes and ensure the name appears in the string form.
    struct Case {
        PaimonTantivyStatus code;
        const char* expected_substr;
    };
    const Case cases[] = {
        {PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_INVALID_ARGUMENT, "InvalidArgument"},
        {PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_NOT_FOUND, "NotFound"},
        {PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_IO_ERROR, "IoError"},
        {PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_UNSUPPORTED, "Unsupported"},
        {PaimonTantivyStatus::PAIMON_TANTIVY_STATUS_TOKENIZER_ERROR, "TokenizerError"},
    };
    for (const auto& c : cases) {
        Status s = FfiStatusToStatus(c.code);
        ASSERT_FALSE(s.ok());
        ASSERT_NE(s.ToString().find(c.expected_substr), std::string::npos)
            << "got: " << s.ToString();
    }
}

// ------------------------- buffer lifetime -------------------------

TEST(TantivyFfiBuffer, EmptyBufferGuard) {
    BufferGuard g;
    ASSERT_EQ(g.size(), 0u);
    ASSERT_EQ(g.data(), nullptr);
    // Destructor must accept empty buffer
}

// ------------------------- handle stress -------------------------

// Sanity stress: create/destroy a dummy "handle" via into_handle/free_handle.
// Since the Rust side doesn't yet export writer/reader, we stress via a
// temporary wrapping of the buffer API: alloc buffers repeatedly, ensure no
// crash (LSAN / ASAN would catch leaks).
TEST(TantivyFfiBuffer, StressAllocFree) {
    for (int i = 0; i < 1000; ++i) {
        BufferGuard g;
        // We don't have a way to populate the buffer from C++ here;
        // this just exercises empty construction + destruction path.
        (void)g;
    }
}

// ------------------------- log bridge -------------------------

namespace {
std::atomic<int> g_log_count{0};
extern "C" void CountingLogCb(int32_t /*level*/, const char* /*msg*/, std::size_t /*len*/) {
    g_log_count.fetch_add(1, std::memory_order_relaxed);
}
}  // namespace

TEST(TantivyFfiLog, SetCallbackIsIdempotent) {
    g_log_count.store(0);
    paimon_tantivy_set_log_callback(&CountingLogCb);
    paimon_tantivy_set_log_callback(&CountingLogCb);
    paimon_tantivy_clear_log_callback();
    // Should not crash even though called multiple times (idempotent install)
    SUCCEED();
}

TEST(TantivyFfiLog, InstallBridgeThenUninstall) {
    // Bridge to glog; must not crash.
    InstallTantivyLogBridge();
    UninstallTantivyLogBridge();
    SUCCEED();
}

// ------------------------- version still works -------------------------

TEST(TantivyFfi, VersionReachable) {
    const char* v = paimon_tantivy_version();
    ASSERT_NE(v, nullptr);
    ASSERT_GT(std::strlen(v), 0u);
}

}  // namespace paimon::tantivy
