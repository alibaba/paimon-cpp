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

#include "paimon/global_index/tantivy/tantivy_ffi_log.h"

#include <cstring>
#include <string>

#include "glog/logging.h"

extern "C" {
#include "paimon_tantivy_ffi.h"  // NOLINT(build/include_subdir)
}

namespace paimon::tantivy {

// Not in an anonymous namespace: `extern "C"` (external linkage) and an
// anonymous namespace (internal linkage) are contradictory. Only referenced
// via function pointer below, so symbol visibility is not a concern.
/// Level mapping matches Rust side (0=trace..4=error).
extern "C" void PaimonTantivyLogAdapter(int32_t level, const char* msg, std::size_t len) {
    // msg is NOT null-terminated; slice with len.
    std::string s(msg, len);
    switch (level) {
        case 4:
            LOG(ERROR) << "[tantivy] " << s;
            break;
        case 3:
            LOG(WARNING) << "[tantivy] " << s;
            break;
        case 2:
            LOG(INFO) << "[tantivy] " << s;
            break;
        case 1:
            VLOG(1) << "[tantivy] " << s;
            break;
        case 0:
            VLOG(2) << "[tantivy] " << s;
            break;
        default:
            LOG(INFO) << "[tantivy:lvl=" << level << "] " << s;
            break;
    }
}

void InstallTantivyLogBridge() {
    paimon_tantivy_set_log_callback(&PaimonTantivyLogAdapter);
}

void UninstallTantivyLogBridge() {
    paimon_tantivy_clear_log_callback();
}

}  // namespace paimon::tantivy
