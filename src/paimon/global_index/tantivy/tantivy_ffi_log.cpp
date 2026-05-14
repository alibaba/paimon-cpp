/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0.
 */

#include "paimon/global_index/tantivy/tantivy_ffi_log.h"

#include <cstring>
#include <string>

#include "glog/logging.h"

extern "C" {
#include "paimon_tantivy_ffi.h"
}

namespace paimon::tantivy {
namespace {

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

}  // namespace

void InstallTantivyLogBridge() {
    paimon_tantivy_set_log_callback(&PaimonTantivyLogAdapter);
}

void UninstallTantivyLogBridge() {
    paimon_tantivy_clear_log_callback();
}

}  // namespace paimon::tantivy
