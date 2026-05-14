/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * Bridge tantivy (Rust) logs into paimon's logger.
 * See docs/dev/tantivy_ffi_design.md §7.
 *
 * Registered once at TantivyGlobalIndexFactory static-init time.
 */
#pragma once

namespace paimon::tantivy {

/// Install the Rust -> C++ log callback. Idempotent; only the last caller's
/// callback is active. Threading: C callback runs on tantivy worker threads;
/// our adapter must be thread-safe (it routes to glog which is).
void InstallTantivyLogBridge();

/// Uninstall (revert to Rust stderr). Mostly useful for tests.
void UninstallTantivyLogBridge();

}  // namespace paimon::tantivy
