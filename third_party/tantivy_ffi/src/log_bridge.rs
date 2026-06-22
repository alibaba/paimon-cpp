//! Log bridge: tantivy internally emits log records via the `log` crate
//! (via `tantivy::debug` / `info` etc.). This module registers a global
//! `log::Log` implementation that forwards records to a C callback.
//!
//! Contract:
//! - C++ calls `paimon_tantivy_set_log_callback(cb)` once at process startup
//! - Passing null unregisters (reverts to stderr)
//! - Callback receives (level, msg_ptr, msg_len); pointer is non-null,
//!   UTF-8, NOT null-terminated, valid only for the duration of the call
//! - Level mapping: 0=trace 1=debug 2=info 3=warn 4=error
//! - Callback must be thread-safe: tantivy writes from worker threads
//!
//! NOTE: tantivy uses `tracing` in newer versions and `log` in others.
//! Our current `tantivy = "0.22"` uses `log`.
//! If a future upgrade switches to `tracing`, install a `tracing-log`
//! bridge here.

use std::ffi::c_char;
use std::sync::atomic::{AtomicPtr, Ordering};

pub type PaimonTantivyLogFn = extern "C" fn(level: i32, msg: *const c_char, len: usize);

static CALLBACK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

struct LogBridge;

impl log::Log for LogBridge {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let level = match record.level() {
            log::Level::Trace => 0,
            log::Level::Debug => 1,
            log::Level::Info => 2,
            log::Level::Warn => 3,
            log::Level::Error => 4,
        };
        let msg = format!("[{}] {}", record.target(), record.args());
        let ptr = CALLBACK.load(Ordering::Acquire);
        if ptr.is_null() {
            // Fallback: stderr
            eprintln!("{msg}");
            return;
        }
        // SAFETY: ptr was installed as PaimonTantivyLogFn via transmute below
        let cb: PaimonTantivyLogFn = unsafe { std::mem::transmute(ptr) };
        cb(level, msg.as_ptr() as *const c_char, msg.len());
    }

    fn flush(&self) {}
}

static LOGGER: LogBridge = LogBridge;

/// Install a non-null callback. First call also registers `LogBridge` as
/// the global `log` crate sink. Subsequent calls swap the callback atomically.
/// Thread-safety: safe to call from any thread.
///
/// Note: we use separate `set`/`clear` functions instead of `Option<fn>`
/// because cbindgen translates `Option<extern "C" fn>` into an opaque struct
/// rather than a nullable C function pointer.
#[no_mangle]
pub extern "C" fn paimon_tantivy_set_log_callback(cb: PaimonTantivyLogFn) {
    let ptr = cb as *mut ();
    CALLBACK.store(ptr, Ordering::Release);
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Info);
}

/// Clear the installed callback (revert to Rust-side stderr fallback).
#[no_mangle]
pub extern "C" fn paimon_tantivy_clear_log_callback() {
    CALLBACK.store(std::ptr::null_mut(), Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Simple test callback that counts invocations
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    extern "C" fn counting_cb(_: i32, _: *const c_char, _: usize) {
        COUNT.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn install_then_log() {
        COUNT.store(0, Ordering::SeqCst);
        paimon_tantivy_set_log_callback(counting_cb);
        log::info!("hello");
        assert!(COUNT.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn clear_reverts_to_stderr() {
        paimon_tantivy_set_log_callback(counting_cb);
        paimon_tantivy_clear_log_callback();
        log::warn!("goes to stderr");
    }
}
