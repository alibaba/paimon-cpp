//! Error model for paimon_tantivy_ffi.
//!
//! Contract:
//! - Every fallible FFI function returns `paimon_tantivy_status_t`
//! - Failure sets `last_error` (thread-local) with human-readable text
//! - C++ calls `paimon_tantivy_last_error()` after a non-OK status to fetch text
//! - Pointer returned by `last_error()` is thread-local and valid until the
//!   next failing FFI call on the same thread. C++ must NOT free it.

use std::cell::RefCell;
use std::ffi::c_char;
use std::ffi::CString;

/// Status codes. Values are stable ABI; append-only.
#[repr(i32)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PaimonTantivyStatus {
    Ok = 0,
    InvalidArgument = 1,
    NotFound = 2,
    IoError = 3,
    Unsupported = 4,
    TokenizerError = 5,
    QueryParseError = 6,
    IndexFormatError = 7,
    InternalError = 99,
}

thread_local! {
    /// Pre-allocated empty string so `paimon_tantivy_last_error()` can always
    /// return a valid non-null pointer.
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::new("").unwrap());
}

/// Record an error message for the current thread. Called by fallible FFI
/// functions right before returning a non-OK status.
pub(crate) fn set_last_error(msg: impl Into<String>) {
    // Interior nul bytes would make CString::new fail; strip them as a safety net.
    let s: String = msg.into().replace('\0', "\u{FFFD}");
    LAST_ERROR.with(|cell| {
        // CString::new clones the bytes and appends a nul terminator.
        *cell.borrow_mut() = CString::new(s).unwrap_or_else(|_| CString::new("").unwrap());
    });
}

/// Clear the current thread's error slot. Called at the top of fallible APIs
/// so a subsequent successful call doesn't return stale text.
#[allow(dead_code)]
pub(crate) fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = CString::new("").unwrap();
    });
}

/// Macro that wraps a `Result<T, String>`-returning block: sets last_error on
/// Err and returns the given status code; returns Ok value on success.
#[macro_export]
macro_rules! ffi_try {
    ($expr:expr, $err_status:expr) => {{
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $crate::error::set_last_error(format!("{e}"));
                return $err_status;
            }
        }
    }};
}

/// Return the last error text for the calling thread. Always non-null; returns
/// pointer to "" when there is no error recorded yet. Pointer is thread-local;
/// C++ must NOT free it; treat as valid until the next failing FFI call on
/// the same thread.
#[no_mangle]
pub extern "C" fn paimon_tantivy_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| cell.borrow().as_ptr())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn initial_last_error_is_empty() {
        let ptr = paimon_tantivy_last_error();
        assert!(!ptr.is_null());
        let s = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn set_then_retrieve() {
        set_last_error("boom");
        let s = unsafe { CStr::from_ptr(paimon_tantivy_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(s, "boom");
    }

    #[test]
    fn clear_resets_to_empty() {
        set_last_error("x");
        clear_last_error();
        let s = unsafe { CStr::from_ptr(paimon_tantivy_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn embedded_nul_is_stripped() {
        set_last_error("a\0b");
        let s = unsafe { CStr::from_ptr(paimon_tantivy_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(s, "a\u{FFFD}b");
    }

    #[test]
    fn thread_local_isolation() {
        set_last_error("main");
        let t = std::thread::spawn(|| {
            let s = unsafe { CStr::from_ptr(paimon_tantivy_last_error()) }
                .to_str()
                .unwrap();
            s.to_owned()
        })
        .join()
        .unwrap();
        assert_eq!(t, "");
        let s = unsafe { CStr::from_ptr(paimon_tantivy_last_error()) }
            .to_str()
            .unwrap();
        assert_eq!(s, "main");
    }
}
