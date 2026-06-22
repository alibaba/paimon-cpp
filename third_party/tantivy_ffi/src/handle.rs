//! Opaque handle helpers.
//!
//! Contract:
//! - Rust creates handles with `Box::into_raw(Box::new(T))`
//! - C++ must free with the matching `xxx_free(*mut T)` function, once
//! - Functions accepting handles treat null as invalid argument

use std::ffi::c_void;

/// Consume `T`, return a raw opaque pointer suitable for C++.
#[inline]
pub(crate) fn into_handle<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

/// Reconstitute a `Box<T>` from an FFI-provided pointer and drop it.
/// SAFETY: caller must pass a pointer previously returned by `into_handle::<T>`,
/// and must not use it again after this call.
#[inline]
pub(crate) unsafe fn free_handle<T>(handle: *mut T) {
    if handle.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(handle) });
}

/// Borrow an `&T` from an FFI-provided pointer. Returns None on null.
/// SAFETY: caller must ensure the pointer was previously returned by
/// `into_handle::<T>` and is still alive (not freed).
#[inline]
pub(crate) unsafe fn borrow_handle<'a, T>(handle: *const T) -> Option<&'a T> {
    if handle.is_null() {
        None
    } else {
        Some(unsafe { &*handle })
    }
}

/// Borrow `&mut T` from an FFI-provided pointer. Returns None on null.
/// SAFETY: same as `borrow_handle`, plus caller must ensure there is no
/// concurrent access via another pointer (writer/reader handles are
/// documented as thread-unsafe).
#[inline]
pub(crate) unsafe fn borrow_handle_mut<'a, T>(handle: *mut T) -> Option<&'a mut T> {
    if handle.is_null() {
        None
    } else {
        Some(unsafe { &mut *handle })
    }
}

/// Opaque ctx pointer from C++ (passed through to Rust Directory callbacks).
/// Type-erased on purpose: only C++ side knows the concrete type.
pub(crate) type OpaqueCtx = *mut c_void;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn into_then_free() {
        struct X(i32);
        let h: *mut X = into_handle(X(42));
        assert!(!h.is_null());
        unsafe { free_handle(h) };
        // no leak (LSAN would catch if compiled with sanitizers)
    }

    #[test]
    fn free_null_is_noop() {
        let h: *mut i32 = std::ptr::null_mut();
        unsafe { free_handle(h) };
    }

    #[test]
    fn borrow_roundtrip() {
        let h = into_handle(42i32);
        unsafe {
            assert_eq!(*borrow_handle(h as *const i32).unwrap(), 42);
            *borrow_handle_mut(h).unwrap() = 7;
            assert_eq!(*borrow_handle(h as *const i32).unwrap(), 7);
            free_handle(h);
        }
    }

    #[test]
    fn borrow_null_is_none() {
        unsafe {
            assert!(borrow_handle::<i32>(std::ptr::null()).is_none());
            assert!(borrow_handle_mut::<i32>(std::ptr::null_mut()).is_none());
        }
    }

    #[test]
    fn stress_many_create_destroy() {
        // smoke stress: many allocations, no leak
        for i in 0..10_000 {
            let h = into_handle(vec![i; 8]);
            unsafe {
                let v = borrow_handle(h as *const Vec<i32>).unwrap();
                assert_eq!(v.len(), 8);
                free_handle(h);
            }
        }
    }
}
