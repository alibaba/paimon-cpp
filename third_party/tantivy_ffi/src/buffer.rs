//! `paimon_tantivy_buffer_t`: Rust-allocated byte buffer returned to C++.
//!
//! Contract:
//! - Buffer is allocated by Rust (as a `Box<[u8]>`)
//! - C++ reads `data[0..len]`, **must not** write past len
//! - C++ must call `paimon_tantivy_buffer_free()` exactly once per non-empty buffer
//! - Empty (len=0) buffer has null `data`; buffer_free accepts it as no-op
//!
//! This struct is #[repr(C)] so cbindgen generates a matching C struct.

use std::ptr;

#[repr(C)]
pub struct PaimonTantivyBuffer {
    /// Pointer to `len` bytes. Null iff len == 0.
    pub data: *mut u8,
    /// Number of valid bytes.
    pub len: usize,
    /// Internal capacity hint for Rust-side reconstruction. C++ treats as opaque.
    pub capacity: usize,
}

impl PaimonTantivyBuffer {
    /// Build a buffer from owned bytes; consumes the Vec.
    pub(crate) fn from_vec(mut v: Vec<u8>) -> Self {
        if v.is_empty() {
            return Self::empty();
        }
        v.shrink_to_fit();
        let len = v.len();
        let capacity = v.capacity();
        let data = v.as_mut_ptr();
        std::mem::forget(v);
        Self { data, len, capacity }
    }

    pub(crate) fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }
}

/// Free a buffer returned by any Rust FFI function. Safe to call on an empty
/// buffer (len=0 / data=null). Must only be called once per buffer.
///
/// SAFETY: `buf` must be either null, or point to a live `paimon_tantivy_buffer_t`
/// produced by this crate and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_buffer_free(buf: *mut PaimonTantivyBuffer) {
    if buf.is_null() {
        return;
    }
    let b = unsafe { &mut *buf };
    if b.len != 0 && !b.data.is_null() {
        // Reconstruct the Vec<u8> and drop it
        let v = unsafe { Vec::from_raw_parts(b.data, b.len, b.capacity) };
        drop(v);
    }
    b.data = ptr::null_mut();
    b.len = 0;
    b.capacity = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_has_null_data() {
        let b = PaimonTantivyBuffer::empty();
        assert!(b.data.is_null());
        assert_eq!(b.len, 0);
    }

    #[test]
    fn from_vec_roundtrip() {
        let src = vec![1u8, 2, 3, 4, 5];
        let src_clone = src.clone();
        let mut b = PaimonTantivyBuffer::from_vec(src);
        assert_eq!(b.len, 5);
        assert!(!b.data.is_null());
        let view: &[u8] = unsafe { std::slice::from_raw_parts(b.data, b.len) };
        assert_eq!(view, src_clone.as_slice());
        unsafe { paimon_tantivy_buffer_free(&mut b) };
        assert!(b.data.is_null());
        assert_eq!(b.len, 0);
    }

    #[test]
    fn free_null_is_noop() {
        unsafe { paimon_tantivy_buffer_free(std::ptr::null_mut()) };
    }

    #[test]
    fn free_empty_is_noop() {
        let mut b = PaimonTantivyBuffer::empty();
        unsafe { paimon_tantivy_buffer_free(&mut b) };
    }

    #[test]
    fn stress_alloc_free() {
        // LSAN would catch any leak
        for i in 0..5_000usize {
            let mut b = PaimonTantivyBuffer::from_vec(vec![42u8; i.min(256)]);
            unsafe { paimon_tantivy_buffer_free(&mut b) };
        }
    }
}
