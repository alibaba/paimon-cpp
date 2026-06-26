//! paimon_tantivy_ffi: C ABI layer for tantivy + jieba-rs,
//! consumed by paimon-cpp's `tantivy-fulltext` global index.
//!
//! Modules: error / handle / buffer / log (common FFI layer), tokenizer,
//! writer, callback directory, and reader (query).

#![deny(unsafe_op_in_unsafe_fn)]

use std::ffi::c_char;

pub mod error;
pub mod handle;
pub mod buffer;
pub mod log_bridge;
pub mod tokenizer;
pub mod writer;
pub mod callback_directory;
pub mod reader;

// Re-export public FFI symbols at crate root so cbindgen picks them up.
pub use buffer::{paimon_tantivy_buffer_free, PaimonTantivyBuffer};
pub use error::{paimon_tantivy_last_error, PaimonTantivyStatus};
pub use log_bridge::{
    paimon_tantivy_clear_log_callback, paimon_tantivy_set_log_callback, PaimonTantivyLogFn,
};
pub use tokenizer::{
    paimon_tantivy_tokenizer_free, paimon_tantivy_tokenizer_new,
    paimon_tantivy_tokenizer_tokenize, PaimonJiebaTokenizer,
};
pub use writer::{
    paimon_tantivy_writer_add, paimon_tantivy_writer_finish_streaming,
    paimon_tantivy_writer_free, paimon_tantivy_writer_new, PaimonTantivyWriter,
    PaimonWriteCallbacks,
};
pub use callback_directory::{PaimonCallbackDirectory, PaimonStreamCallbacks};
pub use reader::{
    paimon_tantivy_reader_free, paimon_tantivy_reader_new_streaming,
    paimon_tantivy_reader_search, PaimonTantivyReader,
};

/// Semantic version of this crate, **'static lifetime**; C++ must NOT free.
/// Format: `"<semver>"` (git sha postfix can be added later via build.rs).
/// Returned as a NUL-terminated UTF-8 C string.
#[no_mangle]
pub extern "C" fn paimon_tantivy_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn version_is_non_empty() {
        let ptr = paimon_tantivy_version();
        assert!(!ptr.is_null());
        let s = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap();
        assert!(!s.is_empty(), "version must be non-empty");
        assert!(s.contains('.'), "version must look like semver, got {s:?}");
    }

    #[test]
    fn tantivy_and_jieba_are_linked() {
        let _ = tantivy::schema::Schema::builder();
        let _ = jieba_rs::Jieba::new();
    }

    #[test]
    fn croaring_serialize_roundtrip() {
        use croaring::Bitmap;
        let mut b = Bitmap::new();
        b.add(42);
        b.add(100);
        let bytes = b.serialize::<croaring::Portable>();
        let b2 = Bitmap::deserialize::<croaring::Portable>(&bytes);
        assert_eq!(b.cardinality(), b2.cardinality());
    }
}
