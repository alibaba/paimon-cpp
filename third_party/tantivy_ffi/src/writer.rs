//! PaimonTantivyWriter: Writer for tantivy-fulltext global index.
//!
//! Contract (see docs/dev/tantivy_java_compat_plan.md §2.5 + §5.1 J2):
//! - `writer_new(field_name, mode, with_position, dict_dir, out)` — create on a
//!   private tmp dir backed by MmapDirectory + PaimonJiebaTokenizer.
//!   `field_name` is **ignored** by the Rust schema (kept for FFI ABI
//!   compatibility); schema field names are fixed (`row_id`, `text`) to match
//!   paimon-java `paimon-tantivy-jni/rust/src/lib.rs:55-66`.
//! - `writer_add(writer, row_id, text, len)` — add a single document with the
//!   caller-supplied `row_id` (u64) and a TEXT field
//! - `writer_finish(writer, out_row_count, out_buf)` — commit + force-merge to
//!   single segment + pack all on-disk index files into a Rust-allocated buffer
//! - `writer_free(writer)` — destroy (RAII removes tmp dir)
//!
//! Packing format (big-endian, **cross-readable with paimon-java archive**;
//! see `paimon-tantivy-index/README.md` §Archive File Format):
//!   `[i32 BE file_count |
//!     (i32 BE name_len | name_bytes | i64 BE file_len | file_bytes)*]`

use std::ffi::{c_char, c_void, CStr};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::{doc, Index, IndexWriter, TantivyDocument};
use tempfile::TempDir;

use crate::error::{set_last_error, PaimonTantivyStatus};
use crate::handle::{borrow_handle_mut, free_handle, into_handle};
use crate::tokenizer::{PaimonJiebaTokenizer, TokenizeMode};

/// Schema field names. Fixed to match paimon-java's tantivy schema so that
/// indexes are cross-readable. Both fields are required.
pub const PAIMON_ROW_ID_FIELD_NAME: &str = "row_id";
pub const PAIMON_TEXT_FIELD_NAME: &str = "text";

/// Name registered with the tantivy `TokenizerManager`. Reader must register
/// the same name to make stored term dictionaries readable.
pub const PAIMON_TOKENIZER_NAME: &str = "paimon_jieba";

/// Heap budget for the in-process IndexWriter (50 MB; tantivy minimum is ~3 MB).
/// Default multi-threaded writer (`Index::writer(heap)`) splits this budget
/// across `min(num_cpus, MAX_NUM_THREAD=8)` worker threads.
const WRITER_HEAP_SIZE: usize = 50_000_000;

pub struct PaimonTantivyWriter {
    /// Owned tmp dir; cleaned up when this struct drops.
    tmpdir: TempDir,
    /// `row_id` u64 field (stored + indexed + fast). Reader retrieves the
    /// caller-supplied row_id via `fast_fields().u64("row_id").first(doc_id)`.
    row_id_field: Field,
    /// `text` TEXT field tokenized via the registered jieba tokenizer.
    text_field: Field,
    /// tantivy index instance, file-backed in `tmpdir`.
    index: Index,
    /// Active writer; consumed by `wait_merging_threads()` in `finish`.
    writer: Option<IndexWriter>,
    /// Documents added since construction.
    row_count: i64,
}

impl PaimonTantivyWriter {
    pub fn new(
        field_name: &str,
        mode: TokenizeMode,
        with_position: bool,
        dict_dir: &Path,
        tokenizer_name: &str,
    ) -> Result<Self, String> {
        if field_name.is_empty() {
            return Err("field_name must be non-empty".into());
        }
        // Schema is fixed to match paimon-java: row_id (u64
        // stored+indexed+fast) + text (TEXT). The caller-supplied `field_name`
        // parameter is currently ignored by the Rust schema (kept for FFI
        // backward-compatibility); the C++ side still uses it to extract the
        // right column from arrow batches.
        let _ = field_name; // intentionally unused on the Rust side
        let mut schema_builder = Schema::builder();
        let row_id_field = schema_builder.add_u64_field(
            PAIMON_ROW_ID_FIELD_NAME,
            NumericOptions::default()
                .set_stored()
                .set_indexed()
                .set_fast(),
        );
        let index_option = if with_position {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::Basic
        };
        // Empty input falls back to tantivy's built-in "default" (SimpleTokenizer),
        // matching the cpp-side default in `tantivy_defs.h::kDefaultTantivyWriteTokenizer`.
        // Cross-read with paimon-java works out of the box; CJK callers must
        // pass "paimon_jieba" explicitly.
        let effective_tokenizer = if tokenizer_name.is_empty() {
            "default"
        } else {
            tokenizer_name
        };
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(effective_tokenizer)
                .set_index_option(index_option),
        );
        let text_field = schema_builder.add_text_field(PAIMON_TEXT_FIELD_NAME, text_options);
        let schema = schema_builder.build();

        let tmpdir = tempfile::Builder::new()
            .prefix("paimon-tantivy-")
            .tempdir()
            .map_err(|e| format!("create tmp dir: {e}"))?;

        let index = Index::create_in_dir(tmpdir.path(), schema)
            .map_err(|e| format!("create tantivy index: {e}"))?;
        // When caller picks "paimon_jieba" we construct + register the jieba
        // tokenizer. For any tantivy built-in name ("default", "whitespace",
        // "raw", "en_stem", ...) tantivy's TokenizerManager already has it
        // registered via `TokenizerManager::default()`; no-op here. This lets
        // paimon-cpp emit archives cross-readable by paimon-java's default
        // TEXT tokenizer path.
        if effective_tokenizer == PAIMON_TOKENIZER_NAME {
            let tokenizer = PaimonJiebaTokenizer::new(dict_dir, mode, with_position)
                .map_err(|e| format!("create tokenizer: {e}"))?;
            index
                .tokenizers()
                .register(PAIMON_TOKENIZER_NAME, tokenizer);
        }

        // Default multi-threaded writer (schema stores row_id explicitly so
        // we no longer need single-threaded ordering invariants). tantivy will
        // use min(num_cpus, MAX_NUM_THREAD=8) workers, splitting heap budget.
        let writer: IndexWriter = index
            .writer(WRITER_HEAP_SIZE)
            .map_err(|e| format!("create index writer: {e}"))?;

        Ok(Self {
            tmpdir,
            row_id_field,
            text_field,
            index,
            writer: Some(writer),
            row_count: 0,
        })
    }

    pub fn add(&mut self, row_id: u64, text: &str) -> Result<(), String> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| "writer already finished".to_string())?;
        let document: TantivyDocument = doc!(
            self.row_id_field => row_id,
            self.text_field => text,
        );
        writer
            .add_document(document)
            .map_err(|e| format!("add document: {e}"))?;
        self.row_count += 1;
        Ok(())
    }

    /// Commit + force-merge + GC on-disk index. Extracted from `finish_*`
    /// so both streaming and test paths can share it.
    fn commit_and_merge(&mut self) -> Result<(), String> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| "writer already finished".to_string())?;
        writer.commit().map_err(|e| format!("commit: {e}"))?;

        let segment_metas = self
            .index
            .searchable_segment_metas()
            .map_err(|e| format!("list segments: {e}"))?;
        if segment_metas.len() > 1 {
            let segment_ids: Vec<_> = segment_metas.iter().map(|m| m.id()).collect();
            writer
                .merge(&segment_ids)
                .wait()
                .map_err(|e| format!("merge: {e}"))?;
        }
        writer
            .garbage_collect_files()
            .wait()
            .map_err(|e| format!("garbage_collect_files: {e}"))?;
        writer
            .wait_merging_threads()
            .map_err(|e| format!("wait_merging_threads: {e}"))?;
        Ok(())
    }

    /// Streaming finish (production path): commit + force-merge + push archive
    /// bytes through the FFI callback in fixed-size chunks (see
    /// WRITER_STREAM_BUFFER_SIZE). Peak RAM independent of archive size — one
    /// heap buffer + a few KB metadata.
    pub fn finish_streaming(
        &mut self,
        cb: &PaimonWriteCallbacks,
    ) -> Result<i64, String> {
        self.commit_and_merge()?;
        let ctx = cb.ctx;
        let write_fn = cb.write;
        pack_index_dir_stream(self.tmpdir.path(), |bytes| {
            // Calling extern "C" fn pointer is safe; C++ side owns ctx validity.
            let rc = (write_fn)(ctx, bytes.as_ptr(), bytes.len());
            if rc != 0 {
                return Err(format!("write callback rc={rc} len={}", bytes.len()));
            }
            Ok(())
        })?;
        Ok(self.row_count)
    }

    /// Test-only convenience: collect streaming output into a `Vec<u8>`.
    /// Rust unit tests / integration tests use this; production path is
    /// `finish_streaming`.
    #[cfg(test)]
    pub(crate) fn finish(&mut self) -> Result<(i64, Vec<u8>), String> {
        self.commit_and_merge()?;
        let mut out: Vec<u8> = Vec::new();
        pack_index_dir_stream(self.tmpdir.path(), |bytes| {
            out.extend_from_slice(bytes);
            Ok(())
        })?;
        Ok((self.row_count, out))
    }

    #[cfg(test)]
    pub(crate) fn tmpdir_path(&self) -> &Path {
        self.tmpdir.path()
    }
}

// =========================================================================
// Streaming pack
// =========================================================================

/// Streaming pack buffer size. 1MB matches the buffer size data-lake storage
/// backends (e.g. Pangu) use for good throughput, still far below any archive
/// size we care about. Heap-allocated (see pack_index_dir_stream), so the size
/// does not affect stack usage.
const WRITER_STREAM_BUFFER_SIZE: usize = 1024 * 1024;

/// Callback table passed from C++ for streaming writer output.
///
/// `ctx` is an opaque pointer to C++'s `WriteCtx` (holding a `paimon::OutputStream`).
/// `write` is called in-order by Rust (not concurrently) to push bytes.
#[repr(C)]
pub struct PaimonWriteCallbacks {
    pub ctx: *mut c_void,
    /// Returns 0 on success, non-zero to signal C++ side error (Rust aborts pack).
    pub write: extern "C" fn(ctx: *mut c_void, data: *const u8, len: usize) -> i32,
}

/// Walk tempdir + pack into the Java-compatible archive format, pushing each
/// chunk through `write_fn`. Peak RAM = one WRITER_STREAM_BUFFER_SIZE heap
/// buffer + a few KB of entry metadata (name + PathBuf + u64 length). Mirrors
/// Java `TantivyFullTextGlobalIndexWriter.packIndex` but with a bigger buffer.
///
/// Archive format (BE, no version): `[i32 file_count | (i32 name_len, name,
/// i64 file_len, file_bytes)*]`. Files sorted alphabetically for deterministic
/// output; `.`-prefixed (lock) files and non-regular entries skipped.
fn pack_index_dir_stream<F>(dir: &Path, mut write_fn: F) -> Result<(), String>
where
    F: FnMut(&[u8]) -> Result<(), String>,
{
    let entries = collect_dir_entries(dir)?;

    // Header: BE i32 file_count
    write_fn(&(entries.len() as i32).to_be_bytes())?;

    let mut buf = vec![0u8; WRITER_STREAM_BUFFER_SIZE];
    for (name, path, file_len) in &entries {
        // Per-entry header: name_len, name, data_len
        write_fn(&(name.len() as i32).to_be_bytes())?;
        write_fn(name.as_bytes())?;
        write_fn(&(*file_len as i64).to_be_bytes())?;

        // Payload: fixed-size buffer loop
        let mut f = File::open(path)
            .map_err(|e| format!("open {}: {e}", path.display()))?;
        let mut pushed: u64 = 0;
        loop {
            let n = f
                .read(&mut buf)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            if n == 0 {
                break;
            }
            write_fn(&buf[..n])?;
            pushed += n as u64;
        }
        if pushed != *file_len {
            return Err(format!(
                "file {} changed size during packing: header said {}, streamed {}",
                name, file_len, pushed
            ));
        }
    }
    Ok(())
}

/// Enumerate the tempdir: sorted (name, path, len) for regular non-`.lock` files.
fn collect_dir_entries(dir: &Path) -> Result<Vec<(String, PathBuf, u64)>, String> {
    let mut entries: Vec<(String, PathBuf, u64)> = Vec::new();
    let read_dir =
        std::fs::read_dir(dir).map_err(|e| format!("read tmp dir {}: {e}", dir.display()))?;
    for entry_res in read_dir {
        let entry = entry_res.map_err(|e| format!("read entry: {e}"))?;
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        if name.starts_with('.') {
            continue;
        }
        let ft = entry
            .file_type()
            .map_err(|e| format!("file_type for {}: {e}", entry.path().display()))?;
        if !ft.is_file() {
            continue;
        }
        let len = entry
            .metadata()
            .map_err(|e| format!("metadata for {}: {e}", entry.path().display()))?
            .len();
        entries.push((name, entry.path(), len));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
}

// ============================ FFI surface ============================

/// Create a writer handle on a private tmp dir.
///
/// SAFETY: all C-string args must be NUL-terminated UTF-8; `out` non-null.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_writer_new(
    field_name_cstr: *const c_char,
    mode_cstr: *const c_char,
    with_position: bool,
    dict_dir_cstr: *const c_char,
    tokenizer_cstr: *const c_char,
    out: *mut *mut PaimonTantivyWriter,
) -> PaimonTantivyStatus {
    if field_name_cstr.is_null()
        || mode_cstr.is_null()
        || dict_dir_cstr.is_null()
        || tokenizer_cstr.is_null()
        || out.is_null()
    {
        set_last_error("paimon_tantivy_writer_new: null argument");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let field_name = match unsafe { CStr::from_ptr(field_name_cstr) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("field_name not utf-8: {e}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    let mode_str = match unsafe { CStr::from_ptr(mode_cstr) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("mode not utf-8: {e}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    let dict_dir = match unsafe { CStr::from_ptr(dict_dir_cstr) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("dict_dir not utf-8: {e}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    let tokenizer_name = match unsafe { CStr::from_ptr(tokenizer_cstr) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("tokenizer not utf-8: {e}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    let mode = match TokenizeMode::parse(mode_str) {
        Some(m) => m,
        None => {
            set_last_error(format!(
                "unknown tokenize mode {mode_str:?}; expected one of mp/hmm/mix/full/query"
            ));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    match PaimonTantivyWriter::new(
        field_name,
        mode,
        with_position,
        Path::new(dict_dir),
        tokenizer_name,
    ) {
        Ok(w) => {
            unsafe { *out = into_handle(w) };
            PaimonTantivyStatus::Ok
        }
        Err(e) => {
            // hmm-mode rejection bubbles through tokenizer construction.
            let unsupported = e.contains("'hmm' is not supported");
            set_last_error(e);
            if unsupported {
                PaimonTantivyStatus::Unsupported
            } else {
                PaimonTantivyStatus::InternalError
            }
        }
    }
}

/// Add a single document. `text` need not be NUL-terminated; treat as a slice
/// of `text_len` UTF-8 bytes. Empty text (len=0) inserts an empty-text doc.
/// `row_id` is the caller-supplied paimon row id (u64), stored in a fast field
/// for retrieval by the reader.
///
/// SAFETY: `writer` must be a live handle from `writer_new`.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_writer_add(
    writer: *mut PaimonTantivyWriter,
    row_id: u64,
    text: *const c_char,
    text_len: usize,
) -> PaimonTantivyStatus {
    let Some(w) = (unsafe { borrow_handle_mut::<PaimonTantivyWriter>(writer) }) else {
        set_last_error("paimon_tantivy_writer_add: null writer handle");
        return PaimonTantivyStatus::InvalidArgument;
    };
    if text.is_null() && text_len != 0 {
        set_last_error("text is null but len > 0");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let text_str = if text_len == 0 {
        ""
    } else {
        let slice = unsafe { std::slice::from_raw_parts(text as *const u8, text_len) };
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("text not utf-8: {e}"));
                return PaimonTantivyStatus::InvalidArgument;
            }
        }
    };
    match w.add(row_id, text_str) {
        Ok(()) => PaimonTantivyStatus::Ok,
        Err(e) => {
            set_last_error(e);
            PaimonTantivyStatus::InternalError
        }
    }
}

/// Commit + force-merge + stream archive bytes through `callbacks.write` in
/// fixed-size chunks (see WRITER_STREAM_BUFFER_SIZE). May only be called once
/// per writer; subsequent calls return InvalidArgument with
/// last_error="writer already finished". Peak Rust RAM ≈ one buffer + entry
/// metadata (independent of archive size).
///
/// The callback is invoked **serially** (not concurrently) within this call;
/// C++ side can write directly to paimon OutputStream without locking.
///
/// SAFETY: `writer` must be a live handle; `out_row_count` non-null.
/// `callbacks.write` / `callbacks.ctx` must remain valid for the duration of
/// the call (callback is consumed in-place, not retained).
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_writer_finish_streaming(
    writer: *mut PaimonTantivyWriter,
    callbacks: PaimonWriteCallbacks,
    out_row_count: *mut i64,
) -> PaimonTantivyStatus {
    if out_row_count.is_null() {
        set_last_error("paimon_tantivy_writer_finish_streaming: null out_row_count");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let Some(w) = (unsafe { borrow_handle_mut::<PaimonTantivyWriter>(writer) }) else {
        set_last_error("paimon_tantivy_writer_finish_streaming: null writer handle");
        return PaimonTantivyStatus::InvalidArgument;
    };
    match w.finish_streaming(&callbacks) {
        Ok(rows) => {
            unsafe { *out_row_count = rows };
            PaimonTantivyStatus::Ok
        }
        Err(e) => {
            let already_finished = e == "writer already finished";
            let io_err = e.starts_with("write callback rc=")
                || e.starts_with("open ")
                || e.starts_with("read ");
            set_last_error(e);
            if already_finished {
                PaimonTantivyStatus::InvalidArgument
            } else if io_err {
                PaimonTantivyStatus::IoError
            } else {
                PaimonTantivyStatus::InternalError
            }
        }
    }
}

/// Destroy a writer handle. Safe on null. Tmp dir is removed via Drop.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_writer_free(writer: *mut PaimonTantivyWriter) {
    unsafe { free_handle(writer) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    /// Test dict dir for jieba; defaults to a non-existent path so jieba-rs uses
    /// its built-in dict (which is enough for these smoke tests).
    fn dict_dir_from_env() -> std::path::PathBuf {
        std::env::var("PAIMON_JIEBA_DICT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/nonexistent-dict"))
    }

    #[test]
    fn empty_field_name_rejected() {
        let err = PaimonTantivyWriter::new("", TokenizeMode::Mix, true, Path::new("/tmp/nx"), "paimon_jieba")
            .err()
            .unwrap();
        assert!(err.contains("field_name"), "got: {err}");
    }

    #[test]
    fn hmm_mode_rejected() {
        let err =
            PaimonTantivyWriter::new("f0", TokenizeMode::Hmm, true, Path::new("/tmp/nx"), "paimon_jieba")
                .err()
                .unwrap();
        assert!(err.contains("'hmm' is not supported"), "got: {err}");
    }

    #[test]
    fn create_add_finish_roundtrip() {
        let mut w =
            PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba").unwrap();
        w.add(0, "hello world").unwrap();
        w.add(1, "中国人民").unwrap();
        w.add(2, "").unwrap(); // empty doc
        let (rows, bytes) = w.finish().unwrap();
        assert_eq!(rows, 3);
        assert!(bytes.len() > 4);

        // Validate header (Java-compatible: BE int32 file_count, no version)
        let file_count = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        assert!(file_count > 0, "expected >0 packed files");

        // Walk entries (BE)
        let mut off: usize = 4;
        let mut names = Vec::new();
        for _ in 0..file_count {
            let nlen = i32::from_be_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let name = std::str::from_utf8(&bytes[off..off + nlen]).unwrap().to_owned();
            off += nlen;
            let flen = i64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()) as usize;
            off += 8;
            assert!(off + flen <= bytes.len(), "file {name} extends past buffer");
            off += flen;
            names.push(name);
        }
        assert_eq!(off, bytes.len(), "trailing bytes after pack");
        // tantivy must produce at least meta.json
        assert!(names.iter().any(|n| n == "meta.json"), "names={names:?}");
    }

    #[test]
    fn schema_field_names_are_fixed() {
        // Schema must be `row_id` (u64) + `text` (TEXT) regardless of caller's
        // field_name argument — matches paimon-java for cross-readability.
        let w =
            PaimonTantivyWriter::new("ignored_name", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba")
                .unwrap();
        let schema = w.index.schema();
        assert!(schema.get_field(PAIMON_ROW_ID_FIELD_NAME).is_ok(),
                "schema must have row_id field");
        assert!(schema.get_field(PAIMON_TEXT_FIELD_NAME).is_ok(),
                "schema must have text field");
        // Caller-supplied name must NOT appear
        assert!(schema.get_field("ignored_name").is_err(),
                "caller-supplied field_name must be ignored");
    }

    #[test]
    fn archive_uses_big_endian_no_version_header() {
        // Strong guard: header must be BE int32 file_count, NOT LE int32
        // version=1 + LE int32 file_count. Any regression to LE/version-header
        // would silently break paimon-java cross-read.
        let mut w =
            PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba").unwrap();
        w.add(0, "hello").unwrap();
        let (_, bytes) = w.finish().unwrap();
        let header_be = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let header_le = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
        // BE file_count is small (single-segment force-merge: ~6-7 files)
        assert!(header_be > 0 && header_be < 100,
                "expected sensible BE file_count, got BE={header_be} LE={header_le}");
        // LE-decoded header would be a huge number (e.g. 0x06000000), ensuring
        // we did NOT regress to the old LE+version layout.
        assert_ne!(header_be, header_le, "buffer must be BE-encoded");
    }

    #[test]
    fn multi_thread_writer_default() {
        // Schema stores row_id explicitly so we no longer enforce
        // single-threaded writer. Just verify many docs across threads land
        // correctly and force-merge collapses to a single segment.
        let mut w =
            PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba").unwrap();
        for i in 0..200u64 {
            w.add(i, &format!("row {i} apple banana")).unwrap();
        }
        let (rows, bytes) = w.finish().unwrap();
        assert_eq!(rows, 200);
        assert!(bytes.len() > 4);
        // After force-merge there must be exactly one meta.json + segment files.
        let file_count = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        assert!(file_count >= 2, "force-merged single segment needs ≥ 2 files (meta + segment), got {file_count}");
    }

    #[test]
    fn finish_twice_errors() {
        let mut w =
            PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba").unwrap();
        w.add(0, "hi").unwrap();
        let _ = w.finish().unwrap();
        let err = w.finish().err().unwrap();
        assert!(err.contains("already finished"), "got: {err}");
    }

    /// Mock collector for FFI streaming tests: push bytes into a Box<Vec<u8>>
    /// pointed to by `ctx`. (No Arc / atomic needed — test is single-threaded.)
    extern "C" fn mock_write_collect(ctx: *mut c_void, data: *const u8, len: usize) -> i32 {
        let vec = unsafe { &mut *(ctx as *mut Vec<u8>) };
        let slice = unsafe { std::slice::from_raw_parts(data, len) };
        vec.extend_from_slice(slice);
        0
    }

    /// Mock that counts the largest single `write` call — sanity check that
    /// Rust streams in chunks bounded by the buffer (+ small header fields).
    extern "C" fn mock_write_max_chunk(
        ctx: *mut c_void,
        _data: *const u8,
        len: usize,
    ) -> i32 {
        let max = unsafe { &mut *(ctx as *mut usize) };
        if len > *max {
            *max = len;
        }
        0
    }

    #[test]
    fn ffi_full_path_streaming() {
        unsafe {
            let field = CString::new("f0").unwrap();
            let mode = CString::new("mix").unwrap();
            let dict = CString::new(dict_dir_from_env().to_str().unwrap()).unwrap();
            let tokenizer = CString::new("paimon_jieba").unwrap();
            let mut handle: *mut PaimonTantivyWriter = std::ptr::null_mut();
            let st = paimon_tantivy_writer_new(
                field.as_ptr(),
                mode.as_ptr(),
                true,
                dict.as_ptr(),
                tokenizer.as_ptr(),
                &mut handle,
            );
            assert_eq!(st, PaimonTantivyStatus::Ok);
            assert!(!handle.is_null());

            let txt = "hello world";
            let st =
                paimon_tantivy_writer_add(handle, 42u64, txt.as_ptr() as *const c_char, txt.len());
            assert_eq!(st, PaimonTantivyStatus::Ok);

            // Streaming finish: collect bytes into a Vec<u8> via FFI callback
            let mut out: Vec<u8> = Vec::new();
            let cb = PaimonWriteCallbacks {
                ctx: &mut out as *mut _ as *mut c_void,
                write: mock_write_collect,
            };
            let mut rows: i64 = 0;
            let st = paimon_tantivy_writer_finish_streaming(handle, cb, &mut rows);
            assert_eq!(st, PaimonTantivyStatus::Ok);
            assert_eq!(rows, 1);
            // BE file_count at byte 0,> 0
            let file_count = i32::from_be_bytes(out[0..4].try_into().unwrap());
            assert!(file_count > 0);

            // double finish must error
            let mut out2: Vec<u8> = Vec::new();
            let cb2 = PaimonWriteCallbacks {
                ctx: &mut out2 as *mut _ as *mut c_void,
                write: mock_write_collect,
            };
            let mut rows2: i64 = 0;
            let st = paimon_tantivy_writer_finish_streaming(handle, cb2, &mut rows2);
            assert_eq!(st, PaimonTantivyStatus::InvalidArgument);

            paimon_tantivy_writer_free(handle);
        }
    }

    #[test]
    fn streaming_chunk_size_bounded_by_buffer() {
        // After force-merge, a 200-doc index still streams in chunks bounded by
        // WRITER_STREAM_BUFFER_SIZE (payload) or small header-field chunks.
        let mut w =
            PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba").unwrap();
        for i in 0..200u64 {
            w.add(i, &format!("row {i} apple banana")).unwrap();
        }
        let mut max_chunk: usize = 0;
        let cb = PaimonWriteCallbacks {
            ctx: &mut max_chunk as *mut _ as *mut c_void,
            write: mock_write_max_chunk,
        };
        let rows = w.finish_streaming(&cb).unwrap();
        assert_eq!(rows, 200);
        assert!(
            max_chunk <= WRITER_STREAM_BUFFER_SIZE,
            "streaming chunk size {} exceeded buffer {}",
            max_chunk,
            WRITER_STREAM_BUFFER_SIZE
        );
    }

    #[test]
    fn streaming_write_callback_error_propagates() {
        extern "C" fn always_fail(_ctx: *mut c_void, _data: *const u8, _len: usize) -> i32 {
            7
        }
        let mut w =
            PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir_from_env(), "paimon_jieba").unwrap();
        w.add(0, "hello").unwrap();
        let cb = PaimonWriteCallbacks {
            ctx: std::ptr::null_mut(),
            write: always_fail,
        };
        let err = w.finish_streaming(&cb).unwrap_err();
        assert!(err.contains("write callback rc=7"), "got: {err}");
    }

    #[test]
    fn ffi_null_writer_invalid() {
        unsafe {
            let txt = "x";
            let st = paimon_tantivy_writer_add(
                std::ptr::null_mut(),
                0u64,
                txt.as_ptr() as *const c_char,
                txt.len(),
            );
            assert_eq!(st, PaimonTantivyStatus::InvalidArgument);
        }
    }
}
