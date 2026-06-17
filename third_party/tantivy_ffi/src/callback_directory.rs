//! PaimonCallbackDirectory: streaming tantivy `Directory` backed by C FFI
//! callbacks. Replaces the V1 `PaimonDirectory` (RamDirectory wrapper) with a
//! callback-driven design that mirrors Java paimon-tantivy-jni's `JniDirectory`.
//!
//! ## Why callback-based?
//!
//! Loading the entire archive (100MB+) into `RamDirectory` at reader
//! construction would give ~2x archive peak RAM and pay the whole download
//! cost up front even for small queries. This directory keeps just the
//! `HashMap<PathBuf, FileMeta>` layout and issues pread calls through the FFI
//! callback whenever tantivy asks for bytes — peak RAM is ~KB, startup is
//! ~header size.
//!
//! ## Concurrency
//!
//! `read_at` is serialized via `stream_mutex` (same as Java JniDir's
//! `stream_lock`). pread-style callbacks in principle allow concurrent reads,
//! but some `paimon::InputStream` subclasses (notably `JindoInputStream`)
//! have shared-state races, so we play it safe.

use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite,
    WatchCallback, WatchHandle, WritePtr,
};
use tantivy::HasLen;

// =========================================================================
// FFI types
// =========================================================================

/// pread-style callback table passed from C++ at reader construction.
///
/// `ctx` is an opaque pointer to C++'s `StreamCtx` (holding a
/// `paimon::InputStream`). Rust never dereferences it — only forwards it
/// into the callback functions. `release` is called exactly once when the
/// last `Arc<CallbackCtx>` is dropped.
#[repr(C)]
pub struct PaimonStreamCallbacks {
    pub ctx: *mut c_void,
    pub read_at:
        extern "C" fn(ctx: *mut c_void, offset: u64, len: usize, out_buf: *mut u8) -> i32,
    pub release: extern "C" fn(ctx: *mut c_void),
}

// =========================================================================
// Internal state
// =========================================================================

#[derive(Clone, Debug)]
struct FileMeta {
    offset: u64,
    length: u64,
}

/// RAII wrapper owning the FFI callbacks. On drop, invokes `release(ctx)`.
/// Shared across clones of `PaimonCallbackDirectory` via `Arc`.
struct CallbackCtx {
    callbacks: PaimonStreamCallbacks,
}

impl Drop for CallbackCtx {
    fn drop(&mut self) {
        // Calling an extern "C" fn pointer from safe Rust is legal; the
        // contract safety relies on the C++ side providing a valid ctx.
        (self.callbacks.release)(self.callbacks.ctx);
    }
}

// Safety: callbacks.ctx is treated as opaque; C++ owner is responsible for
// the ctx being usable across threads. Rust's stream_mutex serializes
// read_at calls, and release is only invoked once (when Arc refcount hits 0).
unsafe impl Send for CallbackCtx {}
unsafe impl Sync for CallbackCtx {}

// =========================================================================
// PaimonCallbackDirectory
// =========================================================================

#[derive(Clone)]
pub struct PaimonCallbackDirectory {
    /// name → (offset, length) in the stream. Immutable after construction.
    layout: Arc<HashMap<PathBuf, FileMeta>>,
    /// FFI callbacks + their ctx lifetime.
    ctx: Arc<CallbackCtx>,
    /// tantivy writes small atomic files (`.lock`, in some paths `meta.json`)
    /// via `atomic_write`; we keep them in memory instead of pushing back
    /// through C++ (read-only archive). Shared across clones.
    atomic_data: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    /// Serialize seek+read (mirrors Java JniDir's `stream_lock`) to guard
    /// against shared-state races in some InputStream subclasses.
    stream_mutex: Arc<Mutex<()>>,
}

impl fmt::Debug for PaimonCallbackDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaimonCallbackDirectory")
            .field("files", &self.layout.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl PaimonCallbackDirectory {
    /// Construct a new directory from the C++-parsed archive layout + callbacks.
    /// The ctx ownership transfers to this Directory; `release` is invoked on
    /// drop of the last clone.
    pub fn new(
        entries: Vec<(String, u64, u64)>,
        callbacks: PaimonStreamCallbacks,
    ) -> Self {
        let mut layout = HashMap::with_capacity(entries.len());
        for (name, offset, length) in entries {
            layout.insert(PathBuf::from(name), FileMeta { offset, length });
        }
        Self {
            layout: Arc::new(layout),
            ctx: Arc::new(CallbackCtx { callbacks }),
            atomic_data: Arc::new(Mutex::new(HashMap::new())),
            stream_mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Perform an FFI pread. Serialized via `stream_mutex`.
    fn pread(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let _guard = self.stream_mutex.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("stream_mutex poisoned: {e}"))
        })?;
        let mut buf = vec![0u8; len];
        // Calling extern "C" fn pointer — safe from Rust's POV (ABI is C);
        // the contract safety (ctx validity, buffer ownership) is on the C++ side.
        let rc =
            (self.ctx.callbacks.read_at)(self.ctx.callbacks.ctx, offset, len, buf.as_mut_ptr());
        if rc != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("pread callback rc={rc} offset={offset} len={len}"),
            ));
        }
        Ok(buf)
    }

    /// Sorted file names, for diagnostic / test use.
    #[cfg(test)]
    pub(crate) fn file_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .layout
            .keys()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

// =========================================================================
// FileHandle
// =========================================================================

#[derive(Clone)]
struct PaimonCallbackFileHandle {
    directory: PaimonCallbackDirectory,
    file_offset: u64,
    file_length: u64,
}

impl fmt::Debug for PaimonCallbackFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaimonCallbackFileHandle")
            .field("offset", &self.file_offset)
            .field("length", &self.file_length)
            .finish()
    }
}

impl HasLen for PaimonCallbackFileHandle {
    fn len(&self) -> usize {
        self.file_length as usize
    }
}

impl FileHandle for PaimonCallbackFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let start = self.file_offset + range.start as u64;
        let len = range.end - range.start;
        let data = self.directory.pread(start, len)?;
        Ok(OwnedBytes::new(data))
    }
}

// =========================================================================
// Directory trait (13 methods for tantivy 0.22)
// =========================================================================

impl Directory for PaimonCallbackDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let meta = self
            .layout
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(Arc::new(PaimonCallbackFileHandle {
            directory: self.clone(),
            file_offset: meta.offset,
            file_length: meta.length,
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let in_layout = self.layout.contains_key(path);
        let in_atomic = self.atomic_data.lock().unwrap().contains_key(path);
        Ok(in_layout || in_atomic)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(data) = self.atomic_data.lock().unwrap().get(path) {
            return Ok(data.clone());
        }
        let meta = self
            .layout
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        self.pread(meta.offset, meta.length as usize)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.atomic_data
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), data.to_vec());
        Ok(())
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        // read-only archive: ignore
        Ok(())
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        // tantivy needs this for lock files when opening an index; provide a
        // dummy in-memory writer (same trick as Java JniDirectory).
        let buf: Vec<u8> = Vec::new();
        Ok(io::BufWriter::new(Box::new(VecTerminatingWrite(buf))))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        // Read-only: no actual locking.
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

/// Throwaway writer for `open_write` — tantivy creates it for lock files but
/// the bytes never matter in a read-only archive.
struct VecTerminatingWrite(Vec<u8>);

impl io::Write for VecTerminatingWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for VecTerminatingWrite {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}

// =========================================================================
// Test support (pub(crate) — used by reader.rs tests too)
// =========================================================================

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock backend: an in-memory buffer serving pread requests. Counters
    /// expose behavior for test assertions (read count / release count).
    pub(crate) struct MockBackend {
        pub data: Vec<u8>,
        pub read_count: AtomicUsize,
        pub release_count: AtomicUsize,
    }

    extern "C" fn mock_read_at(
        ctx: *mut c_void,
        offset: u64,
        len: usize,
        out_buf: *mut u8,
    ) -> i32 {
        let backend = unsafe { &*(ctx as *const MockBackend) };
        backend.read_count.fetch_add(1, Ordering::SeqCst);
        let data = &backend.data;
        let end = (offset as usize).saturating_add(len);
        if end > data.len() {
            return 1; // out of range
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr().add(offset as usize), out_buf, len);
        }
        0
    }

    extern "C" fn mock_release(ctx: *mut c_void) {
        // Reclaim the strong ref that `Arc::into_raw` leaked at construction.
        let backend = unsafe { Arc::from_raw(ctx as *const MockBackend) };
        backend.release_count.fetch_add(1, Ordering::SeqCst);
        // `arc` drops here → decrement; test still holds its own clone.
    }

    /// Build a mock-backed directory for tests. Returns (dir, backend clone).
    /// The backend Arc is shared — drop the directory to trigger release.
    pub(crate) fn build_mock_directory(
        data: Vec<u8>,
        entries: Vec<(String, u64, u64)>,
    ) -> (PaimonCallbackDirectory, Arc<MockBackend>) {
        let backend = Arc::new(MockBackend {
            data,
            read_count: AtomicUsize::new(0),
            release_count: AtomicUsize::new(0),
        });
        let ctx_ptr = Arc::into_raw(backend.clone()) as *mut c_void;
        let cb = PaimonStreamCallbacks {
            ctx: ctx_ptr,
            read_at: mock_read_at,
            release: mock_release,
        };
        let dir = PaimonCallbackDirectory::new(entries, cb);
        (dir, backend)
    }

    /// Build mock callbacks (+ a backend clone) without wrapping them in a
    /// directory, for tests that drive the FFI entry points directly.
    pub(crate) fn make_mock_callbacks(data: Vec<u8>) -> (PaimonStreamCallbacks, Arc<MockBackend>) {
        let backend = Arc::new(MockBackend {
            data,
            read_count: AtomicUsize::new(0),
            release_count: AtomicUsize::new(0),
        });
        let ctx_ptr = Arc::into_raw(backend.clone()) as *mut c_void;
        let cb = PaimonStreamCallbacks {
            ctx: ctx_ptr,
            read_at: mock_read_at,
            release: mock_release,
        };
        (cb, backend)
    }

    /// Parse a packed archive blob (BE, no version header, matching
    /// `writer::pack_index_dir`) and build a mock-backed directory. Used by
    /// `reader.rs::tests` since writer.finish currently still returns a Vec<u8>.
    pub(crate) fn build_directory_from_archive(
        packed: Vec<u8>,
    ) -> (PaimonCallbackDirectory, Arc<MockBackend>) {
        let entries = parse_archive_header(&packed);
        build_mock_directory(packed, entries)
    }

    /// Parse the archive header — mirrors the layout that
    /// C++ `ArchiveLayout::Parse` produces in production.
    fn parse_archive_header(bytes: &[u8]) -> Vec<(String, u64, u64)> {
        let mut off = 0usize;
        let file_count = i32::from_be_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        let mut entries = Vec::with_capacity(file_count);
        for _ in 0..file_count {
            let nlen = i32::from_be_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let name =
                std::str::from_utf8(&bytes[off..off + nlen]).unwrap().to_owned();
            off += nlen;
            let flen = i64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()) as u64;
            off += 8;
            let data_offset = off as u64;
            entries.push((name, data_offset, flen));
            off += flen as usize;
        }
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn file_handle_reads_correct_bytes() {
        let data = b"hello world".to_vec();
        let entries = vec![("foo.txt".to_string(), 0, 11)];
        let (dir, _backend) = build_mock_directory(data, entries);

        let handle = dir.get_file_handle(Path::new("foo.txt")).unwrap();
        let bytes = handle.read_bytes(0..5).unwrap();
        assert_eq!(&bytes[..], b"hello");
        let bytes = handle.read_bytes(6..11).unwrap();
        assert_eq!(&bytes[..], b"world");
    }

    #[test]
    fn missing_file_returns_error() {
        let (dir, _backend) = build_mock_directory(vec![], vec![]);
        let err = dir.get_file_handle(Path::new("nonexistent")).unwrap_err();
        match err {
            OpenReadError::FileDoesNotExist(p) => {
                assert_eq!(p.to_string_lossy(), "nonexistent")
            }
            other => panic!("expected FileDoesNotExist, got {other:?}"),
        }
    }

    #[test]
    fn pread_out_of_range_propagates_error() {
        let data = b"short".to_vec();
        let entries = vec![("bad.txt".to_string(), 0, 100)]; // length exceeds data
        let (dir, _backend) = build_mock_directory(data, entries);
        let handle = dir.get_file_handle(Path::new("bad.txt")).unwrap();
        let err = handle.read_bytes(0..100).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn atomic_write_read_roundtrip_and_exists() {
        let (dir, _backend) = build_mock_directory(vec![], vec![]);
        dir.atomic_write(Path::new(".lock"), b"locked").unwrap();
        let data = dir.atomic_read(Path::new(".lock")).unwrap();
        assert_eq!(data, b"locked");
        assert!(dir.exists(Path::new(".lock")).unwrap());
        assert!(!dir.exists(Path::new("gone")).unwrap());
    }

    #[test]
    fn release_called_exactly_once_on_last_drop() {
        let entries = vec![("a".to_string(), 0, 5)];
        let (dir, backend) = build_mock_directory(b"hello".to_vec(), entries);
        assert_eq!(backend.release_count.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(dir);
        assert_eq!(backend.release_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn cloned_directory_shares_ctx_and_atomic_data() {
        let (dir, backend) = build_mock_directory(vec![], vec![]);
        let dir2 = dir.clone();
        dir.atomic_write(Path::new("x"), b"hello").unwrap();
        assert!(dir2.exists(Path::new("x")).unwrap()); // shared atomic_data
        drop(dir);
        assert_eq!(backend.release_count.load(std::sync::atomic::Ordering::SeqCst), 0); // ctx still held by dir2
        drop(dir2);
        assert_eq!(backend.release_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_pread_results_correct_under_stream_mutex() {
        use std::thread;

        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let entries = vec![("data".to_string(), 0, 1000)];
        let (dir, backend) = build_mock_directory(data.clone(), entries);
        let handle: Arc<dyn FileHandle> =
            dir.get_file_handle(Path::new("data")).unwrap();

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let h = handle.clone();
                let expected = data.clone();
                thread::spawn(move || {
                    for _ in 0..20 {
                        let bytes = h.read_bytes(100..200).unwrap();
                        assert_eq!(&bytes[..], &expected[100..200]);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(
            backend.read_count.load(std::sync::atomic::Ordering::SeqCst),
            8 * 20
        );
    }

    #[test]
    fn file_names_sorted() {
        let entries = vec![
            ("z.idx".to_string(), 0, 10),
            ("a.meta".to_string(), 10, 20),
            ("m.term".to_string(), 30, 5),
        ];
        let (dir, _backend) = build_mock_directory(vec![0u8; 100], entries);
        let names = dir.file_names();
        assert_eq!(names, vec!["a.meta", "m.term", "z.idx"]);
    }
}
