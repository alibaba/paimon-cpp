//! PaimonJiebaTokenizer: tantivy Tokenizer impl wrapping jieba-rs.
//!
//! Contract:
//! - Behavior-equivalent with `JiebaAnalyzer` in src/paimon/global_index/lucene/
//! - 5 modes: mp / hmm / mix / full / query
//!   - `hmm` is Unsupported (jieba-rs has no standalone HMM entry point)
//!   - `mp` accepts cut(hmm=false) but does not replicate cppjieba's
//!     max_word_len truncation
//! - Normalize: skip pure whitespace, skip stop_words, lowercase ASCII-only tokens
//! - Token offsets: byte offsets into the original UTF-8 string
//! - `with_position=false`: all tokens emitted at `position=0` (disables PhraseQuery)
//! - Custom dict dir: loads `jieba.dict.utf8` (+optional `user.dict.utf8`) from
//!   `$PAIMON_JIEBA_DICT_DIR`; stop_words.utf8 loaded if present

use std::collections::HashSet;
use std::ffi::{c_char, CStr};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use jieba_rs::Jieba;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use crate::buffer::PaimonTantivyBuffer;
use crate::error::{set_last_error, PaimonTantivyStatus};
use crate::handle::{borrow_handle, free_handle, into_handle};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizeMode {
    Mp,
    Hmm,
    Mix,
    Full,
    Query,
}

impl TokenizeMode {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "mp" => Some(Self::Mp),
            "hmm" => Some(Self::Hmm),
            "mix" => Some(Self::Mix),
            "full" => Some(Self::Full),
            "query" => Some(Self::Query),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct PaimonJiebaTokenizer {
    jieba: Arc<Jieba>,
    mode: TokenizeMode,
    with_position: bool,
    stop_words: Arc<HashSet<String>>,
}

impl PaimonJiebaTokenizer {
    pub fn new(
        dict_dir: &Path,
        mode: TokenizeMode,
        with_position: bool,
    ) -> Result<Self, String> {
        if mode == TokenizeMode::Hmm {
            return Err(
                "tokenize mode 'hmm' is not supported (jieba-rs does not expose standalone HMM)"
                    .into(),
            );
        }
        let jieba = load_jieba(dict_dir)?;
        let stop_words = load_stop_words(dict_dir);
        Ok(Self {
            jieba: Arc::new(jieba),
            mode,
            with_position,
            stop_words: Arc::new(stop_words),
        })
    }

    /// Directly tokenize, returning a Vec of (offset_start, offset_end, text) tuples.
    /// Used both by the tantivy Tokenizer impl and the standalone `tokenize` FFI.
    pub fn tokenize_raw(&self, text: &str) -> Vec<(usize, usize, String)> {
        // Use jieba-rs's cut variants which return Vec<&'a str>; compute byte offsets
        // via pointer arithmetic (each &str is a slice of the original).
        let cuts: Vec<&str> = match self.mode {
            TokenizeMode::Mp => self.jieba.cut(text, false),
            TokenizeMode::Hmm => Vec::new(), // unreachable (caught in new())
            TokenizeMode::Mix => self.jieba.cut(text, true),
            TokenizeMode::Full => self.jieba.cut_all(text),
            TokenizeMode::Query => self.jieba.cut_for_search(text, true),
        };

        let text_start = text.as_ptr() as usize;
        let mut out = Vec::with_capacity(cuts.len());
        for piece in cuts {
            // skip pure whitespace
            if piece.chars().all(char::is_whitespace) {
                continue;
            }
            // skip stop words (compare original case)
            if self.stop_words.contains(piece) {
                continue;
            }
            // offset calc
            let start = piece.as_ptr() as usize - text_start;
            let end = start + piece.len();
            // lowercase only if pure ASCII alphanumeric (match cppjieba Normalize behavior)
            let token_text = if is_ascii_alnum(piece) {
                piece.to_ascii_lowercase()
            } else {
                piece.to_string()
            };
            out.push((start, end, token_text));
        }
        out
    }
}

fn is_ascii_alnum(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric())
}

fn load_jieba(dict_dir: &Path) -> Result<Jieba, String> {
    let main_dict = dict_dir.join("jieba.dict.utf8");
    let mut jieba = if main_dict.exists() {
        let file = File::open(&main_dict)
            .map_err(|e| format!("open {}: {e}", main_dict.display()))?;
        let mut rdr = BufReader::new(file);
        Jieba::with_dict(&mut rdr).map_err(|e| format!("load jieba dict: {e:?}"))?
    } else {
        // No custom dict; use jieba-rs builtin
        Jieba::new()
    };
    // Optional user dict. cppjieba's user.dict.utf8 is lenient: lines are
    // `word [freq] [tag]` where freq can be omitted (e.g. `<word> nz`), but
    // jieba-rs's load_dict strictly requires `word freq [tag]` and fails if
    // freq is not an integer. We parse line-by-line with `add_word` to stay
    // compatible.
    let user_dict = dict_dir.join("user.dict.utf8");
    if user_dict.exists() {
        let file = File::open(&user_dict)
            .map_err(|e| format!("open {}: {e}", user_dict.display()))?;
        for (n, line_res) in BufReader::new(file).lines().enumerate() {
            let line = match line_res {
                Ok(l) => l,
                Err(_) => continue, // skip unreadable lines
            };
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut it = trimmed.split_whitespace();
            let word = it.next().unwrap(); // non-empty guaranteed
            let next = it.next();
            let freq = next.and_then(|s| s.parse::<usize>().ok());
            let tag = match (freq, next) {
                (Some(_), _) => it.next(),       // <word> <freq> [tag]
                (None, tok) => tok,              // <word> <tag>  (no freq)
            };
            // `add_word` returns the assigned frequency; ignore it. For lines
            // with bogus content we silently keep going, matching cppjieba's
            // tolerant behavior.
            let _ = jieba.add_word(word, freq, tag);
            let _ = n; // keep for potential debug
        }
    }
    Ok(jieba)
}

fn load_stop_words(dict_dir: &Path) -> HashSet<String> {
    let path = dict_dir.join("stop_words.utf8");
    let mut out = HashSet::new();
    if let Ok(f) = File::open(&path) {
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            let w = line.trim();
            if !w.is_empty() {
                out.insert(w.to_owned());
            }
        }
    }
    out
}

// ----------------- tantivy Tokenizer integration -----------------

pub struct PaimonJiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for PaimonJiebaTokenStream {
    fn advance(&mut self) -> bool {
        self.index += 1;
        self.index <= self.tokens.len()
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

impl Tokenizer for PaimonJiebaTokenizer {
    type TokenStream<'a> = PaimonJiebaTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let raw = self.tokenize_raw(text);
        let tokens: Vec<Token> = raw
            .into_iter()
            .enumerate()
            .map(|(i, (s, e, t))| Token {
                offset_from: s,
                offset_to: e,
                position: if self.with_position { i } else { 0 },
                text: t,
                position_length: 1,
            })
            .collect();
        PaimonJiebaTokenStream { tokens, index: 0 }
    }
}

// ----------------- FFI surface -----------------

/// Create a tokenizer handle. Returns OK and writes *out on success; returns
/// status and sets last_error on failure.
///
/// SAFETY: `mode_cstr` and `dict_dir_cstr` must be NUL-terminated UTF-8;
/// `out` must be a valid non-null pointer.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_tokenizer_new(
    mode_cstr: *const c_char,
    with_position: bool,
    dict_dir_cstr: *const c_char,
    out: *mut *mut PaimonJiebaTokenizer,
) -> PaimonTantivyStatus {
    if mode_cstr.is_null() || dict_dir_cstr.is_null() || out.is_null() {
        set_last_error("paimon_tantivy_tokenizer_new: null argument");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let mode_s = match unsafe { CStr::from_ptr(mode_cstr) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("mode not utf-8: {e}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    let dict_s = match unsafe { CStr::from_ptr(dict_dir_cstr) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("dict_dir not utf-8: {e}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    let mode = match TokenizeMode::parse(mode_s) {
        Some(m) => m,
        None => {
            set_last_error(format!(
                "unknown tokenize mode {mode_s:?}; expected one of mp/hmm/mix/full/query"
            ));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    match PaimonJiebaTokenizer::new(Path::new(dict_s), mode, with_position) {
        Ok(t) => {
            unsafe { *out = into_handle(t) };
            PaimonTantivyStatus::Ok
        }
        Err(e) => {
            let is_hmm_unsupported = e.contains("'hmm' is not supported");
            set_last_error(e);
            if is_hmm_unsupported {
                PaimonTantivyStatus::Unsupported
            } else {
                PaimonTantivyStatus::TokenizerError
            }
        }
    }
}

/// Free a tokenizer handle. Safe on null.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_tokenizer_free(tok: *mut PaimonJiebaTokenizer) {
    unsafe { free_handle(tok) };
}

/// Tokenize a string and return a newline-delimited list of tokens as bytes.
/// Used for golden-sample tests (easy to diff from C++).
///
/// Output format:
///   `<offset_from>\t<offset_to>\t<position>\t<text>\n` for each token.
///
/// SAFETY: `tok` must be a valid handle; `text` must point to `text_len` UTF-8 bytes;
/// `out` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_tokenizer_tokenize(
    tok: *const PaimonJiebaTokenizer,
    text: *const c_char,
    text_len: usize,
    out: *mut PaimonTantivyBuffer,
) -> PaimonTantivyStatus {
    if out.is_null() {
        set_last_error("paimon_tantivy_tokenizer_tokenize: out is null");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let Some(tokenizer) = (unsafe { borrow_handle::<PaimonJiebaTokenizer>(tok) }) else {
        set_last_error("paimon_tantivy_tokenizer_tokenize: null tokenizer handle");
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
    let raw = tokenizer.tokenize_raw(text_str);
    let mut buf = String::new();
    for (i, (s, e, t)) in raw.iter().enumerate() {
        let pos = if tokenizer.with_position { i } else { 0 };
        buf.push_str(&format!("{s}\t{e}\t{pos}\t{t}\n"));
    }
    let bytes = buf.into_bytes();
    unsafe { *out = PaimonTantivyBuffer::from_vec(bytes) };
    PaimonTantivyStatus::Ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn dict_dir_from_env() -> std::path::PathBuf {
        std::env::var("PAIMON_JIEBA_DICT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/nonexistent-dict"))
    }

    #[test]
    fn mode_parse() {
        for (s, m) in [
            ("mp", TokenizeMode::Mp),
            ("hmm", TokenizeMode::Hmm),
            ("mix", TokenizeMode::Mix),
            ("full", TokenizeMode::Full),
            ("query", TokenizeMode::Query),
        ] {
            assert_eq!(TokenizeMode::parse(s), Some(m));
        }
        assert!(TokenizeMode::parse("bogus").is_none());
    }

    #[test]
    fn hmm_mode_returns_unsupported() {
        let tok = PaimonJiebaTokenizer::new(
            &dict_dir_from_env(),
            TokenizeMode::Hmm,
            true,
        );
        match tok {
            Err(e) => assert!(e.contains("'hmm' is not supported"), "got: {e}"),
            Ok(_) => panic!("expected Err"),
        }
    }

    #[test]
    fn tokenize_mix_default_dict_smoke() {
        // If no custom dict dir, jieba-rs builtin is used.
        let t = PaimonJiebaTokenizer::new(Path::new("/tmp/nonexistent-dict"), TokenizeMode::Mix, true)
            .unwrap();
        let raw = t.tokenize_raw("他来到了网易杭研大厦");
        let texts: Vec<&str> = raw.iter().map(|(_, _, s)| s.as_str()).collect();
        assert!(texts.contains(&"网易"));
        assert!(texts.contains(&"大厦"));
    }

    #[test]
    fn ascii_alnum_is_lowercased() {
        let t = PaimonJiebaTokenizer::new(Path::new("/tmp/nx"), TokenizeMode::Mix, true).unwrap();
        let raw = t.tokenize_raw("Hello World 中国");
        let texts: Vec<&str> = raw.iter().map(|(_, _, s)| s.as_str()).collect();
        assert!(texts.contains(&"hello"));
        assert!(texts.contains(&"world"));
        assert!(texts.contains(&"中国"));
    }

    #[test]
    fn with_position_false_emits_zero_position() {
        let t = PaimonJiebaTokenizer::new(Path::new("/tmp/nx"), TokenizeMode::Mix, false).unwrap();
        let raw = t.tokenize_raw("中国人");
        // Can't check position on raw tuples; check via tantivy Token stream:
        let mut t2 = t.clone();
        let mut stream = <PaimonJiebaTokenizer as Tokenizer>::token_stream(&mut t2, "中国人");
        let mut positions = Vec::new();
        while stream.advance() {
            positions.push(stream.token().position);
        }
        assert!(!raw.is_empty());
        assert!(positions.iter().all(|&p| p == 0));
    }

    #[test]
    fn ffi_roundtrip() {
        let dict = dict_dir_from_env();
        let dict_str = dict.to_str().unwrap();
        let mode = CString::new("mix").unwrap();
        let dict_c = CString::new(dict_str).unwrap();
        let mut handle: *mut PaimonJiebaTokenizer = std::ptr::null_mut();
        unsafe {
            let st = paimon_tantivy_tokenizer_new(
                mode.as_ptr(),
                true,
                dict_c.as_ptr(),
                &mut handle,
            );
            assert_eq!(st, PaimonTantivyStatus::Ok);
            assert!(!handle.is_null());

            let input = "Hello 中国";
            let input_c = CString::new(input).unwrap();
            let mut buf = PaimonTantivyBuffer::empty();
            let st2 = paimon_tantivy_tokenizer_tokenize(
                handle,
                input_c.as_ptr(),
                input.len(),
                &mut buf,
            );
            assert_eq!(st2, PaimonTantivyStatus::Ok);
            assert!(buf.len > 0);
            crate::buffer::paimon_tantivy_buffer_free(&mut buf);
            paimon_tantivy_tokenizer_free(handle);
        }
    }
}
