//! PaimonTantivyReader: query side of tantivy-fulltext.
//!
//! Constructs a tantivy Index from a packed-blob produced by writer.rs (via
//! PaimonDirectory), registers the same `paimon_jieba` tokenizer, and runs
//! one of 5 search types (mirrors `paimon::FullTextSearch::SearchType`):
//!
//!   1 MATCH_ALL — tokenize query, BooleanQuery (Must)
//!   2 MATCH_ANY — tokenize query, BooleanQuery (Should)
//!   3 PHRASE    — tokenize query, PhraseQuery
//!   4 PREFIX    — RegexQuery `<escaped>.*` (no tokenization, mirrors lucene-fts)
//!   5 WILDCARD  — RegexQuery from glob pattern (`*` → `.*`, `?` → `.`, others escaped)
//!
//! For paimon-java compatibility, row_id is stored as an explicit u64 field
//! (`fast` for O(1) retrieval). Reader translates tantivy DocAddress → row_id
//! via `fast_fields().u64("row_id").first(doc_id)` per segment.
//!
//! FFI return format (little-endian, **doc identifiers are u64 row_ids**):
//!   `[u8 has_scores | u64 count | u64 row_id[count] | optional f32 score[count]]`

use std::ffi::{c_char, CStr};
use std::path::Path;

use croaring::{Portable, Treemap};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::Column;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{DocAddress, DocId, Index, IndexReader, ReloadPolicy, Score, SegmentOrdinal,
               SegmentReader, Term};

use crate::buffer::PaimonTantivyBuffer;
use crate::callback_directory::{PaimonCallbackDirectory, PaimonStreamCallbacks};
use crate::error::{set_last_error, PaimonTantivyStatus};
use crate::handle::{borrow_handle_mut, free_handle, into_handle};
use crate::tokenizer::{PaimonJiebaTokenizer, TokenizeMode};
use crate::writer::{PAIMON_ROW_ID_FIELD_NAME, PAIMON_TEXT_FIELD_NAME, PAIMON_TOKENIZER_NAME};

/// Numeric encoding of `paimon::FullTextSearch::SearchType`. Kept in sync
/// with include/paimon/predicate/full_text_search.h.
#[repr(i32)]
#[derive(Clone, Copy, Debug)]
pub enum SearchType {
    MatchAll = 1,
    MatchAny = 2,
    Phrase = 3,
    Prefix = 4,
    Wildcard = 5,
}

impl SearchType {
    fn from_i32(v: i32) -> Option<Self> {
        match v {
            1 => Some(Self::MatchAll),
            2 => Some(Self::MatchAny),
            3 => Some(Self::Phrase),
            4 => Some(Self::Prefix),
            5 => Some(Self::Wildcard),
            _ => None,
        }
    }
}

pub struct PaimonTantivyReader {
    /// Held alive so `IndexReader::searcher()` + `index.tokenizers()` stay
    /// usable for the reader's lifetime.
    index: Index,
    reader: IndexReader,
    text_field: Field,
    /// Name of the tokenizer the `text` field is actually bound to in the open
    /// index's schema (read from `meta.json` at construction time). Query-side
    /// tokenization looks this up in `index.tokenizers()` every time
    tokenizer_name: String,
}

impl PaimonTantivyReader {
    /// Construct a reader from a pre-built callback-backed Directory.
    /// Layout (file names + offsets + lengths) must come from the caller
    /// (C++ side `ArchiveLayout::Parse`); Rust does not re-parse the archive.
    pub fn new(
        directory: PaimonCallbackDirectory,
        mode: TokenizeMode,
        with_position: bool,
        dict_dir: &Path,
    ) -> Result<Self, String> {
        let index = Index::open(directory)
            .map_err(|e| format!("tantivy::Index::open: {e}"))?;

        // Resolve fields by their fixed names (schema is `row_id` + `text`).
        let schema = index.schema();
        let text_field = schema.get_field(PAIMON_TEXT_FIELD_NAME).map_err(|e| {
            format!("tantivy index missing '{PAIMON_TEXT_FIELD_NAME}' field: {e}")
        })?;

        // Read the tokenizer name the `text` field was actually written with
        // (lives in meta.json's schema). Auto-aligns cpp query-side tokenizer
        // with whatever the writer side used.
        let tokenizer_name = match schema.get_field_entry(text_field).field_type() {
            tantivy::schema::FieldType::Str(text_options) => text_options
                .get_indexing_options()
                .map(|io| io.tokenizer().to_string())
                .unwrap_or_else(|| "default".to_string()),
            other => {
                return Err(format!(
                    "text field has non-TEXT type: {other:?} (schema corrupted?)"
                ));
            }
        };

        // Only register paimon_jieba if the index actually uses it. The
        // tantivy-builtin "default" / "raw" / "en_stem" etc. are pre-registered
        // by the TokenizerManager — no setup needed for those.
        if tokenizer_name == PAIMON_TOKENIZER_NAME {
            // `Path::is_empty` is unstable; check via OsStr.
            if dict_dir.as_os_str().is_empty() {
                return Err(format!(
                    "paimon_jieba tokenizer required by archive schema but dict dir \
                     is empty — set the PAIMON_JIEBA_DICT_DIR env var to a directory \
                     containing jieba.dict.utf8 / hmm_model.utf8 / user.dict.utf8 / \
                     idf.utf8 / stop_words.utf8"
                ));
            }
            let jieba = PaimonJiebaTokenizer::new(dict_dir, mode, with_position)
                .map_err(|e| format!("create paimon_jieba tokenizer: {e}"))?;
            index.tokenizers().register(PAIMON_TOKENIZER_NAME, jieba);
        } else {
            // For other known-safe names we trust tantivy's builtin registry.
            // `mode` / `dict_dir` are unused in this branch — no-op; we still
            // require them in the ABI for backward-compat with the jieba case.
            let _ = (mode, dict_dir);
        }

        // Sanity: the tokenizer MUST be resolvable now; otherwise query-time
        // lookup fails mid-flight.
        if index.tokenizers().get(&tokenizer_name).is_none() {
            return Err(format!(
                "tokenizer {tokenizer_name:?} referenced by text field is not \
                 registered; add it to TokenizerManager before opening the reader"
            ));
        }

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| format!("build IndexReader: {e}"))?;

        Ok(Self {
            index,
            reader,
            text_field,
            tokenizer_name,
        })
    }

    /// Tokenize the query string using the *same* tokenizer the index's text
    /// field was built with. Looks up `self.tokenizer_name` in the index's
    /// `TokenizerManager` — which was populated by `new()` with either
    /// `paimon_jieba` (if cpp wrote the index) or a tantivy builtin like
    /// `default` (if paimon-java wrote it).
    fn tokenize_query(&self, query: &str) -> Vec<String> {
        // `TokenizerManager::get` returns a fresh clone per call — safe to use
        // across threads / calls. If the tokenizer was missing we'd have
        // failed in `new()`; we still defend with `unwrap_or_default`.
        let mut analyzer = match self.index.tokenizers().get(&self.tokenizer_name) {
            Some(a) => a,
            None => return Vec::new(),
        };
        let mut stream = analyzer.token_stream(query);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        out
    }

    fn build_match_query(&self, query: &str, occur: Occur) -> Result<Box<dyn Query>, String> {
        let terms = self.tokenize_query(query);
        if terms.is_empty() {
            return Err(format!("query {query:?} produced no tokens after analysis"));
        }
        if terms.len() == 1 {
            let term = Term::from_field_text(self.text_field, &terms[0]);
            return Ok(Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)));
        }
        let clauses: Vec<(Occur, Box<dyn Query>)> = terms
            .iter()
            .map(|t| {
                let term = Term::from_field_text(self.text_field, t);
                let q: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (occur, q)
            })
            .collect();
        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    fn build_phrase_query(&self, query: &str) -> Result<Box<dyn Query>, String> {
        let terms = self.tokenize_query(query);
        if terms.is_empty() {
            return Err(format!("phrase query {query:?} produced no tokens"));
        }
        if terms.len() == 1 {
            // PhraseQuery requires >=2 terms in tantivy; degrade to TermQuery.
            let term = Term::from_field_text(self.text_field, &terms[0]);
            return Ok(Box::new(TermQuery::new(term, IndexRecordOption::WithFreqsAndPositions)));
        }
        let tantivy_terms: Vec<Term> = terms
            .iter()
            .map(|t| Term::from_field_text(self.text_field, t))
            .collect();
        Ok(Box::new(PhraseQuery::new(tantivy_terms)))
    }

    fn build_prefix_query(&self, query: &str) -> Result<Box<dyn Query>, String> {
        if query.is_empty() {
            return Err("prefix query is empty".into());
        }
        // Mirror lucene-fts: don't tokenize prefix; match indexed term bytes
        // starting with the given prefix verbatim.
        let pattern = format!("{}.*", regex_escape(query));
        RegexQuery::from_pattern(&pattern, self.text_field)
            .map(|q| Box::new(q) as Box<dyn Query>)
            .map_err(|e| format!("RegexQuery from prefix {query:?}: {e}"))
    }

    fn build_wildcard_query(&self, query: &str) -> Result<Box<dyn Query>, String> {
        if query.is_empty() {
            return Err("wildcard query is empty".into());
        }
        let pattern = wildcard_to_regex(query);
        RegexQuery::from_pattern(&pattern, self.text_field)
            .map(|q| Box::new(q) as Box<dyn Query>)
            .map_err(|e| format!("RegexQuery from wildcard {query:?} (pattern {pattern}): {e}"))
    }

    fn build_query(&self, search_type: SearchType, query: &str) -> Result<Box<dyn Query>, String> {
        match search_type {
            SearchType::MatchAll => self.build_match_query(query, Occur::Must),
            SearchType::MatchAny => self.build_match_query(query, Occur::Should),
            SearchType::Phrase => self.build_phrase_query(query),
            SearchType::Prefix => self.build_prefix_query(query),
            SearchType::Wildcard => self.build_wildcard_query(query),
        }
    }

    /// Return all matching row_ids (no scoring, no limit, no pre_filter).
    /// row_ids come from the explicit `row_id` u64 fast field, supporting
    /// multi-segment indexes (e.g. produced by paimon-java without force-merge).
    pub fn search_all(&self, search_type: SearchType, query: &str) -> Result<Vec<u64>, String> {
        let q = self.build_query(search_type, query)?;
        let searcher = self.reader.searcher();
        let mut ids: Vec<u64> = searcher
            .search(&*q, &RowIdCollector)
            .map_err(|e| format!("tantivy search: {e}"))?;
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    /// 4-path dispatch on `(with_score, limit)` — see `docs/dev/tantivy_bm25_score_contract.md`
    /// §4.
    ///
    /// | with_score | limit  | path | collector              | sort           | truncate | output score |
    /// |------------|--------|------|------------------------|----------------|----------|--------------|
    /// | false      | None   |  A   | RowIdCollector         | row_id asc     | —        | ❌           |
    /// | false      | Some(n)|  B   | AllScoredCollector     | score desc     | top n    | ❌ (dropped) |
    /// | true       | None   |  C   | AllScoredCollector     | row_id asc     | —        | ✅           |
    /// | true       | Some(n)|  D   | AllScoredCollector     | score desc     | top n    | ✅           |
    ///
    /// Pre-filter is a `Treemap` of paimon row_ids (not tantivy doc_ids), applied BEFORE
    /// truncation so high-score matches outside the filter don't crowd out valid ones.
    ///
    /// **v0.2 contract change**: previously `limit.is_some()` implicitly triggered scoring; now
    /// scoring is gated solely by `with_score`.
    pub fn search_with_limit_and_filter(
        &self,
        search_type: SearchType,
        query: &str,
        with_score: bool,
        limit: Option<usize>,
        pre_filter: Option<&Treemap>,
        min_score: Option<f32>,
    ) -> Result<Vec<(u64, Option<f32>)>, String> {
        let q = self.build_query(search_type, query)?;
        let searcher = self.reader.searcher();
        match (with_score, limit) {
            // Path A: all rows, no score. RowIdCollector reads the `row_id` fast
            // field inline per segment (opened once), avoiding a DocSetCollector
            // HashSet and per-doc handle — hot path for high-cardinality counts.
            (false, None) => {
                let mut row_ids: Vec<u64> = searcher
                    .search(&*q, &RowIdCollector)
                    .map_err(|e| format!("tantivy search: {e}"))?;
                if let Some(filter) = pre_filter {
                    row_ids.retain(|id| filter.contains(*id));
                }
                row_ids.sort_unstable();
                row_ids.dedup();
                Ok(row_ids.into_iter().map(|id| (id, None)).collect())
            }
            // Path B: any N matches, unscored. Used by SR's `WHERE MATCH ... LIMIT N` (no
            // ORDER BY): pushes the limit down so each shard stops collecting once N hits
            // are gathered per segment instead of materialising the full posting list.
            // If the caller wants top-N by BM25 they should set `with_score=true` (Path D)
            // and ignore the score values.
            (false, Some(n)) => {
                if n == 0 {
                    return Ok(Vec::new());
                }
                if min_score.is_some() {
                    // min_score requires scoring — fall back to collect_scored path
                    let mut filtered = self.collect_scored(&*q, &searcher, pre_filter)?;
                    if let Some(threshold) = min_score {
                        filtered.retain(|(s, _)| *s > threshold);
                    }
                    let truncated = Self::sort_by_score_desc_truncate(filtered, n);
                    Ok(truncated.into_iter().map(|(_, id)| (id, None)).collect())
                } else if let Some(filter) = pre_filter {
                    // pre_filter present: it MUST be applied to the full match set
                    // before truncation. LimitedDocSetCollector stops after the
                    // first N raw matches, which could all be filtered out while
                    // valid matches exist further down the posting list — that
                    // would under-return (fewer than N, or even empty). So collect
                    // every matching row_id (filter-aware), then truncate to N.
                    let mut row_ids: Vec<u64> = searcher
                        .search(&*q, &RowIdCollector)
                        .map_err(|e| format!("tantivy search: {e}"))?;
                    row_ids.retain(|id| filter.contains(*id));
                    row_ids.sort_unstable();
                    row_ids.dedup();
                    row_ids.truncate(n);
                    Ok(row_ids.into_iter().map(|id| (id, None)).collect())
                } else {
                    // No pre_filter: fast path — stop collecting once N matches are
                    // gathered per segment instead of materialising the full posting list.
                    let collector = LimitedDocSetCollector::new(n);
                    let mut docset = searcher
                        .search(&*q, &collector)
                        .map_err(|e| format!("tantivy search: {e}"))?;
                    let mut by_segment: std::collections::HashMap<SegmentOrdinal, Vec<DocId>> =
                        std::collections::HashMap::new();
                    for addr in docset.drain(..) {
                        by_segment.entry(addr.segment_ord).or_default().push(addr.doc_id);
                    }
                    let mut row_ids: Vec<u64> = Vec::new();
                    for (segment_ord, doc_ids) in by_segment.iter() {
                        let segment_reader = searcher.segment_reader(*segment_ord);
                        let fast = segment_reader
                            .fast_fields()
                            .u64(PAIMON_ROW_ID_FIELD_NAME)
                            .map_err(|e| format!("fast_fields().u64('row_id') on segment {}: {e}",
                                                 segment_ord))?;
                        for &doc_id in doc_ids {
                            row_ids.push(fast.first(doc_id).unwrap_or(0));
                        }
                    }
                    row_ids.sort_unstable();
                    row_ids.dedup();
                    row_ids.truncate(n);
                    Ok(row_ids.into_iter().map(|id| (id, None)).collect())
                }
            }
            // Path C: all rows + all scores, sorted by row_id asc to match the
            // BitmapScoredGlobalIndexResult contract (bitmap iter order == score order).
            (true, None) => {
                let mut filtered = self.collect_scored(&*q, &searcher, pre_filter)?;
                if let Some(threshold) = min_score {
                    filtered.retain(|(s, _)| *s > threshold);
                }
                filtered.sort_unstable_by(|a, b| a.1.cmp(&b.1));
                Ok(filtered.into_iter().map(|(s, id)| (id, Some(s))).collect())
            }
            // Path D: top-N by BM25 with scores.
            (true, Some(n)) => {
                if n == 0 {
                    return Ok(Vec::new());
                }
                let mut filtered = self.collect_scored(&*q, &searcher, pre_filter)?;
                if let Some(threshold) = min_score {
                    filtered.retain(|(s, _)| *s > threshold);
                }
                let truncated = Self::sort_by_score_desc_truncate(filtered, n);
                Ok(truncated.into_iter().map(|(s, id)| (id, Some(s))).collect())
            }
        }
    }

    /// Helper for paths B/C/D: run AllScoredCollector, translate doc_id → row_id, apply pre_filter.
    /// Groups results by segment so the fast field column handle is opened once per segment
    /// (same rationale as Path A — avoids per-match Column<u64> allocation).
    fn collect_scored(
        &self,
        q: &dyn Query,
        searcher: &tantivy::Searcher,
        pre_filter: Option<&Treemap>,
    ) -> Result<Vec<(Score, u64)>, String> {
        let scored = searcher
            .search(q, &AllScoredCollector)
            .map_err(|e| format!("tantivy search: {e}"))?;
        let mut by_segment: std::collections::HashMap<SegmentOrdinal, Vec<(Score, DocId)>> =
            std::collections::HashMap::new();
        for (s, addr) in scored.into_iter() {
            by_segment.entry(addr.segment_ord).or_default().push((s, addr.doc_id));
        }
        let mut result: Vec<(Score, u64)> = Vec::new();
        for (segment_ord, entries) in by_segment.iter() {
            let segment_reader = searcher.segment_reader(*segment_ord);
            let fast = segment_reader
                .fast_fields()
                .u64(PAIMON_ROW_ID_FIELD_NAME)
                .map_err(|e| format!("fast_fields().u64('row_id') on segment {}: {e}",
                                     segment_ord))?;
            for &(score, doc_id) in entries {
                let rid = fast.first(doc_id).unwrap_or(0);
                if pre_filter.map_or(true, |t| t.contains(rid)) {
                    result.push((score, rid));
                }
            }
        }
        Ok(result)
    }

    /// Helper for paths B/D: sort (score, row_id) by score desc with row_id asc tie-break,
    /// then truncate to `n` items.
    fn sort_by_score_desc_truncate(mut v: Vec<(Score, u64)>, n: usize) -> Vec<(Score, u64)> {
        v.sort_unstable_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
        v.truncate(n);
        v
    }

    #[cfg(test)]
    pub(crate) fn tokenizer_name(&self) -> &str {
        &self.tokenizer_name
    }

    #[cfg(test)]
    pub(crate) fn debug_index(&self) -> &Index {
        &self.index
    }
}

/// Escape regex metacharacters, but leave the input as a verbatim literal.
fn regex_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 4);
    for ch in input.chars() {
        match ch {
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Translate a glob-style wildcard ('*' = any, '?' = single char) into a
/// regex pattern, escaping all other regex metacharacters.
fn wildcard_to_regex(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 4);
    for ch in input.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Collector that reads the explicit `row_id` u64 fast field directly into a
/// `Vec<u64>`, opening the column once per segment in `for_segment`. Replaces
/// the DocSetCollector → HashSet → per-doc translate path for unscored queries.
struct RowIdCollector;

struct RowIdSegmentCollector {
    row_id: Column<u64>,
    ids: Vec<u64>,
}

impl SegmentCollector for RowIdSegmentCollector {
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.ids.push(self.row_id.first(doc).unwrap_or(0));
    }

    fn harvest(self) -> Vec<u64> {
        self.ids
    }
}

impl Collector for RowIdCollector {
    type Fruit = Vec<u64>;
    type Child = RowIdSegmentCollector;

    fn for_segment(
        &self, _ord: SegmentOrdinal, segment: &SegmentReader,
    ) -> tantivy::Result<RowIdSegmentCollector> {
        let row_id = segment.fast_fields().u64(PAIMON_ROW_ID_FIELD_NAME)?;
        Ok(RowIdSegmentCollector { row_id, ids: Vec::new() })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segs: Vec<Vec<u64>>) -> tantivy::Result<Vec<u64>> {
        Ok(segs.into_iter().flatten().collect())
    }
}

/// Collector that returns at most `limit` DocAddresses across all segments,
/// no scoring. Shared atomic counter caps the global total so per-shard
/// transfer stays bounded for plain `LIMIT N` queries (no ORDER BY).
struct LimitedDocSetCollector {
    limit: usize,
    counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl LimitedDocSetCollector {
    fn new(limit: usize) -> Self {
        Self { limit, counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)) }
    }
}

struct LimitedDocSetSegmentCollector {
    segment_ord: SegmentOrdinal,
    docs: Vec<DocId>,
    counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    limit: u64,
}

impl SegmentCollector for LimitedDocSetSegmentCollector {
    type Fruit = Vec<DocAddress>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Best-effort cap: if multiple segments are scanned concurrently the
        // atomic ensures we never accept more than `limit` rows total.
        let prev = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if prev < self.limit {
            self.docs.push(doc);
        }
    }

    fn harvest(self) -> Self::Fruit {
        let segment_ord = self.segment_ord;
        self.docs.into_iter().map(|d| DocAddress::new(segment_ord, d)).collect()
    }
}

impl Collector for LimitedDocSetCollector {
    type Fruit = Vec<DocAddress>;
    type Child = LimitedDocSetSegmentCollector;

    fn for_segment(
        &self, segment_ord: SegmentOrdinal, _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(LimitedDocSetSegmentCollector {
            segment_ord,
            docs: Vec::new(),
            counter: self.counter.clone(),
            limit: self.limit as u64,
        })
    }

    fn requires_scoring(&self) -> bool { false }

    fn merge_fruits(
        &self, segment_fruits: Vec<Vec<DocAddress>>,
    ) -> tantivy::Result<Vec<DocAddress>> {
        let mut result: Vec<DocAddress> = segment_fruits.into_iter().flatten().collect();
        result.truncate(self.limit);
        Ok(result)
    }
}

/// Custom Collector that returns ALL matching (score, DocAddress) tuples,
/// without truncation. tantivy's stock `TopDocs::with_limit(N)` would force
/// us to either pick N upfront (wrong when pre_filter rejects high-score
/// docs) or pass `usize::MAX` (which still enforces a binary heap on every
/// push). Our collector is just a plain Vec append, then merge.
struct AllScoredCollector;

struct AllScoredSegmentCollector {
    segment_ord: SegmentOrdinal,
    docs: Vec<(Score, DocId)>,
}

impl SegmentCollector for AllScoredSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.docs.push((score, doc));
    }

    fn harvest(self) -> Self::Fruit {
        let segment_ord = self.segment_ord;
        self.docs
            .into_iter()
            .map(|(s, d)| (s, DocAddress::new(segment_ord, d)))
            .collect()
    }
}

impl Collector for AllScoredCollector {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = AllScoredSegmentCollector;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        _segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(AllScoredSegmentCollector {
            segment_ord,
            docs: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(Score, DocAddress)>>,
    ) -> tantivy::Result<Vec<(Score, DocAddress)>> {
        Ok(segment_fruits.into_iter().flatten().collect())
    }
}

// ============================ FFI surface ============================

/// Construct a streaming reader from a layout table + pread callbacks.
///
/// The layout arrays (names / offsets / lengths) are produced by C++-side
/// `ArchiveLayout::Parse` after reading only the archive header bytes. Payload
/// bytes are fetched lazily through `callbacks.read_at` as tantivy reads.
///
/// # Arguments
/// * `file_names` — array of `file_count` UTF-8 NUL-terminated C strings
/// * `file_offsets` / `file_lengths` — u64 arrays (archive-absolute offsets and lengths)
/// * `file_count` — number of entries in each of the three arrays
/// * `callbacks` — pread + release callbacks; `ctx` ownership transfers to Rust
/// * `mode_cstr` — tokenize mode ("mp"/"mix"/"full"/"query"; "hmm" → Unsupported)
/// * `with_position` — whether text field was indexed with positions
/// * `dict_dir_cstr` — paimon_jieba dictionary directory
/// * `out` — receives the reader handle on success
///
/// Releases an FFI stream ctx on drop unless disarmed. Gives
/// `paimon_tantivy_reader_new_streaming` a single ownership rule: Rust owns ctx
/// from entry and releases it on any error before the directory takes over.
struct StreamCtxReleaseGuard {
    release: extern "C" fn(*mut std::os::raw::c_void),
    ctx: *mut std::os::raw::c_void,
    armed: bool,
}

impl Drop for StreamCtxReleaseGuard {
    fn drop(&mut self) {
        if self.armed {
            (self.release)(self.ctx);
        }
    }
}

/// # Safety
/// All pointer args must be valid for the duration of the call. Ownership of
/// `callbacks.ctx` transfers to Rust on entry: on any error this function
/// releases it, and on success it lives until the reader handle is freed. The
/// caller must NOT release ctx itself after calling this function.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_reader_new_streaming(
    file_names: *const *const c_char,
    file_offsets: *const u64,
    file_lengths: *const u64,
    file_count: usize,
    callbacks: PaimonStreamCallbacks,
    mode_cstr: *const c_char,
    with_position: bool,
    dict_dir_cstr: *const c_char,
    out: *mut *mut PaimonTantivyReader,
) -> PaimonTantivyStatus {
    // Single, uniform ownership rule: Rust owns ctx from entry. This guard
    // releases it on every error path until ownership moves into the directory
    // (which then releases on its own drop). Prevents the C++ caller and Rust
    // from both releasing the same ctx on the post-directory failure path.
    let mut ctx_guard = StreamCtxReleaseGuard {
        release: callbacks.release,
        ctx: callbacks.ctx,
        armed: true,
    };

    if mode_cstr.is_null() || dict_dir_cstr.is_null() || out.is_null() {
        set_last_error("paimon_tantivy_reader_new_streaming: null mandatory argument");
        return PaimonTantivyStatus::InvalidArgument;
    }
    if file_count > 0
        && (file_names.is_null() || file_offsets.is_null() || file_lengths.is_null())
    {
        set_last_error("file_names/offsets/lengths must be non-null when file_count > 0");
        return PaimonTantivyStatus::InvalidArgument;
    }

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
    let mode = match TokenizeMode::parse(mode_str) {
        Some(m) => m,
        None => {
            set_last_error(format!(
                "unknown tokenize mode {mode_str:?}; expected mp/mix/full/query"
            ));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };

    // Copy the C string array into owned Rust entries so the directory doesn't
    // depend on caller-supplied lifetime.
    let mut entries: Vec<(String, u64, u64)> = Vec::with_capacity(file_count);
    for i in 0..file_count {
        let name_ptr = unsafe { *file_names.add(i) };
        if name_ptr.is_null() {
            set_last_error(format!("file_names[{i}] is null"));
            return PaimonTantivyStatus::InvalidArgument;
        }
        let name = match unsafe { CStr::from_ptr(name_ptr) }.to_str() {
            Ok(s) => s.to_owned(),
            Err(e) => {
                set_last_error(format!("file_names[{i}] not utf-8: {e}"));
                return PaimonTantivyStatus::InvalidArgument;
            }
        };
        let offset = unsafe { *file_offsets.add(i) };
        let length = unsafe { *file_lengths.add(i) };
        entries.push((name, offset, length));
    }

    // Ownership of ctx transfers to the directory from here on (it releases on
    // its own drop, whether it fails below or lives inside the returned reader).
    ctx_guard.armed = false;
    let directory = PaimonCallbackDirectory::new(entries, callbacks);

    match PaimonTantivyReader::new(directory, mode, with_position, Path::new(dict_dir)) {
        Ok(r) => {
            unsafe { *out = into_handle(r) };
            PaimonTantivyStatus::Ok
        }
        Err(e) => {
            let unsupported = e.contains("'hmm' is not supported");
            let bad_format = e.contains("tantivy::Index::open")
                || e.contains("missing 'text' field");
            set_last_error(e);
            if unsupported {
                PaimonTantivyStatus::Unsupported
            } else if bad_format {
                PaimonTantivyStatus::IndexFormatError
            } else {
                PaimonTantivyStatus::InternalError
            }
        }
    }
}

/// Run a query and emit results into `out`.
///
/// Output bytes (little-endian):
///   `[u8 has_scores | u64 count | u64 row_ids[count] | optional f32 scores[count]]`
///
/// `has_scores=1` iff `limit >= 0` (caller asked for scoring + limit).
///
/// `limit < 0` ⇒ no limit, no scoring; sorted ascending by row_id.
/// `limit >= 0` ⇒ top-N by descending score (pre_filter applied first).
/// `pre_filter_bytes`: serialized croaring `Roaring64Map::write` (portable),
///   containing paimon **row_ids** (not tantivy doc_ids); null+0 = no filter.
///
/// SAFETY: `reader` must be a live handle; `query` and `pre_filter_bytes`
/// may be null+0 or readable slices; `out` non-null.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_reader_search(
    reader: *mut PaimonTantivyReader,
    search_type: i32,
    query: *const c_char,
    query_len: usize,
    with_score: bool,
    limit: i32,
    pre_filter_bytes: *const c_char,
    pre_filter_len: usize,
    min_score: f32,
    out: *mut PaimonTantivyBuffer,
) -> PaimonTantivyStatus {
    if out.is_null() {
        set_last_error("reader_search: out is null");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let Some(r) = (unsafe { borrow_handle_mut::<PaimonTantivyReader>(reader) }) else {
        set_last_error("reader_search: null reader handle");
        return PaimonTantivyStatus::InvalidArgument;
    };
    let st = match SearchType::from_i32(search_type) {
        Some(s) => s,
        None => {
            set_last_error(format!("unknown search_type {search_type}"));
            return PaimonTantivyStatus::InvalidArgument;
        }
    };
    if query.is_null() && query_len != 0 {
        set_last_error("query is null but len > 0");
        return PaimonTantivyStatus::InvalidArgument;
    }
    let query_str = if query_len == 0 {
        ""
    } else {
        let slice = unsafe { std::slice::from_raw_parts(query as *const u8, query_len) };
        match std::str::from_utf8(slice) {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("query not utf-8: {e}"));
                return PaimonTantivyStatus::InvalidArgument;
            }
        }
    };

    let pre_filter: Option<Treemap> = if pre_filter_bytes.is_null() && pre_filter_len == 0 {
        None
    } else if pre_filter_bytes.is_null() {
        set_last_error("pre_filter_bytes is null but len > 0");
        return PaimonTantivyStatus::InvalidArgument;
    } else {
        let slice = unsafe {
            std::slice::from_raw_parts(pre_filter_bytes as *const u8, pre_filter_len)
        };
        match Treemap::try_deserialize::<Portable>(slice) {
            Some(t) => Some(t),
            None => {
                set_last_error(format!(
                    "pre_filter not a valid Roaring64Map portable serialization ({} bytes)",
                    pre_filter_len
                ));
                return PaimonTantivyStatus::InvalidArgument;
            }
        }
    };

    let limit_opt: Option<usize> = if limit < 0 { None } else { Some(limit as usize) };
    let min_score_opt: Option<f32> = if min_score > 0.0 { Some(min_score) } else { None };

    match r.search_with_limit_and_filter(st, query_str, with_score, limit_opt, pre_filter.as_ref(), min_score_opt)
    {
        Ok(rows) => {
            // v0.2: has_scores is decoupled from limit — it equals with_score directly.
            let has_scores = with_score;
            let count = rows.len() as u64;
            // 1 byte has_scores + 8 bytes count + 8 bytes per row_id + optional 4 bytes per score
            let mut buf = Vec::with_capacity(
                1 + 8 + rows.len() * 8 + if has_scores { rows.len() * 4 } else { 0 },
            );
            buf.push(if has_scores { 1u8 } else { 0u8 });
            buf.extend_from_slice(&count.to_le_bytes());
            for (id, _) in &rows {
                buf.extend_from_slice(&id.to_le_bytes()); // u64 row_id LE
            }
            if has_scores {
                for (_, score) in &rows {
                    let s = score.unwrap_or(0.0);
                    buf.extend_from_slice(&s.to_le_bytes());
                }
            }
            unsafe { *out = PaimonTantivyBuffer::from_vec(buf) };
            PaimonTantivyStatus::Ok
        }
        Err(e) => {
            let parse_err = e.contains("RegexQuery from")
                || e.contains("phrase query")
                || e.contains("produced no tokens");
            set_last_error(e);
            if parse_err {
                PaimonTantivyStatus::QueryParseError
            } else {
                PaimonTantivyStatus::InternalError
            }
        }
    }
}

/// Destroy a reader handle. Safe on null.
#[no_mangle]
pub unsafe extern "C" fn paimon_tantivy_reader_free(reader: *mut PaimonTantivyReader) {
    unsafe { free_handle(reader) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callback_directory::test_support::{build_directory_from_archive, make_mock_callbacks};
    use crate::writer::PaimonTantivyWriter;
    use std::path::PathBuf;

    // When PaimonTantivyReader::new fails *after* the callback directory (and
    // thus ctx) is constructed, Rust must release ctx exactly once — never zero
    // (leak) and never twice (the C++ caller no longer releases on failure, so a
    // second release would be a double-free). file_count=0 yields an index with
    // no meta.json, which fails to open after the directory is built.
    #[test]
    fn reader_new_streaming_releases_ctx_once_on_failure() {
        let (cb, backend) = make_mock_callbacks(Vec::new());
        let mode = std::ffi::CString::new("mix").unwrap();
        let dict = std::ffi::CString::new("").unwrap();
        let mut out: *mut PaimonTantivyReader = std::ptr::null_mut();
        let st = unsafe {
            paimon_tantivy_reader_new_streaming(
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                0,
                cb,
                mode.as_ptr(),
                true,
                dict.as_ptr(),
                &mut out,
            )
        };
        assert!(!matches!(st, PaimonTantivyStatus::Ok));
        assert!(out.is_null());
        assert_eq!(
            backend.release_count.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    fn dict_dir() -> PathBuf {
        std::env::var("PAIMON_JIEBA_DICT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/nonexistent-dict"))
    }

    fn build(docs: &[&str]) -> Vec<u8> {
        let mut w = PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir(), "paimon_jieba").unwrap();
        for (i, d) in docs.iter().enumerate() {
            w.add(i as u64, d).unwrap();
        }
        w.finish().unwrap().1
    }

    fn open(packed: &[u8]) -> PaimonTantivyReader {
        // Simulate production flow: parse archive header → build layout →
        // back PaimonCallbackDirectory with a mock pread that reads from the
        // packed Vec. Once C++ `ArchiveLayout::Parse` is in place, prod
        // uses the same PaimonCallbackDirectory path.
        let (dir, _backend) = build_directory_from_archive(packed.to_vec());
        PaimonTantivyReader::new(dir, TokenizeMode::Mix, true, &dict_dir()).unwrap()
    }

    #[test]
    fn match_all_single_term() {
        let bytes = build(&["hello world", "hello there", "world peace"]);
        let r = open(&bytes);
        let ids = r.search_all(SearchType::MatchAll, "hello").unwrap();
        assert_eq!(ids, vec![0u64, 1]);
    }

    #[test]
    fn match_all_two_terms_intersection() {
        let bytes = build(&["hello world", "hello there", "world peace"]);
        let r = open(&bytes);
        let ids = r.search_all(SearchType::MatchAll, "hello world").unwrap();
        assert_eq!(ids, vec![0u64]);
    }

    #[test]
    fn match_any_two_terms_union() {
        let bytes = build(&["hello world", "hello there", "world peace"]);
        let r = open(&bytes);
        let ids = r.search_all(SearchType::MatchAny, "hello peace").unwrap();
        assert_eq!(ids, vec![0u64, 1, 2]);
    }

    #[test]
    fn phrase_only_consecutive() {
        let bytes = build(&["hello world there", "world hello there"]);
        let r = open(&bytes);
        let ids = r.search_all(SearchType::Phrase, "hello world").unwrap();
        assert_eq!(ids, vec![0u64]);
    }

    #[test]
    fn prefix_matches_indexed_terms() {
        let bytes = build(&["unordered user-defined doc id"]);
        let r = open(&bytes);
        let ids = r.search_all(SearchType::Prefix, "unorder").unwrap();
        assert_eq!(ids, vec![0u64]);
    }

    #[test]
    fn wildcard_with_star() {
        let bytes = build(&["unordered", "ordered", "border"]);
        let r = open(&bytes);
        let ids = r.search_all(SearchType::Wildcard, "*order*").unwrap();
        assert_eq!(ids, vec![0u64, 1, 2]);
    }

    #[test]
    fn empty_query_for_match_returns_query_parse_error() {
        let bytes = build(&["hello"]);
        let r = open(&bytes);
        let err = r.search_all(SearchType::MatchAll, "").unwrap_err();
        assert!(err.contains("no tokens"), "got: {err}");
    }

    #[test]
    fn wildcard_helper_escapes_dots() {
        assert_eq!(wildcard_to_regex("a*b"), "a.*b");
        assert_eq!(wildcard_to_regex("a?b"), "a.b");
        assert_eq!(wildcard_to_regex("a.b"), r"a\.b");
        assert_eq!(wildcard_to_regex("*a*"), ".*a.*");
    }

    // ----- limit + pre_filter + scoring (row_id-based) -----

    #[test]
    fn limit_returns_top_n_with_scores() {
        let bytes = build(&[
            "doc",                              // 0: low score (1 occurrence)
            "doc doc doc doc doc",              // 1: high score (5 occurrences)
            "doc doc",                          // 2: medium score
        ]);
        let r = open(&bytes);
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "doc", true, Some(2), None, None)
            .unwrap();
        assert_eq!(rows.len(), 2);
        // doc 1 has highest TF, expect first
        assert_eq!(rows[0].0, 1u64);
        assert!(rows[0].1.is_some());
        assert!(rows[1].1.is_some());
        // Scores monotonically decreasing
        assert!(rows[0].1.unwrap() >= rows[1].1.unwrap());
    }

    #[test]
    fn no_limit_returns_all_unscored() {
        let bytes = build(&["hello world", "world hello", "world peace"]);
        let r = open(&bytes);
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "world", false, None, None, None)
            .unwrap();
        let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![0u64, 1, 2]);
        assert!(rows.iter().all(|(_, s)| s.is_none()));
    }

    #[test]
    fn pre_filter_no_limit_intersects() {
        let bytes = build(&["alpha beta", "alpha gamma", "beta gamma"]);
        let r = open(&bytes);
        // pre_filter = {0, 2}; query "alpha" matches {0, 1}; expect intersection {0}
        let mut tm = Treemap::new();
        tm.add(0);
        tm.add(2);
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "alpha", false, None, Some(&tm), None)
            .unwrap();
        let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![0u64]);
    }

    #[test]
    fn pre_filter_with_limit_filters_before_topn() {
        // doc 0 has highest TF for "doc" but is NOT in pre_filter → must NOT
        // be in result, even with limit=1.
        let bytes = build(&[
            "doc doc doc doc doc",    // 0: highest TF, but excluded
            "doc doc",                // 1: medium TF, included
            "doc",                    // 2: low TF, excluded
        ]);
        let r = open(&bytes);
        let mut tm = Treemap::new();
        tm.add(1);  // only doc 1 passes pre_filter
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "doc", true, Some(10), Some(&tm), None)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1u64);
    }

    #[test]
    fn unscored_limit_with_pre_filter_applies_filter_before_truncate() {
        // Regression (review finding #1): with_score=false + limit=N + pre_filter
        // must apply the filter to the FULL match set before truncating to N.
        // All three docs match "doc" but only row_id 2 (the LAST one) passes the
        // pre_filter; a truncate-before-filter impl (LimitedDocSetCollector that
        // stops at N raw matches, then filters) would collect doc 0, filter it
        // out, and wrongly return empty instead of {2}.
        let bytes = build(&["doc", "doc", "doc"]);
        let r = open(&bytes);
        let mut tm = Treemap::new();
        tm.add(2); // only row_id 2 passes the pre_filter
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "doc", false, Some(1), Some(&tm), None)
            .unwrap();
        let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![2u64], "pre_filter must be applied before LIMIT truncation");
        assert!(rows.iter().all(|(_, s)| s.is_none()));
    }

    #[test]
    fn empty_pre_filter_returns_empty() {
        let bytes = build(&["alpha", "beta"]);
        let r = open(&bytes);
        let tm = Treemap::new();  // empty
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "alpha", false, None, Some(&tm), None)
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn limit_zero_returns_empty_without_running_query() {
        let bytes = build(&["alpha", "beta"]);
        let r = open(&bytes);
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "alpha", true, Some(0), None, None)
            .unwrap();
        assert!(rows.is_empty());
    }

    // ----- row_id is independent of doc_id -----

    #[test]
    fn pre_filter_uses_row_id_not_doc_id() {
        // Build with non-contiguous row_ids so doc_id ≠ row_id. Then verify
        // pre_filter operates on row_id values, not internal tantivy doc_ids.
        let mut w = PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir(), "paimon_jieba").unwrap();
        w.add(100, "alpha").unwrap();
        w.add(200, "alpha").unwrap();
        w.add(300, "alpha").unwrap();
        let bytes = w.finish().unwrap().1;
        let r = open(&bytes);

        // pre_filter = {200} as row_id (doc_id would be 1)
        let mut tm = Treemap::new();
        tm.add(200);
        let rows = r
            .search_with_limit_and_filter(SearchType::MatchAll, "alpha", false, None, Some(&tm), None)
            .unwrap();
        let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![200u64], "pre_filter must operate on row_id, not doc_id");
    }

    #[test]
    fn search_returns_caller_supplied_row_ids() {
        // Same setup: row_ids 100/200/300, verify search_all returns those values.
        let mut w = PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir(), "paimon_jieba").unwrap();
        w.add(100, "doc").unwrap();
        w.add(200, "doc").unwrap();
        w.add(300, "doc").unwrap();
        let bytes = w.finish().unwrap().1;
        let r = open(&bytes);
        let ids = r.search_all(SearchType::MatchAll, "doc").unwrap();
        assert_eq!(ids, vec![100u64, 200, 300]);
    }

    #[test]
    fn tokenizer_name_reflects_paimon_jieba_schema_for_cpp_written_index() {
        // cpp-written index: PaimonTantivyWriter binds the text field to
        // `paimon_jieba`. Reader must pick that up from meta.json (not hardcode).
        let bytes = build(&["hello world"]);
        let r = open(&bytes);
        assert_eq!(r.tokenizer_name(), PAIMON_TOKENIZER_NAME);

        // tokenize sanity: jieba mode="mix" picks `hello` + `world` from ASCII.
        let q = r.tokenize_query("hello world");
        assert_eq!(q, vec!["hello".to_string(), "world".to_string()]);
    }

    #[test]
    fn tokenizer_name_reflects_default_schema_for_externally_written_index() {
        // Simulate a paimon-java-shaped index: text field bound to the
        // builtin `default` tokenizer (SimpleTokenizer + LowerCaser), not jieba.
        // Build it directly via tantivy (bypassing PaimonTantivyWriter's jieba
        // schema) so we can prove the reader auto-switches to the builtin.
        use crate::callback_directory::test_support::build_mock_directory;
        use tantivy::directory::Directory;
        use tantivy::schema::{IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions};
        use tantivy::{doc, Index};

        // Build a minimal index with field "text" bound to "default".
        let mut sb = Schema::builder();
        let row_id_f = sb.add_u64_field(
            "row_id",
            NumericOptions::default().set_stored().set_indexed().set_fast(),
        );
        let text_opts = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default") // ← key: match paimon-java's TEXT default
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let text_f = sb.add_text_field("text", text_opts);
        let schema = sb.build();
        let tmp = tempfile::Builder::new()
            .prefix("paimon-tantivy-dyn-tk-")
            .tempdir()
            .unwrap();
        let index = Index::create_in_dir(tmp.path(), schema).unwrap();
        let mut writer = index.writer(15_000_000).unwrap();
        writer
            .add_document(doc!(row_id_f => 0u64, text_f => "Hello World"))
            .unwrap();
        writer
            .add_document(doc!(row_id_f => 1u64, text_f => "Apple.Banana"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();

        // Pack the index dir into our archive format so the callback directory
        // can serve it. Reuse writer.rs's format by streaming entries manually.
        let mut data = Vec::new();
        let mut entries = Vec::<(String, u64, u64)>::new();
        let dir_iter = std::fs::read_dir(tmp.path()).unwrap();
        let mut files: Vec<_> = dir_iter
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().ok().map_or(false, |t| t.is_file()))
            .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        files.sort_by_key(|e| e.file_name());
        data.extend_from_slice(&(files.len() as i32).to_be_bytes());
        for e in &files {
            let name = e.file_name().to_string_lossy().into_owned();
            let bytes = std::fs::read(e.path()).unwrap();
            data.extend_from_slice(&(name.len() as i32).to_be_bytes());
            data.extend_from_slice(name.as_bytes());
            data.extend_from_slice(&(bytes.len() as i64).to_be_bytes());
            let off = data.len() as u64;
            data.extend_from_slice(&bytes);
            entries.push((name, off, bytes.len() as u64));
        }

        let (dir, _backend) = build_mock_directory(data, entries);
        let r = PaimonTantivyReader::new(dir, TokenizeMode::Mix, true, &dict_dir()).unwrap();

        // Reader must pick up `default` from schema, not hardcode `paimon_jieba`.
        assert_eq!(r.tokenizer_name(), "default");

        // Query tokenization now goes through tantivy's builtin default
        // (SimpleTokenizer + LowerCaser):
        //   "Apple.Banana" → ["apple", "banana"]  (dot is non-alnum, split)
        //   "Hello World"  → ["hello", "world"]   (space split + lowercase)
        let q1 = r.tokenize_query("Hello World");
        assert_eq!(q1, vec!["hello".to_string(), "world".to_string()]);
        let q2 = r.tokenize_query("Apple.Banana");
        assert_eq!(q2, vec!["apple".to_string(), "banana".to_string()]);

        // And the search path works across tokenizer:
        let ids = r.search_all(SearchType::MatchAll, "hello").unwrap();
        assert_eq!(ids, vec![0u64]);
        let ids = r.search_all(SearchType::MatchAll, "apple").unwrap();
        assert_eq!(ids, vec![1u64]);
    }

    #[test]
    fn reader_aggregates_row_ids_across_segments() {
        // Multi-thread default writer + many docs => may produce multiple
        // segments before force-merge. After finish(), force-merge collapses
        // to one segment, but this test still validates the row_id retrieval
        // path works for ≥1 segment.
        let mut w = PaimonTantivyWriter::new("f0", TokenizeMode::Mix, true, &dict_dir(), "paimon_jieba").unwrap();
        for i in 0..200u64 {
            w.add(i * 7, &format!("docmark_{i} apple")).unwrap();
        }
        let bytes = w.finish().unwrap().1;
        let r = open(&bytes);
        let ids = r.search_all(SearchType::MatchAll, "apple").unwrap();
        assert_eq!(ids.len(), 200);
        for i in 0..200u64 {
            assert!(ids.contains(&(i * 7)), "missing row_id={}", i * 7);
        }
    }
}
