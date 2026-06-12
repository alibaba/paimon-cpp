/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>

namespace paimon::tantivy {

/// Identifier used by GlobalIndexFileWriter::NewFileName to prefix on-disk
/// filenames. Tantivy and lucene file prefixes intentionally differ so a
/// reader can dispatch the right implementation by filename pattern.
static inline const char kIdentifier[] = "tantivy-fulltext";

/// Schema field names — fixed to match paimon-java. Callers
/// MUST NOT rename these even though `TantivyGlobalIndexWriter::Create` accepts
/// a `field_name` argument (that argument is used only to extract the correct
/// arrow column; the tantivy schema field name is always `"text"`).
static inline const char kTantivyTextFieldName[] = "text";
static inline const char kTantivyRowIdFieldName[] = "row_id";

/// Option-key prefix consumed by TantivyGlobalIndex. Matches the lucene-fts
/// convention so users can configure both implementations with a uniform
/// "<impl>.<knob>" key style.
static inline const char kOptionKeyPrefix[] = "tantivy-fulltext.";

/// Buffer size for streaming raw packed bytes from FFI to OutputStream
/// (Writer) and from InputStream into Rust (Reader).
static inline const int32_t kDefaultReadBufferSize = 1024 * 1024;
/// Read buffer size knob for the reader.
static inline const char kTantivyReadBufferSize[] = "read.buffer-size";

/// If true, omit term frequencies/positions when indexing (smaller index, but
/// no PhraseQuery support). Default false, mirroring lucene-fts.
static inline const char kTantivyWriteOmitTermFreqAndPositions[] =
    "write.omit-term-freq-and-position";

/// Env var carrying jieba dictionary directory; consumed by both writer and
/// reader. Same name as lucene-fts: a single env var configures both backends.
static inline const char kJiebaDictDirEnv[] = "PAIMON_JIEBA_DICT_DIR";

/// Default tokenize mode if not specified in options.
static inline const char kDefaultJiebaTokenizeMode[] = "mix";
/// Tokenize mode option key. Values: "mp", "mix", "full", "query".
/// "hmm" is rejected with Unsupported (jieba-rs does not expose standalone HMM).
static inline const char kJiebaTokenizeMode[] = "jieba.tokenize-mode";

/// Writer-side tokenizer selector. Values:
///   "default" (default) — tantivy built-in SimpleTokenizer;
///   "paimon_jieba" — jieba-rs CJK tokenizer; opt-in for Chinese workloads
///   "whitespace" / "raw" / "en_stem" — other tantivy built-ins
/// The reader side is schema-driven and auto-dispatches to whatever tokenizer
/// name is baked into the archive, so the default here also determines what
/// paimon-java sees when it cross-reads the archive.
static inline const char kTantivyWriteTokenizer[] = "tantivy.write.tokenizer";
/// Default tokenizer for writer: tantivy built-in "default" (SimpleTokenizer),
/// chosen so paimon-cpp ↔ paimon-java cross-read works out of the box.
/// Chinese workloads must opt into "paimon_jieba" via kTantivyWriteTokenizer.
static inline const char kDefaultTantivyWriteTokenizer[] = "default";

/// Reads the jieba dictionary directory from kJiebaDictDirEnv. Returns the
/// directory when the env var is set and non-empty, otherwise std::nullopt.
/// Shared by the writer and reader so the env-lookup lives in one place; each
/// caller applies its own policy for the missing case (the writer treats it as
/// an error because a jieba index needs a dictionary, while the reader tolerates
/// it because paimon-java archives use the built-in tokenizer and need none).
inline std::optional<std::string> GetJiebaDictionaryDirFromEnv() {
    const char* env_dir = std::getenv(kJiebaDictDirEnv);
    if (env_dir != nullptr && *env_dir != '\0') {
        return std::string(env_dir);
    }
    return std::nullopt;
}

}  // namespace paimon::tantivy
