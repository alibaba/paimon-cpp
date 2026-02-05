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

namespace paimon::lucene {
static inline const int32_t kVersion = 1;
static inline const char kIdentifier[] = "lucene-fts";
static inline const wchar_t kEmptyWstring[] = L"";
static inline const wchar_t kRowIdFieldWstring[] = L"_ROW_ID";

static inline const char kOptionKeyPrefix[] = "lucene-fts.";

static inline const int32_t kDefaultReadBufferSize = 1024 * 1024;
// default is 1MB
static inline const char kLuceneReadBufferSize[] = "read.buffer-size";
// default is false
static inline const char kLuceneWriteOmitTermFreqAndPositions[] =
    "write.omit-term-freq-and-position";

static inline const char kJiebaDictDirEnv[] = "PAIMON_JIEBA_DICT_DIR";

static inline const char kDefaultJiebaTokenizeMode[] = "mix";
// default is "mix". Values can be "mp", "hmm", "mix", "full", "query".
static inline const char kJiebaTokenizeMode[] = "jieba.tokenize-mode";
}  // namespace paimon::lucene
