/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {
class InputStream;
}  // namespace paimon

namespace paimon::tantivy {

/// Parsed layout of a packed tantivy archive. Arrays are parallel; `count` is
/// their common length.
///
/// Archive byte format (matches paimon-java `TantivyFullTextGlobalIndexReader.
/// parseArchiveHeader`; big-endian, no version header):
///   `[BE i32 file_count | (BE i32 name_len, name_utf8, BE i64 data_len, data)*]`
///
/// `offsets[i]` is the archive-absolute byte offset of file `i`'s payload
/// (points past the per-entry header). `lengths[i]` is the payload size.
struct ArchiveLayout {
    std::vector<std::string> names;
    std::vector<uint64_t> offsets;
    std::vector<uint64_t> lengths;
    std::size_t count = 0;
};

/// Read the archive header from `in` (seeking past payloads) and return the
/// layout. Does NOT read file payloads — only header bytes (a few KB).
///
/// `in` must support `Seek` (all production `paimon::InputStream` subclasses
/// do; we call `Seek(cur + data_len)` to skip over each file's payload).
///
/// On return, `in`'s internal position is at the end of the archive; callers
/// typically don't care (the stream is subsequently read via pread callbacks).
Result<ArchiveLayout> ParseArchiveHeader(InputStream* in);

}  // namespace paimon::tantivy
