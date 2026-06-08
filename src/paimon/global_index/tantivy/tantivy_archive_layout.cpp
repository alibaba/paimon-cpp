/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "paimon/global_index/tantivy/tantivy_archive_layout.h"

#include <cstring>
#include <memory>

#include "fmt/format.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"

namespace paimon::tantivy {

namespace {

/// Wrap the (non-owning) raw InputStream* in a shared_ptr-like handle so
/// DataInputStream — which takes `shared_ptr<InputStream>` — can be used
/// without transferring ownership. We use a no-op deleter to avoid double-free.
struct NoopDeleter {
    void operator()(InputStream*) const {}
};

}  // namespace

Result<ArchiveLayout> ParseArchiveHeader(InputStream* in) {
    if (in == nullptr) {
        return Status::Invalid("ParseArchiveHeader: null input stream");
    }

    // DataInputStream defaults to BE — matches paimon-java archive format.
    std::shared_ptr<InputStream> wrapped(in, NoopDeleter{});
    DataInputStream dis(wrapped);

    PAIMON_RETURN_NOT_OK(dis.Seek(0));

    PAIMON_ASSIGN_OR_RAISE(int32_t file_count, dis.ReadValue<int32_t>());
    if (file_count < 0) {
        return Status::Invalid(
            fmt::format("ParseArchiveHeader: negative file_count {}", file_count));
    }

    ArchiveLayout layout;
    layout.count = static_cast<std::size_t>(file_count);
    layout.names.reserve(layout.count);
    layout.offsets.reserve(layout.count);
    layout.lengths.reserve(layout.count);

    for (int32_t i = 0; i < file_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t name_len, dis.ReadValue<int32_t>());
        if (name_len <= 0 || name_len > 1 << 20) {
            return Status::Invalid(
                fmt::format("ParseArchiveHeader: bad name_len {} at entry {}", name_len, i));
        }
        std::string name(static_cast<std::size_t>(name_len), '\0');
        PAIMON_RETURN_NOT_OK(dis.Read(name.data(), static_cast<uint32_t>(name_len)));

        PAIMON_ASSIGN_OR_RAISE(int64_t data_len, dis.ReadValue<int64_t>());
        if (data_len < 0) {
            return Status::Invalid(
                fmt::format("ParseArchiveHeader: negative data_len {} for '{}'", data_len, name));
        }

        PAIMON_ASSIGN_OR_RAISE(int64_t data_offset, dis.GetPos());

        layout.names.push_back(std::move(name));
        layout.offsets.push_back(static_cast<uint64_t>(data_offset));
        layout.lengths.push_back(static_cast<uint64_t>(data_len));

        // Skip past the payload without reading it.
        PAIMON_RETURN_NOT_OK(dis.Seek(data_offset + data_len));
    }

    return layout;
}

}  // namespace paimon::tantivy
