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

#include "benchmark/benchmark_suite.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/io/api.h"
#include "benchmark/benchmark_helpers.h"
#include "benchmark/cli_option_parsing.h"
#include "paimon/api.h"
#include "paimon/catalog/catalog.h"

#if __has_include("parquet/arrow/reader.h")
#include "parquet/arrow/reader.h"
#define PAIMON_BENCHMARK_HAS_PARQUET_READER 1
#else
#define PAIMON_BENCHMARK_HAS_PARQUET_READER 0
#endif

namespace paimon::benchmark {

namespace {

using DataBatches = std::vector<std::shared_ptr<arrow::StructArray>>;

struct BenchmarkCliOptions {
    std::string source_data_file;
    std::string external_table_path;
    std::string file_format = "parquet";
    int64_t source_batch_max_rows = 4096;
    int32_t row_to_batch_thread_number = 3;
    std::vector<std::string> pk_columns;
    std::vector<std::pair<std::string, std::string>> extra_options;
};

struct SourceDataSpec {
    std::string format;
    std::string path;
};

BenchmarkCliOptions& MutableBenchmarkCliOptions() {
    static BenchmarkCliOptions options;
    return options;
}

const BenchmarkCliOptions& GetBenchmarkCliOptions() {
    return MutableBenchmarkCliOptions();
}

int64_t ParsePositiveInt64(const std::string& value, const std::string& option_name) {
    char* end = nullptr;
    const auto parsed = std::strtoll(value.c_str(), &end, 10);
    if (end == value.c_str() || *end != '\0' || parsed <= 0) {
        throw std::runtime_error("invalid " + option_name + ", expected positive integer");
    }
    return static_cast<int64_t>(parsed);
}

int32_t ParsePositiveInt32(const std::string& value, const std::string& option_name) {
    const int64_t parsed = ParsePositiveInt64(value, option_name);
    if (parsed > std::numeric_limits<int32_t>::max()) {
        throw std::runtime_error("invalid " + option_name + ", value is too large");
    }
    return static_cast<int32_t>(parsed);
}

void ParsePaimonBenchmarkCliArgsImpl(int* argc, char** argv) {
    auto& options = MutableBenchmarkCliOptions();
    auto parsed_argc = static_cast<int32_t>(*argc);
    int32_t write_index = 1;
    for (int32_t arg_index = 1; arg_index < parsed_argc; ++arg_index) {
        const std::string arg(argv[arg_index]);
        std::string parsed_value;

        if (paimon::benchmark::ParseStringOptionArg(parsed_argc, argv, arg,
                                                    "--paimon_source_data_file", &arg_index,
                                                    &options.source_data_file)) {
            continue;
        }
        if (paimon::benchmark::ParseStringOptionArg(parsed_argc, argv, arg,
                                                    "--paimon_source_parquet", &arg_index,
                                                    &options.source_data_file)) {
            continue;
        }
        if (paimon::benchmark::ParseStringOptionArg(parsed_argc, argv, arg,
                                                    "--paimon_external_table_path", &arg_index,
                                                    &options.external_table_path)) {
            continue;
        }
        if (paimon::benchmark::ParseStringOptionArg(parsed_argc, argv, arg, "--paimon_file_format",
                                                    &arg_index, &options.file_format)) {
            continue;
        }
        if (paimon::benchmark::ParseStringOptionArg(parsed_argc, argv, arg,
                                                    "--paimon_source_batch_max_rows", &arg_index,
                                                    &parsed_value)) {
            options.source_batch_max_rows =
                ParsePositiveInt64(parsed_value, "--paimon_source_batch_max_rows");
            continue;
        }
        if (paimon::benchmark::ParseStringOptionArg(parsed_argc, argv, arg,
                                                    "--paimon_row_to_batch_thread_number",
                                                    &arg_index, &parsed_value)) {
            options.row_to_batch_thread_number =
                ParsePositiveInt32(parsed_value, "--paimon_row_to_batch_thread_number");
            continue;
        }
        if (paimon::benchmark::ParseCsvOptionArg(parsed_argc, argv, arg, "--paimon_pk_columns",
                                                 &arg_index, &options.pk_columns)) {
            continue;
        }
        if (paimon::benchmark::ParseDelimitedRepeatableOptionArg(
                parsed_argc, argv, arg, "--paimon_option", &arg_index, &options.extra_options)) {
            continue;
        }

        argv[write_index++] = argv[arg_index];
    }

    *argc = write_index;
    argv[write_index] = nullptr;
}

bool HasHelpFlagImpl(int argc, char** argv) {
    for (int32_t arg_index = 1; arg_index < argc; ++arg_index) {
        const std::string arg(argv[arg_index]);
        if (arg == "-h" || arg == "--help" || arg == "--help=true") {
            return true;
        }
    }
    return false;
}

void PrintPaimonBenchmarkCliHelpImpl() {
    std::cout << "Paimon benchmark custom options:\n"
              << "  --paimon_source_data_file=<path>\n"
              << "      Required. External source data file used to build benchmark data.\n"
              << "      Currently supports Parquet source files.\n"
              << "      Also supports: --paimon_source_data_file <path>\n"
              << "      Deprecated alias: --paimon_source_parquet\n"
              << "  --paimon_external_table_path=<path>\n"
              << "      Optional for BM_Read and BM_MOR_Read. If set, read directly from existing\n"
              << "      table path and skip source file loading and pre-write stage.\n"
              << "      Also supports: --paimon_external_table_path <path>\n"
              << "  --paimon_file_format=<parquet|orc>\n"
              << "      Optional. Target table file format. Default: parquet.\n"
              << "      Also supports: --paimon_file_format <parquet|orc>\n"
              << "  --paimon_source_batch_max_rows=<rows>\n"
              << "      Optional. Max rows per source batch. Default: 4096.\n"
              << "  --paimon_row_to_batch_thread_number=<threads>\n"
              << "      Optional. Row-to-batch thread number for reads. Default: 3.\n"
              << "  --paimon_pk_columns=<col1,col2,...>\n"
              << "      Required by BM_PK_Write and BM_MOR_Read.\n"
              << "      Also supports: --paimon_pk_columns <col1,col2,...>\n"
              << "  --paimon_option=<key1>:<value1>;<key2>:<value2>\n"
              << "      Optional and repeatable. Pass through table options as-is.\n"
              << "      Also supports: --paimon_option <key1>:<value1>;<key2>:<value2>\n"
              << "      Note: use quotes in shell, e.g. \"--paimon_option k1:v1;k2:v2\".\n"
              << "\n"
              << "Example:\n"
              << "  paimon-read-write-benchmark --paimon_source_data_file /path/data.parquet \\\n"
              << "      --paimon_file_format parquet --paimon_pk_columns=id \\\n"
              << "      --paimon_option \"read.batch-size:8192\" --benchmark_filter=BM_Read\n"
              << std::endl;
}

struct BenchmarkWorkspace {
    explicit BenchmarkWorkspace(const std::string& prefix) {
        std::error_code ec;
        const std::filesystem::path temp_dir = std::filesystem::temp_directory_path(ec);
        if (ec) {
            throw std::runtime_error("failed to get system temp directory: " + ec.message());
        }

        const std::filesystem::path workspace_dir =
            temp_dir / (prefix + "_" + std::to_string(NextId()));
        root_path = workspace_dir.string();
        EnsureDirectory(workspace_dir);
    }

    ~BenchmarkWorkspace() {
        std::error_code ec;
        std::filesystem::remove_all(std::filesystem::path(root_path), ec);
    }

    std::string root_path;

 private:
    static void EnsureDirectory(const std::filesystem::path& path) {
        std::error_code ec;
        std::filesystem::create_directories(path, ec);
        if (ec) {
            throw std::runtime_error("failed to create benchmark workspace: " + path.string() +
                                     ", error=" + ec.message());
        }
    }

    static uint64_t NextId() {
        static std::atomic<uint64_t> id{0};
        return ++id;
    }
};

uint64_t NextTableId() {
    static std::atomic<uint64_t> id{0};
    return ++id;
}

std::string RequirePath(const std::string& root_path, const std::string& db_name,
                        const std::string& table_name) {
    return root_path + "/" + db_name + ".db/" + table_name;
}

template <typename T>
T ValueOrThrow(paimon::Result<T>&& result, const std::string& context) {
    if (!result.ok()) {
        throw std::runtime_error(context + ": " + result.status().ToString());
    }
    return std::move(result).value();
}

void CheckStatus(const paimon::Status& status, const std::string& context) {
    if (!status.ok()) {
        throw std::runtime_error(context + ": " + status.ToString());
    }
}

void SkipWithMessage(::benchmark::State& state, const std::string& message) {
    static thread_local std::string owned_message;
    owned_message = message;
    state.SkipWithError(owned_message.c_str());
}

std::string GetFileFormatFromEnv() {
    std::string file_format = GetBenchmarkCliOptions().file_format;
    for (const auto& kv : GetBenchmarkCliOptions().extra_options) {
        if (kv.first == paimon::Options::FILE_FORMAT) {
            file_format = kv.second;
        }
    }
    return file_format;
}

bool IsFileFormatSupported(const std::string& format) {
    if (format == "parquet") {
        return true;
    }
    if (format == "orc") {
#ifdef PAIMON_ENABLE_ORC
        return true;
#else
        return false;
#endif
    }
    return false;
}

void ApplyExtraOptions(std::map<std::string, std::string>* options) {
    for (const auto& kv : GetBenchmarkCliOptions().extra_options) {
        (*options)[kv.first] = kv.second;
    }
}

std::map<std::string, std::string> BuildOptions(const std::string& file_format) {
    std::map<std::string, std::string> options = {
        {paimon::Options::FILE_FORMAT, file_format},
    };
    ApplyExtraOptions(&options);
    return options;
}

std::map<std::string, std::string> BuildPkOptions(const std::string& file_format) {
    auto options = BuildOptions(file_format);
    options.emplace(paimon::Options::BUCKET, "1");
    options.emplace(paimon::Options::MERGE_ENGINE, "deduplicate");
    return options;
}

std::string GetSourceDataFilePath() {
    return GetBenchmarkCliOptions().source_data_file;
}

std::string GetExternalTablePath() {
    return GetBenchmarkCliOptions().external_table_path;
}

const std::vector<std::string>& GetPkColumns() {
    return GetBenchmarkCliOptions().pk_columns;
}

SourceDataSpec GetSourceDataSpec() {
    const std::string source_data_file_path = GetSourceDataFilePath();
    if (!source_data_file_path.empty()) {
        return {"parquet", source_data_file_path};
    }
    return {"", ""};
}

int64_t GetSourceBatchMaxRows() {
    return GetBenchmarkCliOptions().source_batch_max_rows;
}

int32_t GetRowToBatchThreadNumber() {
    return GetBenchmarkCliOptions().row_to_batch_thread_number;
}

bool SupportsParquetSourceDataMode() {
#if PAIMON_BENCHMARK_HAS_PARQUET_READER
    return true;
#else
    return false;
#endif
}

bool SupportsSourceDataMode(const std::string& source_format) {
    if (source_format == "parquet") {
        return SupportsParquetSourceDataMode();
    }
    return false;
}

struct ParquetSourceCache {
    std::string path;
    int64_t batch_max_rows = 0;
    std::shared_ptr<arrow::Schema> schema;
    DataBatches batches;
    int64_t total_rows = 0;
};

struct SourceDataCache {
    std::shared_ptr<arrow::Schema> schema;
    const DataBatches* batches = nullptr;
    int64_t total_rows = 0;
    std::string format;
    std::string path;
};

std::shared_ptr<arrow::StructArray> BuildStructArrayFromRecordBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    return std::make_shared<arrow::StructArray>(arrow::struct_(batch->schema()->fields()),
                                                batch->num_rows(), batch->columns());
}

const ParquetSourceCache& LoadParquetSource(const std::string& path) {
    const int64_t batch_max_rows = GetSourceBatchMaxRows();
    static ParquetSourceCache cache;
    if (cache.path == path && cache.batch_max_rows == batch_max_rows) {
        return cache;
    }

#if !PAIMON_BENCHMARK_HAS_PARQUET_READER
    throw std::runtime_error(
        "Parquet source data mode requires parquet::arrow reader support in this build");
#else
    auto input = arrow::io::ReadableFile::Open(path);
    if (!input.ok()) {
        throw std::runtime_error("open Parquet source failed: " + path + ", " +
                                 input.status().ToString());
    }

    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    const auto open_status = parquet::arrow::OpenFile(
        input.ValueUnsafe(), arrow::default_memory_pool(), &parquet_reader);
    if (!open_status.ok()) {
        throw std::runtime_error("create Parquet reader failed: " + open_status.ToString());
    }

    std::shared_ptr<arrow::Table> table;
    const auto read_status = parquet_reader->ReadTable(&table);
    if (!read_status.ok()) {
        throw std::runtime_error("read Parquet source failed: " + read_status.ToString());
    }

    if (table->num_rows() <= 0) {
        throw std::runtime_error("Parquet source is empty: " + path);
    }

    DataBatches batches;
    int64_t total_rows = 0;
    arrow::TableBatchReader batch_reader(*table);
    batch_reader.set_chunksize(batch_max_rows);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    while (true) {
        const auto read_batch_status = batch_reader.ReadNext(&record_batch);
        if (!read_batch_status.ok()) {
            throw std::runtime_error("split Parquet table into batches failed: " +
                                     read_batch_status.ToString());
        }
        if (record_batch == nullptr) {
            break;
        }
        if (record_batch->num_rows() <= 0) {
            continue;
        }
        batches.push_back(BuildStructArrayFromRecordBatch(record_batch));
        total_rows += record_batch->num_rows();
    }

    if (batches.empty() || total_rows <= 0) {
        throw std::runtime_error("Parquet source has no non-empty batches: " + path);
    }

    cache.path = path;
    cache.batch_max_rows = batch_max_rows;
    cache.schema = table->schema();
    cache.batches = std::move(batches);
    cache.total_rows = total_rows;
    return cache;
#endif
}

SourceDataCache LoadSourceData(const SourceDataSpec& source_spec) {
    if (source_spec.format == "parquet") {
        const auto& source = LoadParquetSource(source_spec.path);
        return {source.schema, &source.batches, source.total_rows, source_spec.format,
                source_spec.path};
    }
    throw std::runtime_error("unknown source format: " + source_spec.format);
}

std::shared_ptr<arrow::Schema> BuildSchema(const SourceDataSpec& source_spec) {
    return LoadSourceData(source_spec).schema;
}
DataBatches BuildDataBatches(const SourceDataSpec& source_spec) {
    const auto source = LoadSourceData(source_spec);
    if (source.batches == nullptr || source.batches->empty() || source.total_rows <= 0) {
        throw std::runtime_error("source file has no non-empty data batches");
    }

    std::cout << "[benchmark][source] format=" << source.format << ", path=" << source.path
              << ", source_rows=" << source.total_rows
              << ", generated_data_batches=" << source.batches->size() << std::endl;
    return *source.batches;
}

std::unique_ptr<paimon::RecordBatch> MakeRecordBatch(
    const std::shared_ptr<arrow::StructArray>& arr) {
    ArrowArray c_array;
    if (!arrow::ExportArray(*arr, &c_array).ok()) {
        throw std::runtime_error("failed to export arrow array");
    }
    paimon::RecordBatchBuilder builder(&c_array);
    builder.SetBucket(0);
    return ValueOrThrow(builder.Finish(), "build paimon record batch");
}

void EnsureTable(const std::string& root_path, const std::string& db_name,
                 const std::string& table_name, const std::map<std::string, std::string>& options,
                 const std::shared_ptr<arrow::Schema>& schema,
                 const std::vector<std::string>& primary_keys = {}) {
    auto catalog = ValueOrThrow(paimon::Catalog::Create(root_path, options), "create catalog");
    CheckStatus(catalog->CreateDatabase(db_name, options, true), "create database");

    ArrowSchema c_schema;
    if (!arrow::ExportSchema(*schema, &c_schema).ok()) {
        throw std::runtime_error("failed to export table schema");
    }
    CheckStatus(catalog->CreateTable(paimon::Identifier(db_name, table_name), &c_schema,
                                     /*partition_keys=*/{}, primary_keys, options,
                                     /*ignore_if_exists=*/false),
                "create table");
}

void WriteAndCommit(const std::string& table_path,
                    const std::map<std::string, std::string>& options,
                    const DataBatches& data_batches) {
    paimon::WriteContextBuilder write_builder(table_path, "benchmark-writer");
    auto write_ctx =
        ValueOrThrow(write_builder.SetOptions(options).Finish(), "create write context");
    auto writer = ValueOrThrow(paimon::FileStoreWrite::Create(std::move(write_ctx)),
                               "create file store writer");

    for (const auto& data : data_batches) {
        auto batch = MakeRecordBatch(data);
        CheckStatus(writer->Write(std::move(batch)), "write batch");
    }
    auto messages = ValueOrThrow(writer->PrepareCommit(), "prepare commit");

    paimon::CommitContextBuilder commit_builder(table_path, "benchmark-writer");
    auto commit_ctx =
        ValueOrThrow(commit_builder.SetOptions(options).Finish(), "create commit context");
    auto committer =
        ValueOrThrow(paimon::FileStoreCommit::Create(std::move(commit_ctx)), "create committer");
    CheckStatus(committer->Commit(messages), "commit write");
}

struct SharedReadTableCache {
    std::string key;
    std::unique_ptr<BenchmarkWorkspace> workspace;
    std::string table_path;
    int64_t total_rows = 0;
};

struct SharedMorReadTableCache {
    std::string key;
    std::unique_ptr<BenchmarkWorkspace> workspace;
    std::string table_path;
    int64_t total_rows = 0;
};

std::string BuildReadTableCacheKey(const std::string& file_format,
                                   const SourceDataSpec& source_spec) {
    return file_format + "|" + source_spec.format + "|" + source_spec.path + "|" +
           std::to_string(GetSourceBatchMaxRows());
}

std::string JoinColumns(const std::vector<std::string>& columns) {
    std::string joined;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            joined.append(",");
        }
        joined.append(columns[i]);
    }
    return joined;
}

const SharedMorReadTableCache& GetOrCreateSharedMorReadTable(const std::string& file_format,
                                                             const SourceDataSpec& source_spec) {
    static SharedMorReadTableCache cache;
    static std::mutex cache_mutex;

    const std::vector<std::string>& pk_columns = GetPkColumns();
    const std::string cache_key =
        BuildReadTableCacheKey(file_format, source_spec) + "|pk=" + JoinColumns(pk_columns);
    std::lock_guard<std::mutex> lock(cache_mutex);
    if (cache.workspace != nullptr && cache.key == cache_key) {
        std::cout << "[benchmark][mor-read] reuse_output_table_path=" << cache.table_path
                  << std::endl;
        return cache;
    }

    auto options = BuildPkOptions(file_format);
    const auto source = LoadSourceData(source_spec);
    auto schema = BuildSchema(source_spec);
    auto data_batches = BuildDataBatches(source_spec);

    auto workspace = std::make_unique<BenchmarkWorkspace>("paimon_mor_read_bench_shared");
    const std::string db_name = "bench_db";
    const std::string table_name = "mor_read_shared_" + std::to_string(NextTableId());
    EnsureTable(workspace->root_path, db_name, table_name, options, schema,
                /*primary_keys=*/pk_columns);
    const std::string table_path = RequirePath(workspace->root_path, db_name, table_name);
    std::cout << "[benchmark][mor-read] create_shared_output_table_path=" << table_path
              << std::endl;
    WriteAndCommit(table_path, options, data_batches);

    cache.key = cache_key;
    cache.workspace = std::move(workspace);
    cache.table_path = table_path;
    cache.total_rows = source.total_rows;
    return cache;
}

const SharedReadTableCache& GetOrCreateSharedReadTable(const std::string& file_format,
                                                       const SourceDataSpec& source_spec) {
    static SharedReadTableCache cache;
    static std::mutex cache_mutex;

    const std::string cache_key = BuildReadTableCacheKey(file_format, source_spec);
    std::lock_guard<std::mutex> lock(cache_mutex);
    if (cache.workspace != nullptr && cache.key == cache_key) {
        std::cout << "[benchmark][read] reuse_output_table_path=" << cache.table_path << std::endl;
        return cache;
    }

    auto options = BuildOptions(file_format);
    const auto source = LoadSourceData(source_spec);
    auto schema = BuildSchema(source_spec);
    auto data_batches = BuildDataBatches(source_spec);

    auto workspace = std::make_unique<BenchmarkWorkspace>("paimon_read_bench_shared");
    const std::string db_name = "bench_db";
    const std::string table_name = "read_shared_" + std::to_string(NextTableId());
    EnsureTable(workspace->root_path, db_name, table_name, options, schema);
    const std::string table_path = RequirePath(workspace->root_path, db_name, table_name);
    std::cout << "[benchmark][read] create_shared_output_table_path=" << table_path << std::endl;
    WriteAndCommit(table_path, options, data_batches);

    cache.key = cache_key;
    cache.workspace = std::move(workspace);
    cache.table_path = table_path;
    cache.total_rows = source.total_rows;
    return cache;
}

int64_t ReadRows(const std::string& table_path, const std::map<std::string, std::string>& options,
                 int32_t prefetch_parallel_num) {
    paimon::ScanContextBuilder scan_builder(table_path);
    auto scan_ctx = ValueOrThrow(scan_builder.SetOptions(options).Finish(), "create scan context");
    auto scanner = ValueOrThrow(paimon::TableScan::Create(std::move(scan_ctx)), "create scanner");
    auto plan = ValueOrThrow(scanner->CreatePlan(), "create plan");

    paimon::ReadContextBuilder read_builder(table_path);
    constexpr int32_t kPrefetchBatchCount = 600;
    read_builder.SetOptions(options)
        .EnablePrefetch(true)
        .SetPrefetchBatchCount(kPrefetchBatchCount)
        .SetPrefetchMaxParallelNum(prefetch_parallel_num)
        .EnableMultiThreadRowToBatch(GetRowToBatchThreadNumber() > 1)
        .SetRowToBatchThreadNumber(GetRowToBatchThreadNumber());
    auto read_ctx = ValueOrThrow(read_builder.Finish(), "create read context");
    auto reader =
        ValueOrThrow(paimon::TableRead::Create(std::move(read_ctx)), "create table reader");
    auto batch_reader = ValueOrThrow(reader->CreateReader(plan->Splits()), "create batch reader");

    int64_t total_rows = 0;
    while (true) {
        auto batch = ValueOrThrow(batch_reader->NextBatch(), "read next batch");
        if (paimon::BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [array, schema] = batch;
        auto imported = arrow::ImportArray(array.get(), schema.get());
        if (!imported.ok()) {
            throw std::runtime_error("import c data array failed: " + imported.status().ToString());
        }
        total_rows += imported.ValueUnsafe()->length();
    }

    return total_rows;
}

struct PreparedSourceData {
    std::shared_ptr<arrow::Schema> schema;
    DataBatches data_batches;
    int64_t total_rows = 0;
};

bool TryGetSourceSpec(::benchmark::State& state, SourceDataSpec* source_spec) {
    try {
        *source_spec = GetSourceDataSpec();
        return true;
    } catch (const std::exception& e) {
        SkipWithMessage(state, e.what());
        return false;
    }
}

bool TryPrepareSourceData(::benchmark::State& state, const SourceDataSpec& source_spec,
                          PreparedSourceData* prepared) {
    try {
        prepared->total_rows = LoadSourceData(source_spec).total_rows;
        prepared->schema = BuildSchema(source_spec);
        prepared->data_batches = BuildDataBatches(source_spec);
        return true;
    } catch (const std::exception& e) {
        SkipWithMessage(state, e.what());
        return false;
    }
}

}  // namespace

void ParsePaimonBenchmarkCliArgs(int* argc, char** argv) {
    ParsePaimonBenchmarkCliArgsImpl(argc, argv);
}

bool HasHelpFlag(int argc, char** argv) {
    return HasHelpFlagImpl(argc, argv);
}

void PrintPaimonBenchmarkCliHelp() {
    PrintPaimonBenchmarkCliHelpImpl();
}

void RunBMWrite(::benchmark::State& state) {
    const std::string file_format = GetFileFormatFromEnv();
    SourceDataSpec source_spec;
    if (!TryGetSourceSpec(state, &source_spec)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateSourcePresenceOrSkip(
            state, source_spec.path, "--paimon_source_data_file is required", &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateSourceSupportOrSkip(state, source_spec.format,
                                                       SupportsSourceDataMode(source_spec.format),
                                                       &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateFileFormatOrSkip(
            state, file_format, IsFileFormatSupported(file_format), &SkipWithMessage)) {
        return;
    }

    auto options = BuildOptions(file_format);
    PreparedSourceData prepared;
    if (!TryPrepareSourceData(state, source_spec, &prepared)) {
        return;
    }
    BenchmarkWorkspace workspace("paimon_write_bench");

    for (auto _ : state) {
        const std::string db_name = "bench_db";
        const std::string table_name = "write_" + std::to_string(NextTableId());
        EnsureTable(workspace.root_path, db_name, table_name, options, prepared.schema);
        const std::string table_path = RequirePath(workspace.root_path, db_name, table_name);
        std::cout << "[benchmark][write] output_table_path=" << table_path << std::endl;
        WriteAndCommit(table_path, options, prepared.data_batches);
    }

    state.SetItemsProcessed(state.iterations() * prepared.total_rows);
}

void RunBMRead(::benchmark::State& state) {
    const auto prefetch_parallel_num = static_cast<int32_t>(state.range(0));
    const std::string file_format = GetFileFormatFromEnv();
    const std::string external_table_path = GetExternalTablePath();
    SourceDataSpec source_spec;
    if (!TryGetSourceSpec(state, &source_spec)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateFileFormatOrSkip(
            state, file_format, IsFileFormatSupported(file_format), &SkipWithMessage)) {
        return;
    }

    if (!BenchmarkHelpers::ValidatePrefetchParallelOrSkip(state, prefetch_parallel_num,
                                                          &SkipWithMessage)) {
        return;
    }

    auto options = BuildOptions(file_format);

    if (BenchmarkHelpers::TryRunExternalReadMode(state, "read", external_table_path, [&]() {
            return ReadRows(external_table_path, options, prefetch_parallel_num);
        })) {
        return;
    }

    if (!BenchmarkHelpers::ValidateSourcePresenceOrSkip(
            state, source_spec.path,
            "--paimon_source_data_file is required when --paimon_external_table_path is not set",
            &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateSourceSupportOrSkip(state, source_spec.format,
                                                       SupportsSourceDataMode(source_spec.format),
                                                       &SkipWithMessage)) {
        return;
    }

    const SharedReadTableCache* shared_table = nullptr;
    try {
        shared_table = &GetOrCreateSharedReadTable(file_format, source_spec);
    } catch (const std::exception& e) {
        SkipWithMessage(state, e.what());
        return;
    }

    const int64_t rows_read = BenchmarkHelpers::RunReadIterations(state, [&]() {
        return ReadRows(shared_table->table_path, options, prefetch_parallel_num);
    });

    state.SetItemsProcessed(state.iterations() * rows_read);
}

void RunBMPkWrite(::benchmark::State& state) {
    const std::string file_format = GetFileFormatFromEnv();
    SourceDataSpec source_spec;
    if (!TryGetSourceSpec(state, &source_spec)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateSourcePresenceOrSkip(
            state, source_spec.path, "--paimon_source_data_file is required", &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateSourceSupportOrSkip(state, source_spec.format,
                                                       SupportsSourceDataMode(source_spec.format),
                                                       &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateFileFormatOrSkip(
            state, file_format, IsFileFormatSupported(file_format), &SkipWithMessage)) {
        return;
    }
    const std::vector<std::string>& pk_columns = GetPkColumns();
    if (pk_columns.empty()) {
        SkipWithMessage(state, "--paimon_pk_columns is required for BM_PK_Write");
        return;
    }

    auto options = BuildPkOptions(file_format);
    PreparedSourceData prepared;
    if (!TryPrepareSourceData(state, source_spec, &prepared)) {
        return;
    }
    BenchmarkWorkspace workspace("paimon_pk_write_bench");

    for (auto _ : state) {
        const std::string db_name = "bench_db";
        const std::string table_name = "pk_write_" + std::to_string(NextTableId());
        EnsureTable(workspace.root_path, db_name, table_name, options, prepared.schema,
                    /*primary_keys=*/pk_columns);
        const std::string table_path = RequirePath(workspace.root_path, db_name, table_name);
        std::cout << "[benchmark][pk-write] output_table_path=" << table_path << std::endl;
        WriteAndCommit(table_path, options, prepared.data_batches);
    }

    state.SetItemsProcessed(state.iterations() * prepared.total_rows);
}

void RunBMMorRead(::benchmark::State& state) {
    const auto prefetch_parallel_num = static_cast<int32_t>(state.range(0));
    const std::string file_format = GetFileFormatFromEnv();
    const std::string external_table_path = GetExternalTablePath();
    SourceDataSpec source_spec;
    if (!TryGetSourceSpec(state, &source_spec)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateFileFormatOrSkip(
            state, file_format, IsFileFormatSupported(file_format), &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidatePrefetchParallelOrSkip(state, prefetch_parallel_num,
                                                          &SkipWithMessage)) {
        return;
    }

    const auto external_read_options = BuildOptions(file_format);
    if (BenchmarkHelpers::TryRunExternalReadMode(state, "mor-read", external_table_path, [&]() {
            return ReadRows(external_table_path, external_read_options, prefetch_parallel_num);
        })) {
        return;
    }

    if (!BenchmarkHelpers::ValidateSourcePresenceOrSkip(
            state, source_spec.path,
            "--paimon_source_data_file is required when --paimon_external_table_path is not set",
            &SkipWithMessage)) {
        return;
    }
    if (!BenchmarkHelpers::ValidateSourceSupportOrSkip(state, source_spec.format,
                                                       SupportsSourceDataMode(source_spec.format),
                                                       &SkipWithMessage)) {
        return;
    }
    if (GetPkColumns().empty()) {
        SkipWithMessage(state, "--paimon_pk_columns is required for BM_MOR_Read");
        return;
    }

    auto options = BuildPkOptions(file_format);
    const SharedMorReadTableCache* shared_table = nullptr;
    try {
        shared_table = &GetOrCreateSharedMorReadTable(file_format, source_spec);
    } catch (const std::exception& e) {
        SkipWithMessage(state, e.what());
        return;
    }

    const int64_t rows_read = BenchmarkHelpers::RunReadIterations(state, [&]() {
        return ReadRows(shared_table->table_path, options, prefetch_parallel_num);
    });
    state.SetItemsProcessed(state.iterations() * rows_read);
}

}  // namespace paimon::benchmark
