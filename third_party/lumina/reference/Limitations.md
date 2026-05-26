# Limitations

Use this page to check the known limitations and constraints of the current Lumina release before choosing a backend or
upgrading persisted indexes.

## 1. Feature limitations

### Backend support

- **IVF distance metric**: the IVF backend currently supports `L2` only. `Cosine` and `InnerProduct` are under
  development.
- **DiskANN dynamic updates**: DiskANN currently supports offline build and static search only (no incremental
  insert/delete).
- **Bruteforce scale**: the Bruteforce backend is not optimized for extremely large datasets; use it mainly as a
  baseline or for smaller scales.

### Data model

- **Fixed dimension**: vector dimension must stay consistent between build and search; dynamic dimension is not
  supported.
- **ID type**: `vector_id_t` is `uint64_t`.

## 2. Performance & resources

### Memory usage

- **Sampling in `PretrainFrom`**: training may sample vectors via `index.pretrain_sample_ratio`; high ratios can
  increase memory pressure.
- **Streaming ingestion**: `InsertFrom(Dataset&)` reads batch-by-batch, but backends (Bruteforce/IVF) still allocate
  memory based on algorithm needs.

### Concurrency

- **Builder is not thread-safe**: `LuminaBuilder` must be used from a single thread or externally synchronized.
- **Global executor**: internal thread pool size is controlled globally via `LUMINA_EXECUTOR_THREAD_COUNT`.

## 3. IO & persistence

### File format compatibility

- **Format versioning**: Stable Lumina persisted artifacts (`.lmi`) are versioned by the major version, meaning the
  first segment in semantic versions such as `1.x.y` to `2.x.y`. Major-version upgrades may break binary compatibility
  and require rebuilding indexes. For stable (non-experimental) index formats, minor and patch upgrades within the same
  major version remain compatible and do not require rebuilds solely because of the version upgrade.
  The IVF snapshot layout is experimental.
- **CRC verification cost**: enabling section CRC verification (`io.verify_crc=true`) costs ~1–3% performance (file
  header/footer CRC is always verified).