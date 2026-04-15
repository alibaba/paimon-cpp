# Quantization Parameters and Tuning

## Overview

This guide explains how generic quantization algorithms are configured in Lumina. It focuses on the
`BuilderOptions` keys under `encoding.type`, `encoding.pq.*`, and `encoding.rabitq.*`. The content
is aligned with the current release's implementation , including parameter meanings and tuning guidance.

This is an implementation-oriented usage guide, not a separate long-term compatibility contract.

## Audience

- Developers choosing or tuning vector encodings for vector indexes.
- Developers comparing quality, memory usage, and latency across `rawf32`, `sq8`, `pq`, and `rabitq`.

## Quantization Builder Parameters

Builder parameters only take effect during index build.

### Main Quantization Parameters

These keys belong to `api::BuilderOptions`.

| Key | Type | Effect |
| --- | --- | --- |
| `encoding.type` | `FieldType::kString` | Selects the quantization algorithm. Supported values are `rawf32`, `sq8`, `pq`, and `rabitq`. |
| `encoding.pq.*` | see next section | PQ-related parameters. |
| `encoding.rabitq.*` | see next section | RabitQ-related parameters. |

If `encoding.type` is not set:

- it usually defaults to `rawf32`;
- DiskANN is an exception and currently defaults its in-memory quantizer type to `pq`.

If `encoding.type` is set:

- `rawf32` and `sq8` currently have no extra user-facing algorithm parameters;
- `pq` uses `encoding.pq.*`;
- `rabitq` uses `encoding.rabitq.*`.

### Per-Algorithm Parameters

For `rawf32` and `sq8`:

- `rawf32` stores original float vectors without quantization.
- `sq8` applies 8-bit scalar quantization based on training data.
- Neither encoding currently has extra user-facing algorithm parameters.
- Both encodings currently support `l2`, `inner_product`, and `cosine`.

`pq` is configured by `encoding.pq.*`.

| Key | Type | Effect |
| --- | --- | --- |
| `encoding.pq.m` | `FieldType::kInt` | Number of subspaces. Larger `m` usually improves fidelity, but also increases code size and training cost. |
| `encoding.pq.thread_count` | `FieldType::kInt` | Parallelism for PQ training and encoding. |
| `encoding.pq.max_epoch` | `FieldType::kInt` | KMeans iteration limit for plain PQ. |
| `encoding.pq.use_opq` | `FieldType::kBool` | Enables OPQ rotation. |
| `encoding.pq.make_zero_mean` | `FieldType::kBool` | Centers training data before plain L2 PQ. |

Current behavior:

- `encoding.pq.m` must satisfy `0 < m <= dimension`.
- If `encoding.pq.use_opq` is enabled and `encoding.pq.max_epoch` is not set explicitly, the default is treated as `8`.
- If `distance.metric = inner_product`, `encoding.pq.make_zero_mean` is disabled.
- If `encoding.pq.use_opq` is enabled, `encoding.pq.make_zero_mean` is also disabled.
- `pq` currently supports `l2` and `inner_product`, but not `cosine`.

`rabitq` is configured by `encoding.rabitq.*`.

| Key | Type | Effect |
| --- | --- | --- |
| `encoding.rabitq.quantized_bit_count` | `FieldType::kInt` | Supported values are `1`, `4`, `5`, `8`, and `9`. Larger values usually improve fidelity. |
| `encoding.rabitq.centroid_count` | `FieldType::kInt` | Number of centroids used during pretraining. |
| `encoding.rabitq.thread_count` | `FieldType::kInt` | Parallelism for KMeans training. |
| `encoding.rabitq.max_epoch` | `FieldType::kInt` | KMeans iteration limit. |

Current behavior:

- `distance.metric` must be `l2`.
- `dimension` must be a multiple of `64`.
- Unsupported bit counts fail immediately.
- `rabitq` currently supports `l2` only.

## Tuning Notes

- `Memory usage`: `rawf32` uses the most memory. `sq8` is usually the safer compressed alternative. `pq` and
  `rabitq` can usually reduce memory further, but they also come with stricter constraints and more tuning overhead.
  If you want a clear memory reduction without adding much complexity, `sq8` is usually the first option to try.
- `Computation speed`: if distance evaluation on encoded vectors becomes the query-side bottleneck, consider `sq8`,
  `pq`, or `rabitq`. Benchmarks for different quantized distance paths are available under `test/impl/quantizer` for
  reference.
- `Build time`: if you care more about build speed and configuration simplicity, prefer `rawf32` or `sq8`.
  `encoding.pq.thread_count` and `encoding.rabitq.thread_count` mainly improve build throughput. Increasing
  `encoding.pq.max_epoch`, `encoding.rabitq.max_epoch`, and `encoding.rabitq.centroid_count` also increases build
  time, so raise them only when needed. `encoding.pq.use_opq` usually makes build slower as well, so it is better
  considered after plain PQ.
- `Accuracy`: start with `rawf32` as the baseline. When compression is needed, `sq8` is usually the safer first step.
  For `pq`, start with `encoding.pq.m`; if accuracy is still not enough, then consider enabling `encoding.pq.use_opq`.
  `encoding.pq.make_zero_mean` is only worth trying for L2 with OPQ disabled. For `rabitq`, start from the default
  `quantized_bit_count = 4`; if you need higher fidelity, raise `quantized_bit_count` first, then consider increasing
  `centroid_count`.

## Current Caveats

- If `encoding.type` is omitted, it usually falls back to `rawf32`; DiskANN is the exception and defaults its
  in-memory quantizer to `pq`.
- `pq` currently supports `l2` and `inner_product`, but not `cosine`.
- `rabitq` supports `l2` only, and `dimension` must be a multiple of `64`.
- When using `rabitq`, it is safer to pass all required `encoding.rabitq.*` parameters explicitly instead of relying on
  implicit defaults or automatic conversions.
- DiskANN has its own defaulting rules for disk quantization; see [DiskANN Parameters and Tuning](DiskANNParameters.md).

## Status

v0.2.1 Release Tag (2026-04-07).
