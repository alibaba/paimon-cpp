# DiskANN Parameters and Tuning

## Overview

This document describes DiskANN configuration and tuning in Lumina. It focuses on builder parameters in
`BuilderOptions`, searcher parameters in `SearcherOptions`, and per-query parameters in `SearchOptions`. The content
is aligned with the current release's implementation , including parameter meanings and tuning guidance.

This is an implementation-oriented usage guide, not a separate long-term compatibility contract.

## Audience

- Developers building or querying DiskANN indexes through the C++ API.
- Developers building or querying DiskANN indexes through the Python wrapper.

## Builder Parameters

Builder parameters only take effect during index build.

### Main Parameters

These keys belong to `api::BuilderOptions`.

| Key | Type | Effect |
| --- | --- | --- |
| `diskann.build.thread_count` | `FieldType::kInt` | Parallelism during build. If PQ thread count is not configured separately, this value is also used as the default PQ thread count. |
| `diskann.build.ef_construction` | `FieldType::kInt` | Candidate list size during graph construction. Larger values usually improve graph quality, but also increase build cost. |
| `diskann.build.neighbor_count` | `FieldType::kInt` | Maximum number of neighbors kept for each node. Larger values increase graph size and build cost. |
| `diskann.build.slack_pruning_factor` | `FieldType::kDouble` | Pruning factor during graph construction. Must be `> 0`. |
| `diskann.build.reorder_layout` | `FieldType::kBool` | Whether to reorder the on-disk layout to improve locality. It only takes effect when the final layout can place at least 2 nodes in one sector. |
| `diskann.build.quantized_build` | `FieldType::kBool` | Uses the in-memory quantizer to compute distances during graph construction. Currently only the `pq` quantizer is supported. |
| `diskann.disk_encoding.*` | see "Disk Quantization Parameters" section | Disk quantization parameters. |

### In-Memory Quantization Parameters

In-memory quantization is a generic configuration and is not specific to DiskANN. For more detailed parameter
configuration, see [Quantization Parameters and Tuning](./QuantizationParameters.md). The DiskANN-specific behavior is
that in-memory quantization must always be enabled. If `encoding.type` is not set, DiskANN defaults to `pq`.

### Disk Quantization Parameters

`diskann.disk_encoding.*` configures an additional on-disk quantized representation in DiskANN. It is only used to
sort the result set by quantized distance.

If `diskann.disk_encoding.type` is not set:

- the on-disk layout contains original float vectors;
- the result set is sorted by exact distance.

If `diskann.disk_encoding.type` is set:

- the on-disk layout contains quantized vectors;
- the `searcher` keeps disk-quantization metadata in memory and reads quantized values from disk during result-set
  reranking;
- the `builder` can optionally dump an additional copy of the original vectors.

| Key | Type | Effect |
| --- | --- | --- |
| `diskann.disk_encoding.type` | `FieldType::kString` | Enables disk quantization when set. Supported values are `rawf32`, `sq8`, and `pq`. |
| `diskann.disk_encoding.save_origin_embedding` | `FieldType::kBool` | When disk quantization is enabled, whether to additionally dump original vectors. |
| `diskann.disk_encoding.encoding.pq.m` | `FieldType::kInt` | Only takes effect when disk quantization type is `pq`. It is recommended to be larger than the in-memory quantization parameter `encoding.pq.m`. |
| `diskann.disk_encoding.encoding.pq.thread_count` | `FieldType::kInt` | Only takes effect when disk quantization type is `pq`. |
| `diskann.disk_encoding.encoding.pq.max_epoch` | `FieldType::kInt` | Only takes effect when disk quantization type is `pq`. |

Current behavior:

- When `pq` is used as the disk quantization algorithm, `encoding.pq.use_opq` is forced to `false`.
- When `pq` is used as the disk quantization algorithm, `encoding.pq.make_zero_mean` is forced to `false`.
- Disk quantization inherits all restrictions of in-memory quantization. For example, `pq` does not support `cosine`.

## Searcher Parameters

Searcher parameters affect all queries handled by the same searcher.

These keys belong to `api::SearcherOptions`.

| Key | Type | Effect |
| --- | --- | --- |
| `diskann.search.num_nodes_to_cache` | `FieldType::kInt` | Number of nodes to cache during search. `0` disables caching. |
| `diskann.search.sector_aligned_read` | `FieldType::kBool` | Whether query-time IO should read in sector-aligned mode. |

Current cache behavior:

- Cache size does not exceed `10%` of the index node count.
- About `20%` of that budget is used for static cache, and the rest is used for dynamic LRU cache.
- After `diskann.search.sector_aligned_read` is enabled, cache granularity changes from "node" to "sector".

Current aligned-read behavior:

- If the loaded layout cannot fit 2 nodes in one sector, the searcher automatically disables sector-aligned read.
- `diskann.build.reorder_layout` and `diskann.search.sector_aligned_read` usually need to be tuned together.

## Query Parameters

Query parameters only affect a single query.

These keys belong to `api::SearchOptions`.

| Key | Type | Effect |
| --- | --- | --- |
| `search.topk` | `FieldType::kInt` | Required. |
| `search.parallel_number` | `FieldType::kInt` | Query parallelism. Valid range is `1..1000`; values larger than the index node count are clipped to the node count. |
| `search.thread_safe_filter` | `FieldType::kBool` | Only meaningful for filtered search. If `parallel_number > 1`, filtered search requires it to be `true`. |
| `diskann.search.list_size` | `FieldType::kInt` | Required. The effective value is raised to at least `topk` and capped by the index node count. |
| `diskann.search.io_limit` | `FieldType::kInt` | Upper bound on the number of IO operations allowed for a single query. In normal mode, it is roughly "how many nodes may be read"; with sector-aligned read, it is closer to "how many sectors may be read". The effective value is automatically aligned with `topk` and `list_size`: at least `topk`, at most `list_size`. |
| `diskann.search.beam_width` | `FieldType::kInt` | Kept in schema for compatibility, but the current DiskANN backend does not read it. |

## Tuning Notes

- Graph-build parameters should be considered together: `diskann.build.ef_construction` controls the candidate pool
  size, `diskann.build.neighbor_count` controls the final maximum number of edges kept per node, and
  `diskann.build.slack_pruning_factor` controls how loose the pruning is.
- `diskann.build.ef_construction` is usually the first parameter to tune. Larger values let the builder see more
  candidate neighbors during graph construction and usually make it easier to improve graph quality and recall; the
  cost is higher build time, more distance computations, and a larger memory working set. If this value is too small,
  raising `neighbor_count` later may still have limited benefit because the candidate pool itself is not good enough.
- `diskann.build.neighbor_count` controls the final out-degree cap. Larger values usually make the graph denser and
  search more stable, but they also directly increase index size, build cost, and graph-related data per node. It is
  usually more effective to raise it only after `ef_construction` is already sufficiently large.
- `diskann.build.slack_pruning_factor` controls pruning looseness. Larger values make pruning looser and make it
  easier for nodes to keep neighbors closer to the `neighbor_count` limit. Edge-direction diversity is also usually
  better, which often helps recall and connectivity. Smaller values make pruning more aggressive, so long edges in the
  same direction are more likely to remain. Start from the default value `1.0` and fine-tune from there.
- A common order is: first raise `diskann.build.ef_construction` to improve candidate quality, then adjust
  `diskann.build.neighbor_count` according to the index-size budget, and fine-tune
  `diskann.build.slack_pruning_factor` only when a more precise trade-off is needed.
- For an already-built graph, if you want higher recall, increase `diskann.search.list_size` first. If you want
  lower query latency, increase `search.parallel_number` first; `2-4` is a common range.
- If you want to limit per-query disk reads, tune `diskann.search.io_limit`. In practice, start from a value close to
  `list_size`, then gradually lower it based on the latency/recall trade-off.
- For workloads with many repeated queries, try `diskann.search.num_nodes_to_cache` first, and then decide whether to
  increase query parallelism further.
- For disk-locality tuning, tune `diskann.build.reorder_layout` and `diskann.search.sector_aligned_read` together.

## Current Caveats

- `diskann.search.list_size` must be passed explicitly for every query.
- `diskann.search.beam_width` has no practical effect in the current backend.
- If `encoding.type` is not set, DiskANN defaults to `pq`.
- PQ only supports `l2` and `inner_product`. For `cosine`, use `rawf32` or `sq8`.

## Status

v0.2.1 Release Tag (2026-04-07).
