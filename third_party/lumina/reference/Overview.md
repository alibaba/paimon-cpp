# Overview

Lumina is a C++ library for high-performance vector search and persisted indexes. It provides production-oriented
backends (DiskANN / IVF / Bruteforce), a narrow API surface, and extension points for advanced workflows.

In addition to the core C++ API, Lumina also provides an experimental Python interface covering index building, search, and other basic workflows.

## Why Lumina?

Lumina is designed as a production-grade search infrastructure component. Every design decision —
from the API surface to the index format — is made with long-term maintainability and operational reliability in mind.

1. **Mature, deliberate API design**
   A minimal interface with type-safe, exception-safe error handling. All backends share a unified configuration
   system — switching backends is a configuration change, not a code rewrite.

2. **Index format you can trust**
   Persisted indexes follow a versioned format with built-in integrity checks. Upgrades within a compatible
   version range do not require an index rebuild. The same format works across local storage, memory-mapped
   files, and distributed file systems.

3. **Keeps pace with research**
   Core algorithms incorporate results from recent literature — RabitQ quantization, graph pruning
   heuristics, locality-aware disk reordering — and ship as production features, not perpetual experiments.
4. **Deep C++ engineering foundation**
   Resource ownership is explicit and predictable. Memory allocation is tiered and controllable — critical
   for multi-tenant deployments. The codebase is built on modern C++ standards with strict engineering
   governance: mandatory code review, pre-commit validation, versioned release trains, and a compatibility
   policy that distinguishes stable from experimental surfaces. Every release is a deliberate, tested artifact.

5. **Pluggable IO for any storage topology**
   The IO layer accepts user-supplied readers and writers, decoupling index logic from storage. The same
   index binary can be served from local SSD, object storage, or a distributed file system without changes
   to the core library — enabling storage-compute separation and cloud-native deployments out of the box.
6. **Typed extension framework**
   Vector search in production demands capabilities beyond pure ANN — filtering, checkpointing, distributed
   builds — yet bundling them all into the core API would bloat the interface and couple unrelated concerns.
   Lumina addresses this with a typed extension layer: each capability attaches to a Builder or Searcher instance
   through a contract that specifies lifecycle ownership, thread-safety semantics, and supported backends.
   Incompatible combinations are rejected at attach time with a clear error, not discovered at query time.

   | Extension | Status |
   |-----------|--------|
   | Attribute-based filtered search | stable |
   | Build checkpointing | experimental |
   | Range & discrete-label filtering | planned |
   | Distributed build coordination | planned |

## Backends at a glance

### DiskANN

**Scale**: billions of vectors. **Memory**: sub-linear — graph metadata, quantized codes, and a configurable hot-node cache reside in RAM; full-precision or higher-precision quantized vectors stay on disk.

DiskANN builds a Vamana proximity graph offline, then serves queries through a coroutine-based parallel beam search that issues batched, sector-aligned disk reads without blocking threads on I/O. Key engineering choices:

- **Layout optimization** — After graph construction, a locality-aware reordering pass (BNP/BNF) places neighboring nodes into the same disk sector, reducing random I/O during search.
- **Two-tier caching** — A static cache (BFS-loaded entry-region nodes) absorbs the first hops; a dynamic LRU cache adapts to workload skew at runtime.
- **Build-time checkpointing** — Long builds can resume from a saved checkpoint after interruption, avoiding full restarts on billion-scale datasets.
- **Quantization** — Both in-memory and on-disk vectors support SQ8, PQ, and RabitQ encoding. The disk encoding can differ from the in-memory one, trading a small recall margin for significantly smaller index files.
- **Tag-aware graph construction** (in progress) — Filtered search with label dimensions is under active development.


### IVF

**Scale**: millions to tens of millions of vectors. **Memory**: moderate — centroids and quantized codes reside in RAM.

IVF partitions the vector space into inverted lists via k-means clustering, then searches by probing the nearest lists. Supports SQ8, PQ, and RabitQ quantization to control the memory-accuracy tradeoff. Currently supports L2 distance only; Cosine and InnerProduct are under development. The on-disk snapshot layout is experimental and may change across versions.

### Bruteforce

**Scale**: thousands to low millions of vectors. **Memory**: full dataset in RAM.

Bruteforce computes exact distances against every vector — no approximation, no index structure. Use it as a recall-rate baseline for benchmarking other backends, or in production when the dataset is small enough that linear scan meets latency requirements.

## Use cases

- **Vector database backend** — power billion-scale similarity search behind a database or retrieval service.
- **Recommendation systems** — real-time recall of similar items or users from high-dimensional embeddings.
- **Image and video search** — fast matching over visual feature vectors.
- **RAG** — give an LLM a high-performance knowledge-base retrieval layer.

## Core components

| Component | What it does |
|-----------|-------------|
| **API layer** | `LuminaBuilder`, `LuminaSearcher`, `Options`, `Query` — your main integration surface |
| **Python facade** | Experimental `lumina` package wrapping Builder/Searcher, plus a filtered-search wrapper |
| **Backends** | DiskANN, IVF, Bruteforce — the concrete index algorithms |
| **Quantizer** | Vector compression and distance estimation: SQ8, PQ, RabitQ |
| **IO system** | Binary container format with section management and CRC verification |
| **Telemetry** | Production logging and metrics hooks |
| **Extensions** | Typed build-time and search-time extension points: filtered search, checkpointing. Explicit lifecycle and thread-safety contracts |

## Our Publications

Research behind Lumina has been published at top-tier database and systems venues:

- **[SIGMOD'26]** Zhiyuan Hua, Qiji Mo, Zebin Yao, Lixiao Cui, Xiaoguang Liu, Gang Wang, Zijing Wei, Xinyu Liu, Tianxiao Tang, Shaozhi Liu, Lin Qu. *Dynamically Detect and Fix Hardness for Efficient Approximate Nearest Neighbor Search.* ACM Conference on Management of Data, 2026. ([arXiv](https://arxiv.org/abs/2510.22316))
- **[ICDE'26]** Qiji Mo, Zhiyuan Hua, Zebin Yao, Lixiao Cui, Xiaoguang Liu, Gang Wang, Zijing Wei, Xinyu Liu, Tianxiao Tang, Shaozhi Liu, Lin Qu. *Overcoming the Sync-Compute Dilemma in Parallel Graph-Based Vector Retrieval.* IEEE International Conference on Data Engineering, 2026.

## Next steps

- [Python quick start](../PythonQuickStart.md) — run the full build → dump → open → search flow in Python.
- [DiskANN tuning guide](./DiskANNParameters.md) — graph build and search parameter tuning for DiskANN.
- [Options reference](./OptionsReference.md) — complete list of configuration keys.