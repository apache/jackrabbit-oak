---
marp: true
paginate: true
style: |
  section { font-size: 18px; }
  h1 { font-size: 30px; }
  h2 { font-size: 24px; }
  table { font-size: 0.85em; }
  section > p { margin: 0.4em 0; }
---

# Record-level Compaction for Oak Segment-Stores<br><span style="font-size:0.7em">*Research Summary*</span>

Evaluation of a record-level compactor prototype, benchmarked against existing compactors.

---

## Core Idea

- **Today:** semantic compaction - *copies the entire live repo* into a new GC generation at the **NodeState** level
- **Insight:** persisted data is a *DAG of records*
- **Idea:** structural compaction - *copy reachable records to a new GC generation*
- **Caveat:** `RecordId` is `(SegmentId, recordNumber)`. Moving a record changes its id; records are not freely mobile.
- **Solution:** `RecordId` substitution using a temporary alias table
- **Bonus:** record deduplication to reduce number of records and store size

---

## Highlights

- **~1.9× faster**
- **~25% smaller and ~40% fewer records**
- **~35% less heap** - but requires memory-mapped off-heap storage

Numbers are based on a real segmentstore with a compacted size of ~823 MB (tar-files) downloaded from a customer's RDE
environment. Garbage is created artificially. Results are compared to `CheckpointCompactor` and `ParallelCompactor`.

---

## At a glance: record-repacker vs compactor

<div style="display:flex; gap:1.5rem; align-items:flex-start;">
<div>

**Compaction**

| scenario             |      time |  store size | record count |  peak heap |
|----------------------|----------:|------------:|-------------:|-----------:|
| compactor 1x         |    17.9 s |      824 MB |       16.8 M |    1508 MB |
| compactor 16x        |     8.8 s |      843 MB |       17.0 M |    1681 MB |
| record-repacker 1x   |     9.3 s |      629 MB |      10.08 M |     997 MB |
| record-repacker 16x  | **4.6 s** |      629 MB |      10.12 M |    1073 MB |

</div>
<div>

**Read locality** (cold reads, 32 MB cache — segment fetches)

| scenario            | full (distinct) | list-widest | deep-path |
|---------------------|----------------:|------------:|----------:|
| compactor 1x        |            3264 |         164 |        45 |
| compactor 16x       |            3344 |         230 |        54 |
| record-repacker 1x  |            2488 |         142 |        40 |
| record-repacker 16x |            2510 |     **152** |        46 |

</div>
</div>

**~1.9× faster · ~35% less heap · ~25% smaller · ~40% fewer records — and best-in-class read locality**

Note
`compactor` refers to `CheckpointCompactor` or `ParallelCompactor`
`record-repacker` numbers refer to the `hot-dedup+idx` configuration of the repacker

---

## The approach: `RecordRepacker`

- walk the **reachable DAG**
- read record bytes
- replace `RecordId` references in the bytes with their aliases
- write patched record bytes
- record the new `RecordId` in the alias table
 
**bounded heap:** the alias table is stored in a memory-mapped file

---

## Record deduplication

Records can be **byte-identical after reference translation** yet start as separate records.

Before writing a record's bytes, check if the same bytes are in the deduplication cache. If
present, only record an alias mapping from the old `RecordId` to the already written `RecordId`. 

Three modes benchmarked:

- **preserve** — no deduplication, only existing physical sharing preserved
- **full-dedup** — *every* content-equal record collapsed into one (exact, unbounded off-heap dedup table)
- **hot-dedup** — deduplicates only records that recur within a bounded **recency window** (memory-mapped, off-heap)

**Examples of content-equal records:**
- property-index mirrors
- empty version-storage buckets
- permission entries / ACLs

**hot-dedup is a good default** — a window of ~10% of the records recovers full-dedup's size within ~2%

---

## Compaction benchmark setup & scenarios

**Store:**
- real compacted ~823 MB segment store
- garbage injected, resulting in ~1 GB (20.6 M records, several garbage generations, 2 checkpoints)

**Comparison Matrix** = five approaches × {single-threaded, 16 threads}:

| approach          | what it is                                                               |
|-------------------|--------------------------------------------------------------------------|
| **preserve**      | repacker preserves existing record structure (physical sharing)          |
| **full-dedup**    | repacker collapsing *every* content-equal record (exact)                 |
| **hot-dedup**     | repacker collapsing content-equal records seen within a window (default) |
| **hot-dedup+idx** | like `hot-dedup`, but repacked in three stages                           |
| **compactor**     | existing `CheckpointCompactor` / `ParallelCompactor`                     |

- **16 threads (concurrent):** DAG split into sub-roots; repacked in parallel; requires shared alias and deduplication data

---

## Results: compaction (speed / size / heap)

| scenario                      |      time | store size | record count |  peak heap |
|-------------------------------|----------:|-----------:|-------------:|-----------:|
| input (uncompacted)           |         — |    1014 MB |       20.6 M |          — |
| preserve 1x                   |     7.2 s |     770 MB |       15.2 M |     955 MB |
| preserve 16x                  |     4.7 s |     872 MB |       17.7 M |    1126 MB |
| full-dedup 1x                 |     9.7 s | **617 MB** |   **9.81 M** |     983 MB |
| full-dedup 16x                |     5.1 s | **619 MB** |   **9.82 M** |    1147 MB |
| hot-dedup 1x                  |     9.3 s |     629 MB |      10.07 M |     970 MB |
| hot-dedup 16x                 | **4.3 s** |     647 MB |      10.40 M |    1149 MB |
| hot-dedup+idx 1x *(default)*  |     9.3 s |     629 MB |      10.08 M |     997 MB |
| hot-dedup+idx 16x *(default)* | **4.6 s** |     629 MB |      10.12 M |    1073 MB |
| compactor 1x (Checkpoint)     |    17.9 s |     824 MB |       16.8 M |    1508 MB |
| compactor 16x (Parallel)      |     8.8 s |     843 MB |       17.0 M |    1681 MB |

---

## Read Locality Benchmark Setup

The store after each compaction scenario is tested.

**Read-locality:**
- read from a **cold** store
- count **segment fetches** and **distinct segments**

**Access patterns:**
- **full** — depth first traversal of the whole tree
- **list-widest** — list children of the widest node (uuid index, **165,060** nodes)
- **deep-path** — traverse one root-to-leaf path (depth 47)

---

## Lessons on locality

Early benchmark results showed `list-widest` to be an order of magnitude worse when deduplication
was used in a parallel setup. `list-widest` iterates over the children of the UUID index.

The key insight here was that the record holding the UUID in a deduplicated repository can either
be colocated with the index or with the node that has the `jcr:uuid` property - but not both.

Tests confirmed that forcing `/oak:index` to be repacked last during any single-threaded
run using deduplication yielded the same poor result.

Conversely, forcing `/oak:index` to be repacked first, even in parallel, got rid of this
artifact.

Extrapolating from this learning, a three-stage approach was chosen:

1. repack `/oak:index` (internal path `/root/oak:index`)
2. repack the live root (internal path `/root`)
3. repack all checkpoints (internal path `/`)

The underlying assumption is that it should be beneficial to pack the live root more densely
at the expense of slightly more fragmented reads from checkpoints.

---

## Results: read locality (cold reads, 32MB segment cache)

**fetch** = segments loaded (includes reloads due to cache eviction);
**distinct** = distinct segments loaded.
For list-widest and deep-path fetch==distinct, because all repeat accesses are cached.

| scenario                     | full (fetch→distinct) | list-widest | deep-path |
|------------------------------|----------------------:|------------:|----------:|
| preserve 1x          |           3188 → 3052 |         143 |        39 |
| preserve 16x        |           3698 → 3398 |         202 |        40 |
| full-dedup 1x        |       4434 → **2441** |         143 |    **36** |
| full-dedup 16x      |       4391 → **2457** |        1094 |        38 |
| hot-dedup 1x         |       3984 → **2488** |         143 |        40 |
| hot-dedup 16x       |       4578 → **2515** |        1046 |        38 |
| hot-dedup+idx 1x     |       4010 → **2488** |     **142** |        40 |
| hot-dedup+idx 16x   |       4676 → **2510** |     **152** |        46 |
| compactor 1x         |           3552 → 3264 |         164 |        45 |
| compactor 16x       |           4326 → 3344 |         230 |        54 |

---

## Conclusions & status

- ~25% smaller, ~40% fewer records, faster - record-level repacking beats the semantic compactors on this store
- hot-dedup+idx achieves the best overall results
- tradeoff: deduplication vs locality - subtlety: denser stores have inherently higher locality
- a deduplication window covering ~10% of the records nearly ties with full deduplication (+ ~2% size)
- parallel compaction/repacking results in slightly worse locality, independent of the mode
