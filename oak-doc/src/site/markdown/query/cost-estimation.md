<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

# Cost Estimation for Property / Fulltext Indexes

This page describes how Oak's `FulltextIndexPlanner` (used by Lucene and Elastic
property indexes) estimates query cost, what changed in OAK-12221, and how the
result compares with the cost-estimation model used by relational databases.

- [Cost Framework](#cost-framework)
- [Legacy Cost Estimator](#legacy-cost-estimator)
- [Problems with the Legacy Estimator](#problems-with-the-legacy-estimator)
- [How Relational Databases Do It](#how-relational-databases-do-it)
- [The New Selectivity Model (OAK-12221)](#the-new-selectivity-model-oak-12221)
- [Property Reference](#property-reference)
- [Feature Toggles](#feature-toggles)
- [Default Comparison](#default-comparison)
- [Migration Notes](#migration-notes)

## Cost Framework

For every candidate index, the planner produces an `IndexPlan` carrying three
numbers consumed by the query engine:

```
cost = costPerExecution + estimatedEntryCount × costPerEntry / (1 + |sortOrder|)
```

| Field                  | Source                                                                                                  | Default               |
|------------------------|---------------------------------------------------------------------------------------------------------|-----------------------|
| `costPerExecution`     | `costPerExecution` property on the index definition                                                     | `1.0`                 |
| `costPerEntry`         | `costPerEntry` property on the index definition                                                         | `1.0` (`1.5` for the legacy Lucene V1 format because it re-aggregates at runtime) |
| `estimatedEntryCount`  | computed from `numDocs`, the indexed properties' statistics, and the filter (`getMaxPossibleNumDocs`)   | derived per query     |
| `costPerEntryFactor`   | `1 + sortOrder.size()` — sorting amortises the per-entry cost                                           | derived per query     |

The query engine then picks the plan with the lowest `cost`. The interesting
number is `estimatedEntryCount`. The rest of this page is about how that number
is derived.

## Legacy Cost Estimator

`FulltextIndexPlanner.getMaxPossibleNumDocs` (the original implementation,
still active when `FT_OAK-12221` is disabled) starts from `numDocs` (live total
documents in the index) and walks each indexed property mentioned by the
query's filter. For every property it computes a per-field upper bound:

```
scaledDocCnt = weight == 1 : docCntForField     
               weight > 1  : ceil(docCntForField / weight)
minNumDocs   = min(minNumDocs, scaledDocCnt)
```

`docCntForField` is the live Lucene/Elastic field doc count — the number of
documents that have at least one term in that field. `weight` is configured per
property and defaults to **5** (system property
`oak.fulltext.defaultPropertyWeight`).

For `IS NULL` / `IS NOT NULL`, the planner uses `weightNull` / `weightNotNull`
when set, otherwise falls back to the explicit `weight` (when > 1), otherwise
to `DEFAULT_NULL_CHECK_WEIGHT = 5` (OAK-12171). For `IS NULL` queries the field
name is `:nullProps` (a shared Lucene field that holds the name of every
null-tracked property for every document where that property is null).

A final adjustment applies path restrictions (`EXACT`, `DIRECT_CHILDREN`, etc.)
to `minNumDocs`.

### Problems with the Legacy Estimator

1. **No multiplicativity for `AND`.** The `min(minNumDocs, scaledDocCnt)`
   reduction means each condition only competes for "smallest"; it does not
   compound. A query with three indexed equalities is estimated the same as
   the most selective single equality. Adding indexed conditions does not
   reduce the cost.
2. **No per-value distribution.** The estimator treats every value as equally
   probable (1 / `weight`). A query on `status = 'active'` (80 % of rows)
   produces the same estimate as `status = 'archived'` (1 %).
3. **`IS NULL` / `IS NOT NULL` ignore live counts.** The actual number of
   null-tracked and not-null documents is readable from the index, but the
   planner instead applies a heuristic divisor and only caps at the live count.
4. **Defaults are far less aggressive than textbook RDBMSes** (see comparison
   table below). With `weight = 5`, equality is assumed to read 20 % of the
   index, which over-estimates cost for selective columns and prevents the
   planner from preferring a property index over a path scan in cases where it
   should.

## How Relational Databases Do It

PostgreSQL is the canonical reference. Statistics are gathered automatically by
`ANALYZE` and the planner combines them under an independence assumption,
where MCV stands for "Most-Common Values" (the list of most frequent values).

| Predicate                | Selectivity formula                                                              |
|--------------------------|-----------------------------------------------------------------------------------|
| `x = v`, `v` is MCV      | `most_common_freqs[v]`                                                            |
| `x = v`, `v` is not MCV  | `(1 − Σ mcv_freqs) / (n_distinct − |mcv|)`                                       |
| `x < v` / `x BETWEEN`    | linear interpolation across `histogram_bounds[]`                                  |
| `x IS NULL`              | `null_frac`                                                                       |
| `x IS NOT NULL`          | `1 − null_frac`                                                                   |
| `cond_a AND cond_b`      | `sel_a × sel_b` (independence assumption; extended stats if available)            |

Stats stored per column: `n_distinct`, `null_frac`, `most_common_vals` +
`most_common_freqs`, `histogram_bounds`, `correlation`, `avg_width`. When no
stats are available PostgreSQL falls back to constants like
`DEFAULT_EQ_SEL = 0.005` (equality assumed to match 0.5 %).

The cost of a row is split into per-tuple CPU, sequential I/O, random I/O —
finer-grained than Oak's single `costPerEntry`.

## The New Selectivity Model (OAK-12221)

When `FT_OAK-12221` is enabled the planner runs
`getMaxPossibleNumDocsBySelectivity`. The model is order-independent and
matches the textbook independence formula.

**Per-condition selectivity** (probability of matching, conditional on the
document having the field):

| Condition                                    | Selectivity                                                          |
|----------------------------------------------|-----------------------------------------------------------------------|
| `x = v` and `v` ∈ `stats.common`              | `stats.common[v] / 100`                                              |
| `x = v` and `v` ∉ `stats.common`              | `1 / weight`                                                         |
| Range / `LIKE` on `x`                         | `1 / min(3, weight)` (i.e. ≥ 33 %)                                   |
| `x IS NOT NULL`                               | `1.0` — cap below is the exact match count                           |
| `x IS NULL`                                   | `1.0` — cap below is the exact match count                           |

**Combined estimate:**

```
combinedSelectivity = ∏  selectivity_i
selectivityCap      = min over conditions of docCntForField_i
                       (for IS NULL: numDocs − docCntForField(propertyName))
estimatedEntries    = round(combinedSelectivity × selectivityCap)
```

Key consequences:

- **Order independence.** The estimate is a product (commutative), not a
  sequence of `min` / `ceil` operations whose interaction depended on
  HashMap iteration order.
- **`AND` compounds.** Adding indexed conditions reduces the estimate
  multiplicatively, so plans with more indexed restrictions cost less — the
  original motivation for OAK-12221.
- **MCV is stored as a percentage**, not as an absolute count. Percentages
  survive uniform purges / growth without re-tuning, since the estimate is
  derived as `percentage × live docCntForField` at planning time.
- **`IS NULL` / `IS NOT NULL` use live counts directly.** For `IS NOT NULL`,
  the field's live doc count is the exact number of matching documents. For
  `IS NULL`, the estimate is `numDocs − docCntForField(propertyName)` — exact
  per property, independent of how many other properties have null-check
  enabled. The `weightNull` / `weightNotNull` heuristic is bypassed.

### Worked Example

Index: 1 000 documents. Properties `a` and `b` both indexed.

- `a` has `stats = {"common":{"x":10}}` — value `"x"` matches 10 % of `a`.
- `b` has default `weight = 5`.

Query: `a = 'x' AND b = 'y'`. The planner computes:

```
selectivity_a       = 0.10       (MCV hit)
selectivity_b       = 1/5 = 0.20 (weight fallback)
combinedSelectivity = 0.10 × 0.20        = 0.02
selectivityCap      = min(1000, 1000)    = 1000
estimatedEntries    = round(0.02 × 1000) = 20
```

The legacy estimator would have returned the larger of the per-property
estimates (no compounding) and ignored the percentage information.

## Property Reference

The properties below live on `oak:QueryIndexDefinition` (top-level cost knobs)
or on property definitions inside `indexRules/<nodeType>/properties/<name>`
(per-property statistics).

### Index-level cost knobs

| Property             | Type / Default                          | Purpose                                                                                          |
|----------------------|-----------------------------------------|--------------------------------------------------------------------------------------------------|
| `costPerEntry`       | double, `1.0` (Lucene V2+) / `1.5` (V1) | Cost charged per estimated result row. Lower the value to make this index more attractive.       |
| `costPerExecution`   | double, `1.0`                           | Fixed start-up cost added once per query plan. Lower the value to avoid penalising fast indexes. |
| `entryCount`         | long, unset                             | Hard override for `estimatedEntryCount`. Skips statistics-based estimation entirely.             |

### Per-property statistics

| Property              | Type / Default | Purpose                                                                                                          |
|-----------------------|----------------|------------------------------------------------------------------------------------------------------------------|
| `weight`              | int, `5`       | "How selective is equality on this property?" Selectivity ≈ `1 / weight`. Also caps range selectivity at `1 / min(3, weight)`. The default can be overridden by `-Doak.fulltext.defaultPropertyWeight=…`. |
| `weightNull`          | int, `-1`      | Selectivity divisor for `IS NULL`. Used only when **FT_OAK-12221 is disabled**.                                  |
| `weightNotNull`       | int, `-1`      | Selectivity divisor for `IS NOT NULL`. Used only when **FT_OAK-12221 is disabled**.                              |
| `stats`               | JSON string    | Most-Common-Values (MCV) percentages. Format: `{"common":{"value1": pct1, "value2": pct2}}`. Used only when **FT_OAK-12221 is enabled**. Values are percentages (e.g. `33.33` means 33.33 %; `0.5` means half a percent). |

When `FT_OAK-12221` is enabled, `weightNull` / `weightNotNull` are not consulted
— `IS NULL` / `IS NOT NULL` use the exact live counts instead.

## Feature Toggles

The feature toggles are wired into the OSGi `Whiteboard` and can be flipped at
runtime via `org.apache.jackrabbit.oak.spi.toggle.FeatureToggle`.

| Toggle name        | Default       | Effect when enabled                                                                                                          |
|--------------------|---------------|------------------------------------------------------------------------------------------------------------------------------|
| `FT_OAK-12171`     | **disabled** *(improved behaviour is active by default)* | Kill-switch only. When ENABLED, reverts `IS NULL` / `IS NOT NULL` to the pre-OAK-12171 behaviour (weight = 1, raw `docCntForField`). |
| `FT_OAK-12221`     | **disabled**  | Switches `getMaxPossibleNumDocs` from the legacy min model to the multiplicative selectivity model described above. Enables `stats` (MCV) and live-count `IS NULL` / `IS NOT NULL` estimation. |

In code, the toggles are reachable as
`FulltextIndexPlanner.FT_OAK_12171_DISABLE` and
`FulltextIndexPlanner.FT_OAK_12221_ENABLE` (both
`java.util.concurrent.atomic.AtomicBoolean`).

## Default Comparison

| Concern                                    | Oak (legacy)                                     | Oak (FT_OAK-12221)                                                                                          | PostgreSQL                                            |
|--------------------------------------------|--------------------------------------------------|-------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Equality, no per-value stats               | `1 / weight = 20 %`                              | `1 / weight = 20 %`                                                                                         | `DEFAULT_EQ_SEL = 0.5 %` (or `1 / n_distinct`)        |
| Equality, known common value               | not supported                                    | from `stats` (percentage)                                                                                   | from `most_common_freqs`                              |
| Range / `LIKE`                             | `1 / min(3, weight) ≥ 33 %`                      | `1 / min(3, weight) ≥ 33 %`                                                                                 | histogram interpolation                               |
| `IS NULL`                                  | `1 / 5` heuristic, capped at `:nullProps` field  | exact: `numDocs − docCntForField(propertyName)`                                                              | `null_frac`                                           |
| `IS NOT NULL`                              | `1 / 5` heuristic, capped at field doc count     | exact: `docCntForField(propertyName)`                                                                       | `1 − null_frac`                                       |
| Combining `AND`                            | `min` over per-condition estimates               | independence: ∏ selectivities                                                                               | independence (plus extended stats if defined)         |
| Statistics refresh                         | manual (edit `weight` / `stats`)                 | manual (edit `weight` / `stats`); MCV stays valid under uniform purges because it is a percentage           | automatic via `ANALYZE`                               |

## Migration Notes

- Both code paths (legacy and OAK-12221) co-exist; only the body of
  `getMaxPossibleNumDocs` decides which one runs. The legacy method is
  byte-for-byte identical to the pre-OAK-12221 implementation.
- Enabling `FT_OAK-12221` in isolation is safe: it does not require any
  changes to existing index definitions. Existing `weight` settings continue
  to work; only `weightNull` / `weightNotNull` lose effect (the new model
  uses live counts).
- To exercise MCV, add a `stats` JSON property to the property definition.
  Percentages can be tuned over time without redeploying — the planner reads
  them on each plan.
- Estimated cost is exposed in query EXPLAIN output and via the index MBeans;
  changes after enabling FT_OAK-12221 are observable there.
