# LuceneNg Migration Integration Test Design

## Goal

Add an end-to-end OSGi integration test that verifies both routing correctness and result parity when switching a dual-write Oak index from the legacy Lucene 4.7 provider (`activeTarget=lucene47`) to the new LuceneNg provider (`activeTarget=lucene9`).

## Context

`oak-lucene` embeds Lucene 4.7 and exports it at version `4.7.2-oak2`. `oak-search-luceneNg` embeds Lucene 9 and blocks all `org.apache.lucene.*` imports via `!org.apache.lucene.*` in its `Import-Package` manifest header. The two bundles therefore operate in classloader isolation inside an OSGi runtime — the pattern used in production Sling/AEM deployments. A flat Maven classpath cannot host both because the packages collide; the `oak-it-osgi` Pax Exam module is the correct test vehicle because it provisions a real Felix container where each bundle has its own classloader.

## Scope

Three deliverables in dependency order:

1. **Loose-end cleanup** — amend the most recent commit to fix its misleading message and remove a stale TODO comment.
2. **Bundle provisioning** — add `oak-search-luceneNg` to `oak-it-osgi` so the Pax Exam container loads it alongside `oak-lucene`.
3. **`LuceneNgMigrationIT`** — integration test class in `oak-it-osgi` that exercises the dual-write → switch → query flow.

## Deliverable 1: Loose-end cleanup

### 1a. Commit message

The last commit on `lucene9-clean` is `perf: cache IndexSearcher per index node and close on provider deactivation`. It was amended during autosquash to absorb three unrelated fixup commits (document deletion/update, path restriction pushdown, wildcard fulltext queries). The message no longer matches the content.

Amend to:
```
feat: complete LuceneNg feature set — caching, doc lifecycle, path restrictions, wildcards

- Cache IndexSearcher per index node; close on provider deactivation
- Replace addDocument with updateDocument to prevent duplicates on re-index
- Implement childNodeDeleted: remove exact document and all descendant documents
- Store parentPath field at index time to support DIRECT_CHILDREN path restriction
- Push ALL_CHILDREN / DIRECT_CHILDREN / EXACT / PARENT path restrictions into Lucene query
- Detect wildcard/prefix patterns in fulltext terms; bypass tokenization for * and ?
```

### 1b. Stale TODO removal

`LuceneNgIndexEditor.propertyDeleted()` contains:
```java
// TODO: Implement document deletion/update in future phase
```
This is no longer accurate. When a property is deleted the node is still present; Oak calls `childNodeChanged` on the parent, which creates a child editor whose `enter()` calls `indexNode()` → `updateDocument()`, replacing the document with the new state. Remove the comment; the method body stays empty.

## Deliverable 2: Bundle provisioning in `oak-it-osgi`

### `pom.xml` change

Add `oak-search-luceneNg` as a `test`-scoped dependency so Maven resolves it into the local repository before the assembly step:

```xml
<dependency>
    <groupId>org.apache.jackrabbit</groupId>
    <artifactId>oak-search-luceneNg</artifactId>
    <version>${project.version}</version>
    <scope>test</scope>
</dependency>
```

### `test-bundles.xml` change

Add one line inside the existing `<includes>` block, alongside `oak-lucene`:

```xml
<include>org.apache.jackrabbit:oak-search-luceneNg</include>
```

No other infrastructure changes are required — Felix SCR, ConfigAdmin, and the OSGi DS runtime are already provisioned by `OSGiIT.configuration()`, which `LuceneNgMigrationIT` reuses.

## Deliverable 3: `LuceneNgMigrationIT`

**File:** `oak-it-osgi/src/test/java/org/apache/jackrabbit/oak/osgi/LuceneNgMigrationIT.java`

### Class structure

```java
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class LuceneNgMigrationIT {

    @Inject private BundleContext context;
    @Inject private Repository repository;   // javax.jcr.Repository from OSGi whiteboard

    @Configuration
    public Option[] configuration() throws Exception {
        // Delegates to OSGiIT.configuration() and returns same options
    }
}
```

### Index definition

Created in `@Before` via a JCR admin session at `/oak:index/searchIndex`:

| Property | Value |
|---|---|
| `jcr:primaryType` | `oak:QueryIndexDefinition` |
| `type` | `lucene` |
| `storeTargets` | `["lucene47","lucene9"]` |
| `activeTarget` | `lucene47` |
| `indexRules/nt:base/properties/title/propertyIndex` | `true` |
| `indexRules/nt:base/properties/description/analyzed` | `true` |

Three content nodes are saved at `/content/page-a`, `/content/page-b`, `/content/page-c`, each with a `title` and a `description` string property containing the word `"jackrabbit"`.

### Tests

**`testQueryPlanUsesLegacyBeforeSwitch`**

Runs `EXPLAIN SELECT * FROM [nt:base] WHERE CONTAINS(description, 'jackrabbit')`. Asserts the plan string contains `lucene47:/oak:index/searchIndex`.

**`testQueryPlanUsesNgAfterSwitch`**

Sets `activeTarget=lucene9` on the index definition node and saves the session. Runs the same EXPLAIN query. Asserts the plan string contains `lucene9:/oak:index/searchIndex`.

**`testResultParityAfterSwitch`**

Runs the non-EXPLAIN SELECT query before the switch and collects the result paths. Switches `activeTarget` to `lucene9`. Runs the same SELECT query again. Asserts both result sets are equal (same paths, order-insensitive).

### Error handling

If the OSGi container does not activate one of the providers within 10 seconds of container start (detectable by checking the query plan before making assertions), the test fails with a clear message rather than a timeout. A `@Rule Timeout` of 30 seconds guards against hangs.

## Constraints

- No new Maven module. All changes are inside `oak-it-osgi` (provisioning) and `oak-search-luceneNg` (loose ends).
- Tests follow the naming convention `*IT.java` so `maven-failsafe-plugin` picks them up in the `integration-test` phase.
- The `@Configuration` method in `LuceneNgMigrationIT` must match `OSGiIT.configuration()` exactly (same bundle list, same JPMS options) to keep the container consistent. Duplication is acceptable here to keep tests independent.
