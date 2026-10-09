/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.LuceneNgIndexCopier;
import org.apache.jackrabbit.oak.plugins.index.search.test.AbstractIndexComparisonTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Runs the shared {@link AbstractIndexComparisonTest} scenarios against the LuceneNg (Lucene 9) backend.
 */
public class LuceneNgIndexComparisonTest extends AbstractIndexComparisonTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final org.apache.jackrabbit.oak.query.QueryEngineSettings querySettings =
            new org.apache.jackrabbit.oak.query.QueryEngineSettings();
    private LuceneNgIndexTracker tracker;

    @Override
    protected ContentRepository createRepository() {
        return createRepository(null);
    }

    /**
     * Same wiring as {@link #createRepository()}, but lets a test build a repository whose
     * {@link LuceneNgIndexTracker} is backed by a real {@link LuceneNgIndexCopier} (CopyOnRead)
     * instead of the default no-copier tracker every other test in this class uses.
     */
    private ContentRepository createRepository(@Nullable LuceneNgIndexCopier copier) {
        tracker = new LuceneNgIndexTracker(copier);
        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        LuceneNgIndexEditorProvider editor = new LuceneNgIndexEditorProvider(tracker, copier);
        DefaultWhiteboard whiteboard = new DefaultWhiteboard();
        whiteboard.register(org.apache.jackrabbit.oak.query.QueryEngineSettings.class,
                querySettings, java.util.Collections.emptyMap());

        return new Oak(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT))
            .with(new OpenSecurityProvider())
            .with(whiteboard)
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with(editor)
            .createContentRepository();
    }

    @Override
    protected String getIndexType() {
        return "luceneNg";
    }

    @Test
    public void indexedNodeTypesExcludeOtherTypesBeforeCandidateReads() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder().noAsync();
        builder.indexRule("nt:base").property("title").propertyIndex()
                .property("jcr:primaryType").propertyIndex()
                .property("jcr:mixinTypes").propertyIndex();
        builder.build(root.getTree("/oak:index").addChild("types")).setProperty("type", "luceneNg");
        Tree content = root.getTree("/").addChild("content");
        Tree folder = content.addChild("folder");
        folder.setProperty("jcr:primaryType", "nt:folder", org.apache.jackrabbit.oak.api.Type.NAME);
        folder.setProperty("title", "Common");
        Tree file = content.addChild("file");
        file.setProperty("jcr:primaryType", "nt:file", org.apache.jackrabbit.oak.api.Type.NAME);
        file.setProperty("title", "Common");
        root.commit();
        String query = "select [jcr:path] from [nt:file] where [title] = 'Common' "
                + "option(index name types)";
        assertThat(executeQuery("explain " + query, "sql").get(0),
                containsString("jcr:primaryType:nt:file"));
        org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode node =
                tracker.acquireIndexNode("/oak:index/types");
        try {
            assertEquals("Both selector types must be indexed", 2, node.getSearcher().count(
                    new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("title", "Common"))));
            assertEquals("The NAME-typed primary type must be searchable", 1, node.getSearcher().count(
                    new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("jcr:primaryType", "nt:file"))));
        } finally {
            node.release();
        }
        querySettings.setLimitReads(1);
        assertQuery(query, "sql", List.of("/content/file"));
    }

    @Test
    public void multipleDefinitionsExecuteTheSelectedIndex() throws Exception {
        IndexDefinitionBuilder decoy = new IndexDefinitionBuilder().noAsync();
        decoy.indexRule("nt:base").property("decoy").propertyIndex();
        decoy.build(root.getTree("/oak:index").addChild("decoy")).setProperty("type", "luceneNg");
        IndexDefinitionBuilder selected = new IndexDefinitionBuilder().noAsync();
        selected.indexRule("nt:base").property("title").propertyIndex().ordered().facets();
        selected.build(root.getTree("/oak:index").addChild("selected")).setProperty("type", "luceneNg");
        Tree content = root.getTree("/").addChild("content");
        content.addChild("other").setProperty("decoy", "x");
        content.addChild("first").setProperty("title", "a");
        content.addChild("second").setProperty("title", "b");
        root.commit();
        String query = "select [jcr:path] from [nt:base] where [title] is not null "
                + "order by [title] option(index name selected)";
        assertQuery(query, "sql", List.of("/content/first", "/content/second"), false, true);
        assertThat(executeQuery("explain " + query, "sql").get(0),
                containsString("indexDefinition: /oak:index/selected"));
        org.apache.jackrabbit.oak.api.Result result = qe.executeQuery(
                "select [jcr:path], [rep:facet(title)] from [nt:base] where [title] is not null "
                        + "option(index name selected)", javax.jcr.query.Query.JCR_SQL2, Long.MAX_VALUE, 0,
                java.util.Collections.emptyMap(), java.util.Collections.emptyMap());
        String facets = result.getRows().iterator().next().getValue("rep:facet(title)")
                .getValue(org.apache.jackrabbit.oak.api.Type.STRING);
        assertEquals("1", org.apache.jackrabbit.oak.commons.json.JsonObject.fromJson(facets, true)
                .getProperties().get("a"));
        assertEquals("1", org.apache.jackrabbit.oak.commons.json.JsonObject.fromJson(facets, true)
                .getProperties().get("b"));
    }

    @Test
    public void nestedDefinitionsDoNotProduceQueryPlans() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder().noAsync();
        builder.indexRule("nt:base").property("nested").propertyIndex();
        builder.build(content.addChild("oak:index").addChild("nested"))
                .setProperty("type", "luceneNg");
        content.addChild("node").setProperty("nested", "x");
        root.commit();
        String query = "select [jcr:path] from [nt:base] where isdescendantnode('/content') "
                + "and [nested] = 'x'";
        assertThat(executeQuery("explain " + query, "sql").get(0), not(containsString("luceneNg:")));
        createSearchIndex();
        assertThat(executeQuery("explain " + query, "sql").get(0), not(containsString("luceneNg:")));
    }

    @Test
    public void configuredNullChecksUseMarkers() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder().noAsync();
        builder.indexRule("nt:unstructured").property("optional").propertyIndex().nullCheckEnabled();
        Tree definition = builder.build(root.getTree("/oak:index").addChild("nulls"));
        definition.setProperty("type", "luceneNg");
        Tree content = root.getTree("/").addChild("content");
        Tree absent = content.addChild("absent");
        absent.setProperty("jcr:primaryType", "nt:unstructured", org.apache.jackrabbit.oak.api.Type.NAME);
        Tree present = content.addChild("present");
        present.setProperty("jcr:primaryType", "nt:unstructured", org.apache.jackrabbit.oak.api.Type.NAME);
        present.setProperty("optional", "x");
        root.commit();
        String query = "select [jcr:path] from [nt:unstructured] where [optional] is null "
                + "and isdescendantnode('/content')";
        assertQuery(query, "sql", List.of("/content/absent"));
        assertThat(executeQuery("explain " + query, "sql").get(0), containsString(":nullProps:optional"));
    }

    @Test
    public void postFilteredCandidatesStillCountTowardsTheReadLimit() throws Exception {
        createSearchIndex();
        Tree content = root.getTree("/").addChild("content");
        for (int i = 0; i < 20; i++) {
            content.addChild("node" + i).setProperty("title", "Report");
        }
        root.commit();
        querySettings.setLimitReads(10);
        org.junit.Assert.assertThrows(org.apache.jackrabbit.oak.query.RuntimeNodeTraversalException.class,
                () -> executeQuery("select [jcr:path] from [nt:base] where [title] = 'Report' "
                        + "and [undeclared] = 'approved'", "sql"));
    }

    @Test
    public void testLuceneNgIndexIsUsed() throws Exception {
        createSearchIndex();
        createTestContent();
        String explain = executeQuery("explain //element(*, nt:base)[@title = 'Oak Testing']", "xpath").get(0);
        assertThat("Query plan should use lucene:...@v9 for Granite-style parsers",
                explain, containsString("lucene:searchTestIndex@v9"));
        assertThat("Query plan should expose luceneNg type",
                explain, containsString("luceneNg:searchTestIndex"));
        assertThat("Query plan should use luceneQuery label like FulltextIndex.getPlanDescription",
                explain, containsString("luceneQuery:"));
        assertThat("Query plan should carry index definition path for tooling",
                explain, containsString("indexDefinition: /oak:index/searchTestIndex"));
    }

    /**
     * The index declared by {@link #createSearchIndex()} does not index a property named
     * {@code undeclared}. The luceneNg index must not offer a plan for a query restricted on a
     * property it does not index — the query must fall back to traversal instead.
     */
    @Test
    public void undeclaredPropertyNotServedByLuceneNg() throws Exception {
        createSearchIndex();
        createTestContent();
        String explain = executeQuery(
                "explain select [jcr:path] from [nt:base] where [undeclared] = 'x'", "sql").get(0);
        assertThat("luceneNg index must not serve a query on a property it does not index; "
                        + "the query must fall back to traversal. Plan was: " + explain,
                explain, not(containsString("luceneNg:")));
    }

    /**
     * A query that combines a restriction on a DECLARED property ({@code title}) with one on an
     * UNDECLARED property ({@code undeclared}). Because {@code title} is declared, the inherited
     * {@link org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner} does
     * offer a luceneNg plan (unlike {@link #undeclaredPropertyNotServedByLuceneNg}, where the only
     * restriction is undeclared and no plan is offered at all). This pins whether the undeclared
     * restriction still leaks into the constructed Lucene query.
     *
     * <p>The node {@code /mixed/n1} has {@code title='MixedDeclared'} AND {@code undeclared='bar'}.
     * Legacy Lucene (LucenePropertyIndex.addNonFullTextConstraints) never turns a restriction on an
     * undeclared property into a Lucene clause — {@code planResult.getPropDefn(pr) == null} → skip —
     * so it matches on the {@code title} clause and lets the query engine post-filter the
     * {@code undeclared} restriction; the node satisfies both, so legacy returns {@code /mixed/n1}.
     * luceneNg must agree.
     */
    @Test
    public void queryOnUndeclaredPropertyDoesNotWronglyMatchOrMismatch() throws Exception {
        createSearchIndex();

        Tree content = root.getTree("/").addChild("mixed");
        Tree n1 = content.addChild("n1");
        n1.setProperty("title", "MixedDeclared");
        n1.setProperty("undeclared", "bar");
        root.commit();

        // title is declared (index enforces it); undeclared is not (query engine post-filters).
        // The node satisfies both, so both backends must return it. Verified against legacy Lucene
        // (LuceneIndexComparisonTest) for the identical scenario.
        assertQuery(
                "select [jcr:path] from [nt:base] where [title] = 'MixedDeclared' and [undeclared] = 'bar'",
                "sql", List.of("/mixed/n1"));
    }

    @Test
    public void sortByBooleanProperty() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        builder.indexRule("nt:base")
            .property("active").propertyIndex().type("Boolean").ordered();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgBooleanSortIndex"));
        index.setProperty("type", "luceneNg");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("nodeTrue").setProperty("active", true);
        test.addChild("nodeFalse").setProperty("active", false);
        root.commit();

        // "false" < "true" lexicographically, so ascending order is nodeFalse, nodeTrue
        assertQuery("select [jcr:path] from [nt:base] where [active] is not null order by [active]", "sql",
                List.of("/test/nodeFalse", "/test/nodeTrue"), false, true);
    }

    @Test
    public void sortByMultiValuedStringProperty() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        builder.indexRule("nt:base")
            .property("tags").propertyIndex().ordered();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgMultiValuedStringSortIndex"));
        index.setProperty("type", "luceneNg");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("nodeA").setProperty("tags", List.of("b", "c"), org.apache.jackrabbit.oak.api.Type.STRINGS);
        test.addChild("nodeB").setProperty("tags", List.of("a"), org.apache.jackrabbit.oak.api.Type.STRINGS);
        root.commit();

        // Sorting on a multi-valued property compares each document's minimum value:
        // nodeA's minimum tag is "b", nodeB's minimum tag is "a", so ascending order is nodeB, nodeA.
        assertQuery("select [jcr:path] from [nt:base] where [tags] is not null order by [tags]", "sql",
                List.of("/test/nodeB", "/test/nodeA"), false, true);
    }

    @Test
    public void sortByMixedCardinalityOrderedStringProperty() throws Exception {
        // Regression test: an "ordered" String property must use the same Lucene doc-values
        // type (SORTED_SET) whether a given node stores a single value or multiple values.
        // Both cardinalities are legal under the same index rule, so a single commit that
        // indexes one node of each cardinality for the same field must not throw
        // "cannot change field ... doc values type=SORTED to inconsistent doc values type=SORTED_SET".
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        builder.indexRule("nt:base")
            .property("tags").propertyIndex().ordered();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgMixedCardinalityStringSortIndex"));
        index.setProperty("type", "luceneNg");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        // Single-valued: uses the "ordered" single-value branch.
        test.addChild("nodeSingle").setProperty("tags", "b");
        // Multi-valued: uses the "ordered" array branch, for the same field name.
        test.addChild("nodeMulti").setProperty("tags", List.of("a", "c"), org.apache.jackrabbit.oak.api.Type.STRINGS);
        root.commit();

        // Sorting compares each document's minimum value: nodeMulti's minimum tag is "a",
        // nodeSingle's tag is "b", so ascending order is nodeMulti, nodeSingle.
        assertQuery("select [jcr:path] from [nt:base] where [tags] is not null order by [tags]", "sql",
                List.of("/test/nodeMulti", "/test/nodeSingle"), false, true);
    }

    /**
     * End-to-end proof that CopyOnRead ({@link LuceneNgIndexCopier}) is a transparent read-path
     * optimisation: a luceneNg index served through a tracker wired with a real copier must
     * return exactly the same results as the no-copier baseline every other test in this class
     * exercises, for the identical content/query fixture and query used by the shared
     * {@link AbstractIndexComparisonTest#testContainsOnAnalyzedProperty()}.
     *
     * <p>To rule out a false-positive (the wrap being silently skipped while the query still
     * happens to work off the remote directory), the final assertion does not merely check that
     * *some* file landed under the configured local root — {@code LuceneNgIndexCopier}'s
     * constructor unconditionally creates an empty {@code indexWriterDir} scratch directory, so
     * that alone would pass even if CopyOnRead never actually ran. Instead it looks for a real
     * Lucene commit file ({@code segments_N}), which only appears locally if
     * {@code CopyOnReadDirectory} genuinely copied it from the remote {@code OakDirectory}.
     */
    @Test
    public void queryResultsIdenticalWithCopyOnReadEnabled() throws Exception {
        File localRoot = temporaryFolder.newFolder();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, localRoot, false);
        try {
            // Swap in a repository whose luceneNg index is served through a copier-backed
            // tracker, in place of the default no-copier one createRepository() installed via
            // AbstractQueryTest#before(). createSearchIndex()/createTestContent()/assertQuery()
            // all operate on the instance fields reassigned here.
            ContentSession originalSession = session;
            Root originalRoot = root;
            QueryEngine originalQe = qe;
            try {
                session = createRepository(copier).login(null, null);
                root = session.getLatestRoot();
                qe = root.getQueryEngine();

                createSearchIndex();
                createTestContent();

                // Identical content fixture and query as
                // AbstractIndexComparisonTest#testContainsOnAnalyzedProperty's no-copier baseline:
                // "functionality" appears only in page1's description
                // ("Testing Oak search functionality").
                assertQuery(
                        "select [jcr:path] from [nt:base] where CONTAINS(description, 'functionality')",
                        "sql", List.of("/content/page1"));
            } finally {
                session = originalSession;
                root = originalRoot;
                qe = originalQe;
            }

            boolean realSegmentFileCopiedLocally = FileUtils.listFiles(localRoot, null, true).stream()
                    .anyMatch(f -> f.getName().startsWith("segments_"));
            assertTrue("expected a real Lucene commit file (segments_N) to be copied locally under "
                            + localRoot + ", not just the always-created empty scaffold dirs",
                    realSegmentFileCopiedLocally);
        } finally {
            copier.close();
        }
    }
}
