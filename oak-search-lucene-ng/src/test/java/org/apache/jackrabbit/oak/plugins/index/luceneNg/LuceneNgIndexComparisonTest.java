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

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.test.AbstractIndexComparisonTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Runs the shared {@link AbstractIndexComparisonTest} scenarios against the LuceneNg (Lucene 9) backend.
 */
public class LuceneNgIndexComparisonTest extends AbstractIndexComparisonTest {

    @Override
    protected ContentRepository createRepository() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        LuceneNgIndexEditorProvider editor = new LuceneNgIndexEditorProvider(tracker);

        return new Oak()
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with(editor)
            .createContentRepository();
    }

    @Override
    protected void createSearchIndex() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        builder.indexRule("nt:base")
            .property("title").propertyIndex().ordered()
            // analyzed() in addition to propertyIndex(): propertyIndex() backs the equality
            // lookup in testDescriptionQuery, analyzed() backs the property-scoped fulltext
            // lookup in the shared testContainsOnAnalyzedProperty (CONTAINS(description, ...)).
            .property("description").propertyIndex().analyzed()
            .property("age").propertyIndex().type("Long").ordered()
            .property("price").propertyIndex().type("Double").ordered()
            .property("status").propertyIndex().ordered()
            .property("category").propertyIndex();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgTestIndex"));
        index.setProperty("type", "lucene9");
        root.commit();
    }

    @Test
    public void testLuceneNgIndexIsUsed() throws Exception {
        createSearchIndex();
        createTestContent();
        String explain = executeQuery("explain //element(*, nt:base)[@title = 'Oak Testing']", "xpath").get(0);
        assertThat("Query plan should use lucene:...@v9 for Granite-style parsers",
                explain, containsString("lucene:luceneNgTestIndex@v9"));
        assertThat("Query plan should still expose lucene9 engine tag",
                explain, containsString("lucene9:luceneNgTestIndex"));
        assertThat("Query plan should use luceneQuery label like FulltextIndex.getPlanDescription",
                explain, containsString("luceneQuery:"));
        assertThat("Query plan should carry index definition path for tooling",
                explain, containsString("indexDefinition: /oak:index/luceneNgTestIndex"));
    }

    /**
     * The index declared by {@link #createSearchIndex()} does not index a property named
     * {@code undeclared}. The lucene9 index must not offer a plan for a query restricted on a
     * property it does not index — the query must fall back to traversal instead.
     */
    @Test
    public void undeclaredPropertyNotServedByLucene9() throws Exception {
        createSearchIndex();
        createTestContent();
        String explain = executeQuery(
                "explain select [jcr:path] from [nt:base] where [undeclared] = 'x'", "sql").get(0);
        assertThat("lucene9 index must not serve a query on a property it does not index; "
                        + "the query must fall back to traversal. Plan was: " + explain,
                explain, not(containsString("lucene9:")));
    }

    /**
     * A query that combines a restriction on a DECLARED property ({@code title}) with one on an
     * UNDECLARED property ({@code undeclared}). Because {@code title} is declared, the inherited
     * {@link org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner} does
     * offer a lucene9 plan (unlike {@link #undeclaredPropertyNotServedByLucene9}, where the only
     * restriction is undeclared and no plan is offered at all). This pins whether the undeclared
     * restriction still leaks into the constructed Lucene query.
     *
     * <p>The node {@code /mixed/n1} has {@code title='MixedDeclared'} AND {@code undeclared='bar'}.
     * Legacy Lucene (LucenePropertyIndex.addNonFullTextConstraints) never turns a restriction on an
     * undeclared property into a Lucene clause — {@code planResult.getPropDefn(pr) == null} → skip —
     * so it matches on the {@code title} clause and lets the query engine post-filter the
     * {@code undeclared} restriction; the node satisfies both, so legacy returns {@code /mixed/n1}.
     * lucene9 must agree.
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
        index.setProperty("type", "lucene9");
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
        index.setProperty("type", "lucene9");
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
        index.setProperty("type", "lucene9");
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
}
