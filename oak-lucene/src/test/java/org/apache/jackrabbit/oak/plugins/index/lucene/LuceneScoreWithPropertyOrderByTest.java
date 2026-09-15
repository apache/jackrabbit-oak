/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.TestRepository;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.PropertyType;
import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for OAK-12399: ordering a full-text query by an indexed numeric property first
 * and by Lucene/JCR relevance ({@code jcr:score}) second:
 * <pre>ORDER BY [searchScore] DESC, [jcr:score] DESC</pre>
 * <p>
 * Historically Lucene dropped {@code jcr:score} from the sort whenever a property sort field was
 * present, so relevance was not computed: {@code jcr:score} came back as {@code NaN} and the
 * secondary key had no effect. The fix (in {@link LucenePropertyIndex}) keeps {@code jcr:score} as a
 * real relevance sort field at its position and tracks document scores, so the secondary ordering is
 * honoured and the projected {@code jcr:score} is a real value. It is enabled by default and can be
 * reverted at runtime via the {@link LucenePropertyIndex#FT_LEGACY_SORT_OAK_12399} kill-switch
 * feature toggle. Lucene-specific, hence it lives here rather than in the shared
 * {@code OrderByCommonTest}.
 */
public class LuceneScoreWithPropertyOrderByTest extends AbstractQueryTest {

    private static final String INDEX_NAME = "oak12399Index";
    private static final String PRODUCTS_PATH = "/test/oak-12399/products";
    private static final float TITLE_BOOST = 10.0f;

    // Node local names. Within each tied searchScore bucket the "single" node (lower relevance)
    // sorts BEFORE the "triple" node in path order, i.e. the opposite of the expected relevance
    // order, so a fallback to path/document order cannot masquerade as a correct secondary sort.
    // The trailing comment gives the "alpha" term frequency (drives the relevance ranking).
    private static final String HIGH_SINGLE = "product-high-single-alpha"; // searchScore 200, tf 1
    private static final String HIGH_TRIPLE = "product-high-triple-alpha"; // searchScore 200, tf 5
    private static final String MID_SINGLE = "product-mid-single-alpha";   // searchScore 100, tf 2
    private static final String MID_TRIPLE = "product-mid-triple-alpha";   // searchScore 100, tf 4
    private static final String LOW_DOUBLE = "product-low-double-alpha";   // searchScore 10,  tf 3

    /** Relevance-only ordering (established by {@link #controlOrderByScoreOnly()}). */
    private static final List<String> RELEVANCE_ORDER =
            List.of(HIGH_TRIPLE, MID_TRIPLE, LOW_DOUBLE, MID_SINGLE, HIGH_SINGLE);

    private static final String SELECT =
            "select [jcr:path], [searchScore], [title], [jcr:score] from [nt:unstructured] as p" +
                    " where isdescendantnode(p, '" + PRODUCTS_PATH + "')" +
                    " and contains(p.[title], 'alpha')";

    private static final String Q_SCORE_ONLY = SELECT + " order by [jcr:score] desc";
    private static final String Q_SCORE_THEN_PATH = SELECT + " order by [searchScore] desc, [jcr:path] asc";
    private static final String Q_COMBINED_DESC = SELECT + " order by [searchScore] desc, [jcr:score] desc";
    private static final String Q_COMBINED_ASC = SELECT + " order by [searchScore] desc, [jcr:score] asc";

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private TestRepository repository;
    private LuceneIndexProvider luceneIndexProvider;

    @Override
    protected ContentRepository createRepository() {
        LuceneTestRepositoryBuilder builder = new LuceneTestRepositoryBuilder(executorService, temporaryFolder);
        repository = builder.build();
        luceneIndexProvider = (LuceneIndexProvider) builder.getIndexProvider();
        return repository.getOak().createContentRepository();
    }

    private void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repository.isAsync() ? repository.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    /**
     * Builds a Lucene index scoped to the test data providing:
     * <ul>
     *     <li>analyzed, node-scoped, boosted ({@value #TITLE_BOOST}) full-text indexing on
     *         {@code title};</li>
     *     <li>a numeric ({@code Long}), ordered property index on {@code searchScore};</li>
     *     <li>path restriction evaluation, so ISDESCENDANTNODE is served by the index.</li>
     * </ul>
     * and populates the deterministic product data set (two nodes tied at searchScore=200, two
     * tied at 100, one at 10; each node a distinct "alpha" term frequency so the relevance-only
     * baseline is strictly ordered).
     */
    private void setupIndexAndData() throws Exception {
        LuceneIndexDefinitionBuilder builder = new LuceneIndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();
        IndexDefinitionBuilder.IndexRule rule = builder.indexRule("nt:base");
        rule.property("title").propertyIndex().analyzed().nodeScopeIndex().boost(TITLE_BOOST);
        rule.property("searchScore").propertyIndex().type(PropertyType.TYPENAME_LONG).ordered();
        builder.build(root.getTree("/").addChild("oak:index").addChild(INDEX_NAME));

        Tree products = addPath(root.getTree("/"), "test", "oak-12399", "products");
        addProduct(products, HIGH_TRIPLE, "alpha alpha alpha alpha alpha", 200);
        addProduct(products, HIGH_SINGLE, "alpha", 200);
        addProduct(products, MID_TRIPLE, "alpha alpha alpha alpha", 100);
        addProduct(products, MID_SINGLE, "alpha alpha", 100);
        addProduct(products, LOW_DOUBLE, "alpha alpha alpha", 10);
        root.commit();
    }

    private static Tree addPath(Tree parent, String... names) {
        Tree t = parent;
        for (String name : names) {
            t = t.addChild(name);
        }
        return t;
    }

    private static void addProduct(Tree parent, String name, String title, long searchScore) {
        Tree p = parent.addChild(name);
        p.setProperty(JcrConstants.JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        p.setProperty("title", title);
        p.setProperty("searchScore", searchScore, Type.LONG);
    }

    // ----------------------------------------------------------------------------------------
    // controls
    // ----------------------------------------------------------------------------------------

    /**
     * Control 1 - relevance-only ordering ({@code ORDER BY [jcr:score] DESC}). Establishes the
     * baseline relevance order independently of {@code searchScore}: every row gets a real,
     * strictly-descending {@code jcr:score}, proving full-text scoring (and the title boost) work
     * on their own. The other tests rely on this baseline.
     */
    @Test
    public void controlOrderByScoreOnly() throws Exception {
        setupIndexAndData();
        assertEventually(() -> {
            List<Row> rows = runQuery(Q_SCORE_ONLY);
            assertContainsAlpha(rows);
            assertEquals(5, rows.size());
            for (Row row : rows) {
                assertFalse("jcr:score must be a real number for " + row.path, Double.isNaN(row.score));
                assertTrue("jcr:score must be positive for " + row.path, row.score > 0.0d);
            }
            assertScoresStrictlyDescending(rows);
            assertPaths(RELEVANCE_ORDER, rows);
        });
    }

    /**
     * Control 2 - ordinary multi-column ordering by a normal property then path
     * ({@code ORDER BY [searchScore] DESC, [jcr:path] ASC}). Demonstrates the query engine
     * combines a normal indexed property with a second (non-score) sort key correctly, isolating
     * the special property/{@code jcr:score} interaction exercised below.
     */
    @Test
    public void controlOrderBySearchScoreThenPath() throws Exception {
        setupIndexAndData();
        assertEventually(() -> {
            List<Row> rows = runQuery(Q_SCORE_THEN_PATH);
            assertContainsAlpha(rows);
            assertEquals(List.of(200L, 200L, 100L, 100L, 10L), searchScores(rows));
            // within each tied bucket, ascending path order
            assertPaths(List.of(HIGH_SINGLE, HIGH_TRIPLE, MID_SINGLE, MID_TRIPLE, LOW_DOUBLE), rows);
        });
    }

    // ----------------------------------------------------------------------------------------
    // OAK-12399 fix: jcr:score as a secondary sort key after an ordered property
    // ----------------------------------------------------------------------------------------

    /**
     * Main query (OAK-12399): {@code ORDER BY [searchScore] DESC, [jcr:score] DESC}. With the fix
     * (toggle on by default) the primary property ordering groups the ties and, within each tied
     * {@code searchScore} bucket, the higher-relevance node comes first, with a real (non-NaN)
     * {@code jcr:score} on every row.
     */
    @Test
    public void orderBySearchScoreThenJcrScoreDescRespectsRelevance() throws Exception {
        setupIndexAndData();

        // Guard against an accidental pass via traversal + in-memory sort: the query must be
        // served by the Lucene index.
        assertEventually(() -> assertTrue("query should be served by the lucene index, plan was:\n" + explain(Q_COMBINED_DESC),
                explain(Q_COMBINED_DESC).contains("lucene:" + INDEX_NAME)));

        assertEventually(() -> {
            List<Row> rows = runQuery(Q_COMBINED_DESC);

            // 1. every returned node matches the full-text condition
            assertContainsAlpha(rows);
            assertEquals(5, rows.size());

            // 2. searchScore is a real numeric (Long) value, non-increasing, 200-bucket first
            for (Row row : rows) {
                assertEquals("searchScore should be returned as LONG for " + row.path,
                        Type.LONG, row.searchScoreType);
            }
            assertEquals(List.of(200L, 200L, 100L, 100L, 10L), searchScores(rows));

            // 3. jcr:score is a real number for every row (not NaN / not missing)
            for (Row row : rows) {
                assertFalse("jcr:score must not be NaN for " + row.path, Double.isNaN(row.score));
                assertTrue("jcr:score must be positive for " + row.path, row.score > 0.0d);
            }

            // 4. within each tied searchScore bucket the higher jcr:score appears first
            assertTrue("200-bucket must be relevance-ordered: " + rows.get(0).score + " !> " + rows.get(1).score,
                    rows.get(0).score > rows.get(1).score);
            assertTrue("100-bucket must be relevance-ordered: " + rows.get(2).score + " !> " + rows.get(3).score,
                    rows.get(2).score > rows.get(3).score);

            // 5. full deterministic order (relevance within buckets), not path/document order.
            // Node names are chosen so path-asc within a tie is single-then-triple, the opposite of
            // the expected relevance order triple-then-single - so this can't pass by falling back.
            assertPaths(List.of(HIGH_TRIPLE, HIGH_SINGLE, MID_TRIPLE, MID_SINGLE, LOW_DOUBLE), rows);
        });
    }

    /**
     * OAK-12399: reversing the secondary relevance direction ({@code jcr:score DESC} vs {@code ASC})
     * now flips the order of the tied nodes, proving the secondary score sort is actually applied.
     */
    @Test
    public void reversedSecondaryRelevanceReversesTiedOrder() throws Exception {
        setupIndexAndData();
        assertEventually(() -> {
            assertPaths(List.of(HIGH_TRIPLE, HIGH_SINGLE, MID_TRIPLE, MID_SINGLE, LOW_DOUBLE),
                    runQuery(Q_COMBINED_DESC));
            assertPaths(List.of(HIGH_SINGLE, HIGH_TRIPLE, MID_SINGLE, MID_TRIPLE, LOW_DOUBLE),
                    runQuery(Q_COMBINED_ASC));
        });
    }

    /**
     * OAK-12399 kill-switch: enabling the {@link LucenePropertyIndex#FT_LEGACY_SORT_OAK_12399} feature
     * toggle restores the legacy behaviour - {@code jcr:score} is dropped from the sort and comes back
     * as {@code NaN}, while the primary property ordering still works. Confirms the fix is safely
     * reversible at runtime.
     */
    @Test
    public void legacyBehaviourWhenToggleEnabled() throws Exception {
        setupIndexAndData();
        Feature legacyFeature = enabledFeature(LucenePropertyIndex.FT_LEGACY_SORT_OAK_12399);
        luceneIndexProvider.setLegacySortFeature(legacyFeature);
        try {
            assertEventually(() -> {
                List<Row> rows = runQuery(Q_COMBINED_DESC);
                // primary property ordering is unaffected
                assertEquals(List.of(200L, 200L, 100L, 100L, 10L), searchScores(rows));
                // but relevance is dropped again: jcr:score is NaN for every row
                for (Row row : rows) {
                    assertTrue("legacy: jcr:score expected NaN for " + row.path, Double.isNaN(row.score));
                }
            });
        } finally {
            luceneIndexProvider.setLegacySortFeature(null);
            legacyFeature.close();
        }
    }

    /** Registers a feature toggle on a throw-away whiteboard and flips it on. */
    private static Feature enabledFeature(String name) {
        Whiteboard whiteboard = new DefaultWhiteboard();
        Feature feature = Feature.newFeature(name, whiteboard);
        for (FeatureToggle toggle : whiteboard.track(FeatureToggle.class).getServices()) {
            if (name.equals(toggle.getName())) {
                toggle.setEnabled(true);
            }
        }
        return feature;
    }

    /**
     * Explain-plan verification (targeted {@code contains} only, as the plan format is not a stable
     * contract). Confirms: the intended Lucene index is selected; {@code searchScore} is recognised
     * as a descending ordered-property sort ahead of {@code jcr:score}; and the full-text query uses
     * the analyzed {@code title} field. Note: the configured title boost is not surfaced in the plan
     * on this Oak version, so it is verified separately in {@link #titleBoostIsConfiguredInIndex()}.
     */
    @Test
    public void explainPlanUsesIndexAndOrderedProperty() throws Exception {
        setupIndexAndData();
        assertEventually(() -> {
            String plan = explain(Q_COMBINED_DESC);
            assertTrue("index not selected, plan:\n" + plan, plan.contains("lucene:" + INDEX_NAME));
            assertTrue("index definition missing, plan:\n" + plan,
                    plan.contains("indexDefinition: /oak:index/" + INDEX_NAME));
            assertTrue("analyzed title full-text query missing, plan:\n" + plan,
                    plan.contains("full:title:alpha"));
            assertTrue("searchScore not recognised as descending ordered sort, plan:\n" + plan,
                    plan.contains("propertyName : searchScore") && plan.contains("order : DESCENDING"));
            assertTrue("jcr:score not present in sort order, plan:\n" + plan,
                    plan.contains("propertyName : jcr:score"));
        });
    }

    /**
     * Verifies the title boost is actually configured on the index (it is not exposed by the
     * explain plan on this Oak version).
     */
    @Test
    public void titleBoostIsConfiguredInIndex() throws Exception {
        setupIndexAndData();
        Tree properties = root.getTree("/oak:index/" + INDEX_NAME + "/indexRules/nt:base/properties");
        assertTrue("index property definitions missing", properties.exists());
        Tree titleDef = null;
        for (Tree child : properties.getChildren()) {
            if (child.hasProperty("name") && "title".equals(child.getProperty("name").getValue(Type.STRING))) {
                titleDef = child;
                break;
            }
        }
        assertNotNull("no property definition for 'title'", titleDef);
        assertTrue("title should be analyzed", titleDef.getProperty("analyzed").getValue(Type.BOOLEAN));
        assertTrue("title should be node-scoped", titleDef.getProperty("nodeScopeIndex").getValue(Type.BOOLEAN));
        assertEquals("title boost", (double) TITLE_BOOST,
                titleDef.getProperty("boost").getValue(Type.DOUBLE), 0.0001d);
    }

    // ----------------------------------------------------------------------------------------
    // helpers
    // ----------------------------------------------------------------------------------------

    /** A single query result row with typed accessors for the values we assert on. */
    private static final class Row {
        final String path;
        final String title;
        final Long searchScore;
        final Type<?> searchScoreType;
        final double score;

        Row(String path, String title, Long searchScore, Type<?> searchScoreType, double score) {
            this.path = path;
            this.title = title;
            this.searchScore = searchScore;
            this.searchScoreType = searchScoreType;
            this.score = score;
        }
    }

    private List<Row> runQuery(String sql) {
        try {
            Result result = executeQuery(sql, SQL2, NO_BINDINGS);
            List<Row> rows = new ArrayList<>();
            for (ResultRow r : result.getRows()) {
                PropertyValue searchScore = r.getValue("searchScore");
                PropertyValue jcrScore = r.getValue("jcr:score");
                rows.add(new Row(
                        r.getPath(),
                        r.getValue("title").getValue(Type.STRING),
                        searchScore == null ? null : searchScore.getValue(Type.LONG),
                        searchScore == null ? null : searchScore.getType(),
                        jcrScore == null ? Double.NaN : jcrScore.getValue(Type.DOUBLE)));
            }
            return rows;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private String explain(String sql) {
        try {
            Result result = executeQuery("explain " + sql, SQL2, NO_BINDINGS);
            ResultRow row = result.getRows().iterator().next();
            return row.getValue("plan").getValue(Type.STRING);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Long> searchScores(List<Row> rows) {
        return rows.stream().map(r -> r.searchScore).collect(Collectors.toList());
    }

    private static List<String> paths(List<Row> rows) {
        return rows.stream().map(r -> r.path).collect(Collectors.toList());
    }

    private void assertPaths(List<String> expectedLocalNames, List<Row> rows) {
        List<String> expected = expectedLocalNames.stream()
                .map(n -> PRODUCTS_PATH + "/" + n)
                .collect(Collectors.toList());
        assertEquals(expected, paths(rows));
    }

    private static void assertContainsAlpha(List<Row> rows) {
        assertFalse("expected some matching rows", rows.isEmpty());
        for (Row row : rows) {
            assertTrue("title of " + row.path + " should contain 'alpha': " + row.title,
                    row.title.toLowerCase().contains("alpha"));
        }
    }

    private static void assertScoresStrictlyDescending(List<Row> rows) {
        for (int i = 1; i < rows.size(); i++) {
            assertTrue("jcr:score should be strictly descending: " + rows.get(i - 1).score
                            + " !> " + rows.get(i).score,
                    rows.get(i - 1).score > rows.get(i).score);
        }
    }
}
