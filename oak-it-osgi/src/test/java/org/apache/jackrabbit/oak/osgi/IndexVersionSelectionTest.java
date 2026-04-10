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
package org.apache.jackrabbit.oak.osgi;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertTrue;

/**
 * Tests that verify index version selection behavior when multiple versions of an index exist
 * across different index types (lucene and elasticsearch).
 */
public class IndexVersionSelectionTest {

    @ClassRule
    public static ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(System.getProperty("elasticConnectionString"));

    private ElasticConnection elasticConnection;
    private ContentRepository repo;
    private ContentSession session;

    @Before
    public void setUp() throws Exception {
        LuceneIndexProvider luceneProvider = new LuceneIndexProvider();
        LuceneIndexEditorProvider luceneEditorProvider = new LuceneIndexEditorProvider();

        elasticConnection = elasticRule.useDocker()
                ? elasticRule.getElasticConnectionForDocker()
                : elasticRule.getElasticConnectionFromString();
        ElasticIndexTracker elasticTracker = new ElasticIndexTracker(
                elasticConnection,
                new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexProvider elasticProvider = new ElasticIndexProvider(elasticTracker);
        // ElasticIndexEditorProvider is intentionally not registered here. See the comment
        // in testLatestVersionIsUsedEvenWithHigherCost() for the full explanation.

        repo = new Oak(new MemoryNodeStore())
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneProvider)
                .with((Observer) luceneProvider)
                .with(luceneEditorProvider)
                .with((QueryIndexProvider) elasticProvider)
                .with((Observer) elasticTracker)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();

        session = repo.login(null, null);
    }

    @After
    public void tearDown() throws Exception {
        if (session != null) {
            session.close();
        }
        if (elasticConnection != null) {
            elasticConnection.close();
        }
    }

    /**
     * Returns a fresh index definition builder for the given type, configured with
     * {@code noAsync()} so that the index definition is registered in the respective
     * index tracker via the Observer callback during commit, making query plans
     * available immediately after {@code root.commit()} returns.
     */
    private IndexDefinitionBuilder newBuilder(String type) {
        if ("lucene".equals(type)) {
            return new LuceneIndexDefinitionBuilder().noAsync();
        }
        return new ElasticIndexDefinitionBuilder().noAsync();
    }

    /**
     * Verifies that the latest index version (asset-10-custom-3, of type {@code newType}) is
     * selected even when its cost is set to a very high value (1 million), proving that
     * version-based selection takes precedence over cost. The index family is:
     * <pre>
     *   asset-10          (oldType)
     *   asset-10-custom-1 (oldType)
     *   asset-10-custom-2 (oldType)
     *   asset-10-custom-3 (newType, costPerEntry=costPerExecution=1_000_000)
     * </pre>
     * All indexes carry the tag "myTag" and use selectionPolicy="tag". The query uses
     * {@code option(index tag myTag)} and is a {@code contains()} query so that traversal
     * is not an option. Version selection must filter all but asset-10-custom-3 from the
     * candidate list regardless of its higher cost.
     */
    private void testLatestVersionIsUsedEvenWithHigherCost(String oldType, String newType)
            throws Exception {
        Root root = session.getLatestRoot();
        Tree oakIndex = root.getTree("/" + INDEX_DEFINITIONS_NAME);

        for (String name : new String[]{"asset-10", "asset-10-custom-1", "asset-10-custom-2"}) {
            IndexDefinitionBuilder b = newBuilder(oldType);
            b.tags("myTag", oldType);
            b.selectionPolicy("tag");
            b.indexRule("nt:base").property("asset").analyzed();
            b.build(oakIndex.addChild(name));
        }

        // asset-10-custom-3: same setup but with a very high cost. Version selection must
        // still pick this index (it is the latest) and not fall back to a cheaper older version.
        IndexDefinitionBuilder b = newBuilder(newType);
        b.tags("myTag", newType);
        b.selectionPolicy("tag");
        b.indexRule("nt:base").property("asset").analyzed();
        Tree custom3Tree = oakIndex.addChild("asset-10-custom-3");
        b.build(custom3Tree);
        custom3Tree.setProperty(FulltextIndexConstants.COST_PER_ENTRY, 1_000_000.0);
        custom3Tree.setProperty(FulltextIndexConstants.COST_PER_EXECUTION, 1_000_000.0);

        root.getTree("/").addChild("content").setProperty("asset", "test-value");
        root.commit();

        // Why we create the ES index manually instead of registering ElasticIndexEditorProvider:
        //
        // Normally, ElasticIndexEditorProvider would create the Elasticsearch index during
        // root.commit(). However, this module's test classpath has a Lucene version conflict:
        // oak-lucene depends on Lucene 4.7.2, while oak-search-elastic requires Lucene 9.x.
        // Maven's dependency resolution picks lucene-core:4.7.2 (the nearer declaration), so
        // lucene-core:9.x is absent. ElasticIndexEditorProvider transitively loads
        // ElasticCustomAnalyzer, which imports org.apache.lucene.util.ResourceLoader — a class
        // that only exists in lucene-core 9.x (in 4.x it had a different package). The result is
        // a NoClassDefFoundError at commit time, even when no custom analyzers are configured.
        //
        // This test does not need to index actual content — it only checks which index the query
        // planner *selects* (via EXPLAIN). For plan generation, ElasticIndexStatistics.numDocs()
        // is called to estimate the entry count; it issues a COUNT request to Elasticsearch and
        // throws index_not_found_exception if the index does not exist. That exception propagates
        // as UncheckedExecutionException and is caught in FulltextIndex.getPlans(), which silently
        // skips the index — causing the test to fail with "traverse allNodes".
        //
        // The fix: after committing the Oak index definitions (which registers the definition in
        // the ElasticIndexTracker via the Observer callback), we create an empty Elasticsearch
        // index directly via the REST client. numDocs() then returns 0, the planner can generate
        // a plan, and version selection is exercised correctly.
        String elasticName;
        if ("elasticsearch".equals(newType)) {
            elasticName = "asset-10-custom-3";
        } else {
            elasticName = "asset-10-custom-2";
        }
        String alias = ElasticIndexNameHelper.getElasticSafeIndexName(
                elasticConnection.getIndexPrefix(),
                "/" + INDEX_DEFINITIONS_NAME + "/" + elasticName);
        elasticConnection.getClient().indices().create(c -> c.index(alias));

        root = session.getLatestRoot();
        Result result = root.getQueryEngine().executeQuery(
                "explain select * from [nt:base] where contains([asset], 'test-value')" +
                        " option(index tag myTag)",
                "JCR-SQL2",
                QueryEngine.NO_BINDINGS,
                QueryEngine.NO_MAPPINGS);

        String plan = result.getRows().iterator().next().getValue("plan").getValue(Type.STRING);

        // Version selection keeps only asset-10-custom-3 (the latest). The contains() constraint
        // prevents traversal, so the high-cost index must be used.
        assertTrue("Expected asset-10-custom-3 to be used, but got: " + plan,
                plan.contains("asset-10-custom-3"));
    }

    @Test
    public void latestLuceneVersionIsUsedEvenWithHigherCost() throws Exception {
        testLatestVersionIsUsedEvenWithHigherCost("elasticsearch", "lucene");
    }

    @Test
    public void latestElasticsearchVersionIsUsedEvenWithHigherCost() throws Exception {
        testLatestVersionIsUsedEvenWithHigherCost("lucene", "elasticsearch");
    }
}
