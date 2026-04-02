/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexPlannerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticIndexPlannerCommonTest extends IndexPlannerCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    // Default refresh is 1 minute - so we need to lower that otherwise test would need to wait at least 1 minute
    // before it can get the estimated doc count from the remote ES index
    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty("oak.elastic.statsRefreshSeconds", "1");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private final ElasticConnection esConnection;
    private final ElasticIndexTracker indexTracker;
    private final EditorHook hook;

    private static final Logger log = LoggerFactory.getLogger(ElasticIndexPlannerCommonTest.class);

    public ElasticIndexPlannerCommonTest() {
        indexOptions = new ElasticIndexOptions();
        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.indexTracker = new ElasticIndexTracker(esConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        this.hook = new EditorHook(new IndexUpdateProvider(new ElasticIndexEditorProvider(indexTracker, esConnection, null)));
    }

    @After
    public void after() throws IOException {
        if (esConnection != null) {
            try {
                esConnection.getClient().indices().delete(d->d.index(esConnection.getIndexPrefix() + "*"));
            } catch (IOException e) {
                log.error("Unable to delete ES index", e);
            } finally {
                esConnection.close();
            }
        }
    }


    private void createSampleDirectory() throws CommitFailedException {
        createSampleDirectory(1);
    }

    private void createSampleDirectory(long numOfDocs) throws CommitFailedException {
        NodeState before = builder.getNodeState();
        NodeBuilder testBuilder = builder.child("test");

        for (int i =0 ; i < numOfDocs ; i++) {
            testBuilder.child("child" + i).setProperty("foo", "bar" + i);
        }

        NodeState after = builder.getNodeState();
        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);
        indexTracker.update(indexed);
    }

    // This is difference in test implementation from lucene
    // We are directly adding the content in the IndexWriter for lucene - so we can maintain what nodes to add there
    // But for elastic we add the documents to index via normal commit hooks - so in case of fulltext -
    // even the repo nodes get added
    // and the doc count is different from lucene
    @Override
    @Test
    public void fulltextIndexCost() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder defn = getIndexDefinitionNodeBuilder(index, indexName,
                Set.of(TYPENAME_STRING));
        TestUtil.useV2(defn);

        long numOfDocs = IndexDefinition.DEFAULT_ENTRY_COUNT + 100;
        IndexNode node = createIndexNode(getIndexDefinition(root, defn.getNodeState(), "/oak:index/" + indexName), numOfDocs);
        FilterImpl filter = createFilter("nt:base");
        filter.setFullTextConstraint(FullTextParser.parse(".", "mountain"));

        TestUtil.assertEventually(() -> {
            FulltextIndexPlanner planner = getIndexPlanner(node, "/oak:index/" + indexName, filter, Collections.<QueryIndex.OrderEntry>emptyList());

            QueryIndex.IndexPlan plan = planner.getPlan();
            assertNotNull(plan);
            assertTrue(plan.getEstimatedEntryCount() > numOfDocs);

        }, 4500*3);


    }

    @Override
    protected ContentRepository createContentRepository(MemoryNodeStore store) {
        ElasticIndexTracker tracker = new ElasticIndexTracker(esConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(tracker, esConnection, null);
        return new Oak(store)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(tracker)
                .with(new ElasticIndexProvider(tracker))
                .createContentRepository();
    }

    @Override
    protected IndexNode getIndexNodeFromStore(String indexPath, NodeState root) {
        return new ElasticIndexNodeManager(indexPath, root, esConnection).getIndexNode();
    }

    @Override
    protected Map<String, Object> getCommitAttributes() {
        return Map.of("sync-mode", "rt");
    }

    @Override
    protected void awaitIndexing() {
        // wait for ES stats to refresh (oak.elastic.statsRefreshSeconds=1 in test rule)
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition defn) throws IOException {
        try {
            createSampleDirectory();
        } catch (CommitFailedException e) {
            log.error("Error while creating data for tests", e);
        }
        return new ElasticIndexNodeManager(defn.getIndexPath(), builder.getNodeState(), esConnection).getIndexNode();
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition defn, long numOfDocs) throws IOException {
        try {
            createSampleDirectory(numOfDocs);
        } catch (CommitFailedException e) {
            log.error("Error while creating data for tests", e);
        }

        return new ElasticIndexNodeManager(defn.getIndexPath(), builder.getNodeState(), esConnection).getIndexNode();
    }

    @Override
    protected IndexDefinition getIndexDefinition(NodeState root, NodeState defn, String indexPath) {
        return new ElasticIndexDefinition(root, defn, indexPath, esConnection.getIndexPrefix());
    }

    @Override
    protected FulltextIndexPlanner getIndexPlanner(IndexNode indexNode, String indexPath, Filter filter, List<QueryIndex.OrderEntry> sortOrder) {
        return new ElasticIndexPlanner(indexNode, indexPath, filter, sortOrder);
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder() {
        return new ElasticIndexDefinitionBuilder();
    }

    @Override
    protected IndexDefinitionBuilder getIndexDefinitionBuilder(NodeBuilder builder) {
        return new ElasticIndexDefinitionBuilder(builder);
    }
}
