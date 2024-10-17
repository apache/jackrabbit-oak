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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexPlannerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import java.util.Set;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticIndexPlannerCommonTest extends IndexPlannerCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    // Default refresh is 1 minute - so we need to lower that otherwise test would need to wait at least 1 minute
    // before it can get the estimated doc count from the remote ES index
    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty("oak.elastic.statsRefreshSeconds", "5");

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
    protected NodeBuilder getPropertyIndexDefinitionNodeBuilder(@NotNull NodeBuilder builder, @NotNull String name, @NotNull Set<String> includes, @NotNull String async) {
        checkArgument(!includes.isEmpty(), "Lucene property index " +
                "requires explicit list of property names to be indexed");
        NodeBuilder defBuilder = builder.child("oak:index");
        defBuilder = defBuilder.child(name);
        defBuilder.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType())
                .setProperty(REINDEX_PROPERTY_NAME, true);
        defBuilder.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        defBuilder.setProperty(createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, includes, STRINGS));

        return defBuilder;
    }

    @Override
    protected NodeBuilder getIndexDefinitionNodeBuilder(@NotNull NodeBuilder index, @NotNull String name, @Nullable Set<String> propertyTypes) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        NodeBuilder indexDefBuilder = index.child(name);
        indexDefBuilder.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType())
                .setProperty(REINDEX_PROPERTY_NAME, true);

        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            indexDefBuilder.setProperty(createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        return indexDefBuilder;
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
