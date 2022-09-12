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
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class ElasticIndexPlannerCommonTest extends IndexPlannerCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    private final ElasticConnection esConnection;
    private final ElasticIndexTracker indexTracker;
    private EditorHook HOOK;

    public ElasticIndexPlannerCommonTest() {
        indexOptions = new ElasticIndexOptions();
        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.indexTracker = new ElasticIndexTracker(esConnection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        HOOK = new EditorHook(new IndexUpdateProvider(new ElasticIndexEditorProvider(indexTracker, esConnection, null)));
    }


    private void createSampleDirectory() throws IOException, CommitFailedException {
        createSampleDirectory(1);
    }

    private void createSampleDirectory(long numOfDocs) throws IOException, CommitFailedException {
        NodeState before = builder.getNodeState();
        NodeBuilder testBuilder = builder.child("test");

        for (int i =0 ; i < numOfDocs ; i++) {
            testBuilder.child("child" + i).setProperty("foo", "bar" + i);
        }

        NodeState after = builder.getNodeState();
        NodeState indexed = HOOK.processCommit(before, after, new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, Collections.singletonMap("sync-mode", "rt")));
        indexTracker.update(indexed);
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition defn) throws IOException {
        try {
            createSampleDirectory();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        }

        return new ElasticIndexNodeManager(defn.getIndexPath(), root, esConnection).getIndexNode();
    }

    @Override
    protected IndexNode createIndexNode(IndexDefinition defn, long numOfDocs) throws IOException {
        try {
            createSampleDirectory(numOfDocs);
        } catch (CommitFailedException e) {
            e.printStackTrace();
        }

        return new ElasticIndexNodeManager(defn.getIndexPath(), root, esConnection).getIndexNode();
    }

    @Override
    protected IndexDefinition getIndexDefinition(NodeState root, NodeState defn, String indexPath) {
        return new ElasticIndexDefinition(root, defn, indexPath, "testElastic");
    }

    @Override
    protected NodeBuilder getPropertyIndexDefinitionNodeBuilder(@NotNull NodeBuilder builder, @NotNull String name, @NotNull Set<String> includes, @NotNull String async) {
        checkArgument(!includes.isEmpty(), "Lucene property index " +
                "requires explicit list of property names to be indexed");
        builder = builder.child("oak:index");
        builder = builder.child(name);
        builder.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType())
                .setProperty(REINDEX_PROPERTY_NAME, true);
        builder.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        builder.setProperty(createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, includes, STRINGS));

        return builder;
    }

    @Override
    protected NodeBuilder getIndexDefinitionNodeBuilder(@NotNull NodeBuilder index, @NotNull String name, @Nullable Set<String> propertyTypes) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType())
                .setProperty(REINDEX_PROPERTY_NAME, true);

        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        return index;
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
