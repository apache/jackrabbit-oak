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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;
import static org.junit.Assert.assertEquals;

public abstract class ElasticAbstractQueryTest extends AbstractQueryTest {

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticAbstractQueryTest.class);

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String elasticConnectionString = System.getProperty("elasticConnectionString");
    protected ElasticConnection esConnection;

    // This is instantiated during repo creation but not hooked up to the async indexing lane
    // This can be used by the extending classes to trigger the async index update as per need (not having to wait for async indexing cycle)
    protected AsyncIndexUpdate asyncIndexUpdate;
    protected long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;
    protected NodeStore nodeStore;
    protected int DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS = 5;

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        elasticRule.closeElasticConnection();
    }

    // Override this in extending test class to provide different ExtractedTextCache if needed
    protected ElasticIndexEditorProvider getElasticIndexEditorProvider(ElasticConnection esConnection) {
        return new ElasticIndexEditorProvider(esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }

    protected AsyncIndexUpdate getAsyncIndexUpdate(String asyncName, NodeStore store, IndexEditorProvider editorProvider) {
        return new AsyncIndexUpdate(asyncName, store, editorProvider);
    }

    /*
    Override this to create some other repo initializer if needed
    // Make sure to call super.initialize(builder)
     */
    protected InitialContent getInitialContent() {
        return new InitialContent() {
            @Override
            public void initialize(@NotNull NodeBuilder builder) {
                super.initialize(builder);
                // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
                NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
                oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());
            }
        };
    }

    // Override this to provide a different flavour of node store
    // like segment or mongo mk
    // Tests would need to handle the cleanup accordingly.
    // TODO provide a util here so that test classes simply need to mention the type of store they want to create
    // for now, memory store should suffice.
    protected NodeStore getNodeStore() {
        if (nodeStore == null) {
            nodeStore = new MemoryNodeStore();
        }
        return nodeStore;
    }

    protected boolean useAsyncIndexing() {
        return false;
    }

    protected Oak addAsyncIndexingLanesToOak(Oak oak) {
        // Override this in extending classes to configure different
        // indexing lanes with different time limits.
        return oak.withAsyncIndexing("async", DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS);
    }

    @Override
    protected ContentRepository createRepository() {

        esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        ElasticIndexEditorProvider editorProvider = getElasticIndexEditorProvider(esConnection);
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(esConnection,
                new ElasticMetricHandler(StatisticsProvider.NOOP));

        nodeStore = getNodeStore();

        asyncIndexUpdate = getAsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));

        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(INDEX_CORRUPT_INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);

        Oak oak = new Oak(nodeStore)
                .with(getInitialContent())
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with((Observer) indexProvider)
                .with((QueryIndexProvider) indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());

        if (useAsyncIndexing()) {
            oak = addAsyncIndexingLanesToOak(oak);
        }
        return oak.createContentRepository();
    }

    protected void assertEventually(Runnable r) {
        ElasticTestUtils.assertEventually(r,
                ((useAsyncIndexing() ? DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS : 0) + BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
    }

    protected IndexDefinitionBuilder createIndex(String... propNames) {
        return createIndex(true, "nt:base", propNames);
    }

    protected IndexDefinitionBuilder createIndex(boolean isPropertyIndex, String nodeType, String... propNames) {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        if (!useAsyncIndexing()) {
            builder = builder.noAsync();
        }
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule(nodeType);
        if (isPropertyIndex) {
            for (String propName : propNames) {
                indexRule.property(propName).propertyIndex();
            }
        }
        return builder;
    }

    protected Tree setIndex(String idxName, IndexDefinitionBuilder builder) {
        return builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    protected String explain(String query) {
        return explain(query, SQL2);
    }

    protected String explain(String query, String language) {
        String explain = "explain " + query;
        return executeQuery(explain, language).get(0);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    // Utility methods accessing directly Elasticsearch

    protected boolean exists(Tree index) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index);

        try {
            return esConnection.getClient().indices()
                    .exists(new GetIndexRequest(esIdxDef.getRemoteIndexAlias()), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected long countDocuments(Tree index) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index);

        CountRequest request = new CountRequest(esIdxDef.getRemoteIndexAlias());
        try {
            return esConnection.getClient().count(request, RequestOptions.DEFAULT).getCount();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private ElasticIndexDefinition getElasticIndexDefinition(Tree index) {
        return new ElasticIndexDefinition(
                nodeStore.getRoot(),
                nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(index.getName()),
                index.getPath(),
                esConnection.getIndexPrefix());
    }

    protected void assertOrderedQuery(String sql, List<String> paths) {
        List<String> result = executeQuery(sql, AbstractQueryTest.SQL2, true, true);
        assertEquals(paths, result);
    }

}
