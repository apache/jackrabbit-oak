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

import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
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
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.CountRequest;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    protected ElasticIndexTracker indexTracker;
    // This is instantiated during repo creation but not hooked up to the async indexing lane
    // This can be used by the extending classes to trigger the async index update as per need (not having to wait for async indexing cycle)
    protected AsyncIndexUpdate asyncIndexUpdate;
    protected long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;
    protected NodeStore nodeStore;
    protected int DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS = 5;

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    @After
    public void tearDown() throws IOException {
        if (esConnection != null) {
        	esConnection.getClient().indices().delete(i->i
        	        .index(esConnection.getIndexPrefix() + "*"));
            esConnection.close();
        }
    }

    protected AsyncIndexUpdate getAsyncIndexUpdate(String asyncName, NodeStore store, IndexEditorProvider editorProvider) {
        return new AsyncIndexUpdate(asyncName, store, editorProvider);
    }

    /*
     * Override this to create some other repo initializer if needed
     * Make sure to call super.initialize(builder)
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

    protected ElasticConnection getElasticConnection() {
        return elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
    }

    @Override
    protected ContentRepository createRepository() {
        esConnection = getElasticConnection();
        indexTracker = new ElasticIndexTracker(esConnection, getMetricHandler());
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(indexTracker);

        nodeStore = getNodeStore();

        asyncIndexUpdate = getAsyncIndexUpdate("async", nodeStore, compose(List.of(
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
                .with(indexTracker)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());

        if (useAsyncIndexing()) {
            oak = addAsyncIndexingLanesToOak(oak);
        }
        return oak.createContentRepository();
    }

    protected ElasticMetricHandler getMetricHandler() {
        return new ElasticMetricHandler(StatisticsProvider.NOOP);
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((useAsyncIndexing() ? DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS * 1000L : 0) + BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
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
                    .exists(i -> i
                            .index(esIdxDef.getIndexAlias()))
                    .value();
        } catch (IOException e) {
            return false;
        }
    }

    protected long countDocuments(Tree index) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index);
        
        CountRequest count = CountRequest.of( r -> r
                .index(esIdxDef.getIndexAlias()));
        try {
			return esConnection.getClient().count(count).count();
		} catch (ElasticsearchException | IOException e) {
			throw new IllegalStateException(e);
		}
    }

    protected ObjectNode getDocument(Tree index, String id) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index);

        GetRequest get = GetRequest.of(r -> r
                .index(esIdxDef.getIndexAlias())
                .id(id));
        try {
            return esConnection.getClient().get(get, ObjectNode.class).source();
        } catch (ElasticsearchException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void updateDocument(Tree index, String id, ObjectNode doc) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index);
        try {
            esConnection.getClient().update(b -> b
                    .index(esIdxDef.getIndexAlias())
                    .id(id)
                    .doc(doc), ObjectNode.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public IndexMappingRecord getMapping(Tree index) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index);
        try {
            return esConnection.getClient().indices().getMapping(i -> i.index(esIdxDef.getIndexAlias()))
                    .result().entrySet().stream().findFirst().get().getValue();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected ElasticIndexDefinition getElasticIndexDefinition(Tree index) {
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
