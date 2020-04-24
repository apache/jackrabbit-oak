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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;

public abstract class ElasticsearchAbstractQueryTest extends AbstractQueryTest {

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticsearchAbstractQueryTest.class);

    protected static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(ElasticsearchAbstractQueryTest.class.getName() + ".perf"));


    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String elasticConnectionString = System.getProperty("elasticConnectionString");
    private ElasticsearchConnection esConnection;

    // This is instantiated during repo creation but not hooked up to the async indexing lane
    // This can be used by the extending classes to trigger the async index update as per need (not having to wait for async indexing cycle)
    protected AsyncIndexUpdate asyncIndexUpdate;
    protected long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;
    protected ElasticsearchIndexEditorProvider editorProvider;
    protected NodeStore nodeStore;
    protected int DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS = 5;


    @ClassRule
    public static ElasticsearchConnectionRule elasticRule = new ElasticsearchConnectionRule(elasticConnectionString);

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        elasticRule.closeElasticSearchConnection();
    }

    // Override this in extending test class to provide different ExtractedTextCache if needed
    protected ElasticsearchIndexEditorProvider getElasticIndexEditorProvider(ElasticsearchConnection esConnection) {
        return new ElasticsearchIndexEditorProvider(esConnection,
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
        // Override this in extending clases to configure different
        // indexing lanes with different time limits.
        return oak.withAsyncIndexing("async", DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS);
    }


    @Override
    protected ContentRepository createRepository() {

        esConnection = elasticRule.useDocker() ? elasticRule.getElasticSearchConnectionForDocker() :
                elasticRule.getElasticsearchConnectionFromString();
        ElasticsearchIndexEditorProvider editorProvider = getElasticIndexEditorProvider(esConnection);
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(esConnection);

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
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());

        if (useAsyncIndexing()) {
            oak = addAsyncIndexingLanesToOak(oak);
        }
        return oak.createContentRepository();
    }


    protected static void assertEventually(Runnable r) {
        ElasticsearchTestUtils.assertEventually(r, BULK_FLUSH_INTERVAL_MS_DEFAULT * 5);
    }

    protected IndexDefinitionBuilder createIndex(String... propNames) {
        IndexDefinitionBuilder builder = new ElasticsearchIndexDefinitionBuilder();
        if (!useAsyncIndexing()) {
            builder = builder.noAsync();
        }
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }

    protected void setIndex(String idxName, IndexDefinitionBuilder builder) {
        builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
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

}
