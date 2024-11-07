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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Tests: Logging of invalid jcr:primaryType of index.
 */
public class InvalidIndexTest {

    private final long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;
    private MemoryBlobStore blobStore;

    protected Root root;

    private AsyncIndexUpdate asyncIndexUpdate;

    private static final String invalidJcrPrimaryTypeIndexName = "myTestIndexWithWrongJcrPrimaryType";
    private static final String invalidJcrPrimaryTypeForIndex = NodeTypeConstants.NT_OAK_UNSTRUCTURED;

    private NodeStore nodeStore;
    private LogCustomizer customLogger;


    // Setting contentCount to DEFAULT_indexJcrTypeInvalidLogLimiter + 1, so that invalid indexes are logged atleast once
    private final long contentCount = IndexUpdate.INDEX_JCR_TYPE_INVALID_LOG_LIMITER + 1;

    @Before
    public void before() throws Exception {
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
        customLogger = LogCustomizer
                .forLogger(IndexUpdate.class.getName())
                .enable(Level.WARN).create();
        customLogger.starting();
    }

    @After
    public void after() {
        customLogger.finished();
    }

    private ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        blobStore = new MemoryBlobStore();
        blobStore.setBlockSizeMin(48);//make it as small as possible

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(List.of(
                luceneIndexEditorProvider,
                new NodeCounterEditorProvider()
        )));
        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(INDEX_CORRUPT_INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(luceneIndexEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(new PropertyIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();
    }

    private void runEmptyAsyncIndexerCyclesWithoutNewContent(long count) {
        for (int i = 0; i < count; i++) {
            asyncIndexUpdate.run();
        }
    }

    private void runIndexerCyclesAfterEachNodeCommit(long count, boolean async) throws CommitFailedException {
        for (int i = 0; i < count; i++) {
            root.getTree("/").addChild("testNode" + i);
            root.commit();
            if (async) {
                asyncIndexUpdate.run();
            }
        }
    }

    private boolean assertionLogPresent(List<String> logs, String assertionLog) {
        for (String log : logs) {
            if (log.equals(assertionLog)) {
                return true;
            }
        }
        return false;
    }

    private String invalidJcrPrimaryTypeLog() {
        return "jcr:primaryType of index " + invalidJcrPrimaryTypeIndexName + " should be oak:QueryIndexDefinition instead of " + invalidJcrPrimaryTypeForIndex;
    }

    /**
     * Logs warning as index is not a valid index definition
     */
    @Test
    public void loggingInvalidJcrPrimaryType() throws Exception {
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(invalidJcrPrimaryTypeIndexName);
        test1.setProperty("jcr:primaryType", invalidJcrPrimaryTypeForIndex, Type.NAME);
        test1.setProperty("type", "lucene");
        test1.setProperty("reindex", true);
        test1.setProperty("async", "async");
        root.commit();
        runIndexerCyclesAfterEachNodeCommit(contentCount, true);
        runEmptyAsyncIndexerCyclesWithoutNewContent(contentCount);

        List<String> logs = customLogger.getLogs();
        assertTrue(assertionLogPresent(logs, invalidJcrPrimaryTypeLog()));
    }

    @Test
    public void noLoggingIfNodeIsNotIndexDefinition() throws Exception {
        // This is not a valid index definition. Refer method: IndexUpdate.isIncluded
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(invalidJcrPrimaryTypeIndexName);
        test1.setProperty("jcr:primaryType", invalidJcrPrimaryTypeForIndex, Type.NAME);
        // Here index definition itself is not valid as there is no type property
        // test1.setProperty("type", "lucene");
        test1.setProperty("reindex", true);
        test1.setProperty("async", "async");
        root.commit();

        runIndexerCyclesAfterEachNodeCommit(contentCount, true);
        runEmptyAsyncIndexerCyclesWithoutNewContent(contentCount);

        List<String> logs = customLogger.getLogs();
        assertFalse(assertionLogPresent(logs, invalidJcrPrimaryTypeLog()));
    }

    @Test
    public void loggingInCaseOfSyncLuceneIndexDefinition() throws Exception {
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(invalidJcrPrimaryTypeIndexName);
        test1.setProperty("jcr:primaryType", invalidJcrPrimaryTypeForIndex, Type.NAME);
        test1.setProperty("type", "lucene");
        test1.setProperty("reindex", true);
        // test1.setProperty("async", "async");
        root.commit();

        runIndexerCyclesAfterEachNodeCommit(contentCount, false);
        List<String> logs = customLogger.getLogs();
        assertTrue(assertionLogPresent(logs, invalidJcrPrimaryTypeLog()));
    }
}
