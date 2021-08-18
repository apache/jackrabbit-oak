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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.*;

/**
 * Tests: Logging of invalid jcr:primaryType of index.
 */
public class InvalidIndexTest {

    private final long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;
    private MemoryBlobStore blobStore;

    protected Root root;

    private AsyncIndexUpdate asyncIndexUpdate;

    static final String invalidJcrPrimaryTypeIndexName = "myTestIndexWithWrongJcrPrimaryType";
    static final String invalidJcrPrimaryTypeForIndex = NodeTypeConstants.NT_OAK_UNSTRUCTURED;

    NodeStore nodeStore;
    LogCustomizer customLogger;
    private Properties systemProperties;

    @Before
    public void before() throws Exception {
        systemProperties =(Properties) System.getProperties().clone();
        System.setProperty("oak.indexer.indexJcrTypeInvalidLogLimiter", "10");
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
        System.setProperties(systemProperties);
    }

    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        blobStore = new MemoryBlobStore();
        blobStore.setBlockSizeMin(48);//make it as small as possible

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
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

    private void runEmptyIndexerCyclesWithoutNewContent(long count){
        for (int i = 0; i < count; i++) {
            asyncIndexUpdate.run();
        }
    }

    private void runIndexerCyclesWithNewContent(long count) throws CommitFailedException {
        for (int i = 0; i < count; i++) {
            root.getTree("/").addChild("testNode" + i);
            root.commit();
            asyncIndexUpdate.run();
        }
    }

    @Test
    public void loggingInvalidJcrPrimaryType() throws Exception {
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(invalidJcrPrimaryTypeIndexName);
        test1.setProperty("jcr:primaryType", invalidJcrPrimaryTypeForIndex, Type.NAME);
        test1.setProperty("type", "lucene");
        test1.setProperty("reindex", true);
        test1.setProperty("async", "async");
        root.commit();

        // This will lead to 11 cycles and hence 2 logs at count 0 and other at count 10
        runIndexerCyclesWithNewContent(11);
        runEmptyIndexerCyclesWithoutNewContent(10);

        List<String> logs = customLogger.getLogs();
        assertEquals(assertionLogPresent(logs, invalidJcrPrimaryTypeLog()), 2);
    }

    /*
    Whether node is index-definition or not is figured out based on property "type". Refer method: IndexUpdate.isIncluded
     */
    @Test
    public void noLoggingIfNodeIsNotIndexDefinition() throws Exception {
        // This is not a valid index definition. Refer method: IndexUpdate.isIncluded
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(invalidJcrPrimaryTypeIndexName);
        test1.setProperty("jcr:primaryType", invalidJcrPrimaryTypeForIndex, Type.NAME);
//        test1.setProperty("type", "lucene");
        test1.setProperty("reindex", true);
        test1.setProperty("async", "async");
        root.commit();

        // This will lead to 11 cycles and hence 2 logs at count 0 and other at count 10
        runIndexerCyclesWithNewContent(11);
        runEmptyIndexerCyclesWithoutNewContent(10);

        List<String> logs = customLogger.getLogs();
        assertEquals(assertionLogPresent(logs, invalidJcrPrimaryTypeLog()), 0);
    }

    // Lucene indexes are considered as sync in case "async property = sync" or there in no async property.
    // also indexing is happening in sync so on commit itself data will be indexed.
    // We are not logging invalid sync indexes
    @Test
    public void loggingInCaseOfSyncLuceneIndexDefinition() throws Exception {
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(invalidJcrPrimaryTypeIndexName);
        test1.setProperty("jcr:primaryType", invalidJcrPrimaryTypeForIndex, Type.NAME);
        test1.setProperty("type", "lucene");
        test1.setProperty("reindex", true);
//        test1.setProperty("async", "async");
        root.commit();

        root.getTree("/").addChild("testNode" + 1);
        root.getTree("/").addChild("testNode" + 2);
        root.getTree("/").addChild("testNode" + 3);
        root.getTree("/").addChild("testNode" + 4);
        root.commit();

        List<String> logs = customLogger.getLogs();
        assertEquals(assertionLogPresent(logs, invalidJcrPrimaryTypeLog()), 0);
    }

    private long assertionLogPresent(List<String> logs, String assertionLog) {
        long logCount = 0;
        for (String log : logs) {
            if (log.equals(assertionLog)) {
                logCount++;
            }
        }
        return logCount;
    }

    private String invalidJcrPrimaryTypeLog() {
        return "jcr:primaryType of index " + invalidJcrPrimaryTypeIndexName + " should be oak:QueryIndexDefinition instead of " + invalidJcrPrimaryTypeForIndex;
    }
}
