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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Map;
import java.util.List;

import com.mongodb.client.MongoCollection;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.read.ListAppender;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * <code>MongoDocumentStoreTest</code>...
 */
public class MongoDocumentStoreTest extends AbstractMongoConnectionTest {

    private TestStore store;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDBName());
        DocumentMK.Builder builder = new DocumentMK.Builder();
        store = new TestStore(mongoConnection, builder);
        builder.setDocumentStore(store);
        mk = builder.setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName()).open();
    }

    @Test
    public void defaultIndexes() {
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), Document.ID));
        assertFalse(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.SD_TYPE));
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.SD_TYPE, NodeDocument.SD_MAX_REV_TIME_IN_SECS));
        if (new MongoStatus(mongoConnection.getMongoClient(), mongoConnection.getDBName()).isVersion(3, 2)) {
            assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE, NodeDocument.MODIFIED_IN_SECS));
        } else {
            assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE));
        }
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.HAS_BINARY_FLAG));
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.MODIFIED_IN_SECS, Document.ID));
        assertFalse(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.MODIFIED_IN_SECS));
        assertTrue(hasIndex(store.getDBCollection(Collection.JOURNAL), JournalEntry.MODIFIED));
    }

    @Test
    public void oak6423() throws Exception {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        DocumentMK.Builder builder = new DocumentMK.Builder();
        TestStore s = new TestStore(c, builder);
        if (new MongoStatus(mongoConnection.getMongoClient(), mongoConnection.getDBName()).isVersion(3, 2)) {
            assertFalse(hasIndex(s.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE));
        } else {
            assertFalse(hasIndex(s.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE, NodeDocument.MODIFIED_IN_SECS));
        }
    }

    @Test
    public void getStats() throws Exception {
        Map<String, String> info = mk.getNodeStore().getDocumentStore().getStats();
        assertThat(info.keySet(), hasItem("nodes.count"));
        assertThat(info.keySet(), hasItem("clusterNodes.count"));
        assertThat(info.keySet(), hasItem("journal.count"));
        assertThat(info.keySet(), hasItem("settings.count"));
    }

    @Test
    public void readOnly() throws Exception {
        // setup must have created nodes collection with index on _bin
        MongoCollection<?> mc = mongoConnection.getDatabase()
                .getCollection(NODES.toString());
        assertTrue(hasIndex(mc, NodeDocument.HAS_BINARY_FLAG));
        mk.dispose();
        // remove the indexes
        mongoConnection = connectionFactory.getConnection();
        assertNotNull(mongoConnection);
        mc = mongoConnection.getDatabase().getCollection(NODES.toString());
        mc.dropIndexes();
        // must be gone now
        assertFalse(hasIndex(mc, NodeDocument.HAS_BINARY_FLAG));

        // start a new read-only DocumentNodeStore
        mk = newBuilder(mongoConnection.getMongoClient(),
                mongoConnection.getDBName()).setReadOnlyMode().open();
        // must still not exist when started in read-only mode
        assertFalse(hasIndex(mc, NodeDocument.HAS_BINARY_FLAG));
    }

    @Test
    public void documentSizeLoggingOnCreate() throws Exception {
        // Setup logging capture
        Logger logger = LoggerFactory.getLogger("org.apache.jackrabbit.oak.plugins.document.DocumentSizeLogger");
        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
        LoggerContext context = logbackLogger.getLoggerContext();
        ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.setContext(context);
        listAppender.start();
        logbackLogger.addAppender(listAppender);
        logbackLogger.setLevel(Level.WARN);

        try {
            // Create a store with document size logging threshold
            DocumentMK.Builder builder = new DocumentMK.Builder();
            builder.setDocumentSizeLoggingThreshold(1000); // 1KB threshold
            builder.setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName());
            DocumentMK testMk = builder.open();

            // Create a large document using the common Oak approach
            NodeBuilder rootBuilder = testMk.getNodeStore().getRoot().builder();
            NodeBuilder testNode = rootBuilder.child("test").child("large-node");
            testNode.setProperty("largeProperty", createLargeString(2000)); // 2KB string
            
            // Merge the changes
            testMk.getNodeStore().merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Verify warning was logged
            List<ch.qos.logback.classic.spi.ILoggingEvent> logEvents = listAppender.list;
            boolean foundWarning = false;
            for (ch.qos.logback.classic.spi.ILoggingEvent event : logEvents) {
                if (event.getLevel() == Level.WARN && 
                    event.getMessage().contains("Large document detected during create operation")) {
                    foundWarning = true;
                    assertTrue("Log message should contain document ID", 
                              event.getMessage().contains("/test/large-node"));
                    assertTrue("Log message should contain collection name", 
                              event.getMessage().contains("NODES"));
                    break;
                }
            }
            assertTrue("Warning about large document should be logged", foundWarning);
            
            testMk.dispose();
        } finally {
            logbackLogger.detachAppender(listAppender);
        }
    }

    @Test
    public void documentSizeLoggingOnRead() throws Exception {
        // Setup logging capture
        Logger logger = LoggerFactory.getLogger("org.apache.jackrabbit.oak.plugins.document.DocumentSizeLogger");
        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
        LoggerContext context = logbackLogger.getLoggerContext();
        ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.setContext(context);
        listAppender.start();
        logbackLogger.addAppender(listAppender);
        logbackLogger.setLevel(Level.WARN);

        try {
            // Create a store with document size logging threshold
            DocumentMK.Builder builder = new DocumentMK.Builder();
            builder.setDocumentSizeLoggingThreshold(1000); // 1KB threshold
            builder.setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName());
            DocumentMK testMk = builder.open();

            // Create a large document first using the common Oak approach
            NodeBuilder rootBuilder = testMk.getNodeStore().getRoot().builder();
            NodeBuilder testNode = rootBuilder.child("test").child("large-read-node");
            testNode.setProperty("largeProperty", createLargeString(2000)); // 2KB string
            
            // Merge the changes
            testMk.getNodeStore().merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Clear previous log events
            listAppender.list.clear();

            // Read the document using the document store
            NodeDocument doc = testMk.getNodeStore().getDocumentStore().find(NODES, "/test/large-read-node");
            assertNotNull("Document should be found", doc);

            // Verify warning was logged
            List<ch.qos.logback.classic.spi.ILoggingEvent> logEvents = listAppender.list;
            boolean foundWarning = false;
            for (ch.qos.logback.classic.spi.ILoggingEvent event : logEvents) {
                if (event.getLevel() == Level.WARN && 
                    event.getMessage().contains("Large document detected during read operation")) {
                    foundWarning = true;
                    assertTrue("Log message should contain document ID", 
                              event.getMessage().contains("/test/large-read-node"));
                    assertTrue("Log message should contain collection name", 
                              event.getMessage().contains("NODES"));
                    break;
                }
            }
            assertTrue("Warning about large document should be logged", foundWarning);
            
            testMk.dispose();
        } finally {
            logbackLogger.detachAppender(listAppender);
        }
    }

    @Test
    public void documentSizeLoggingDisabled() throws Exception {
        // Setup logging capture
        Logger logger = LoggerFactory.getLogger("org.apache.jackrabbit.oak.plugins.document.DocumentSizeLogger");
        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
        LoggerContext context = logbackLogger.getLoggerContext();
        ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.setContext(context);
        listAppender.start();
        logbackLogger.addAppender(listAppender);
        logbackLogger.setLevel(Level.WARN);

        try {
            // Create a store with document size logging disabled (threshold = 0)
            DocumentMK.Builder builder = new DocumentMK.Builder();
            builder.setDocumentSizeLoggingThreshold(0); // Disabled
            builder.setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName());
            DocumentMK testMk = builder.open();

            // Create a large document using the common Oak approach
            NodeBuilder rootBuilder = testMk.getNodeStore().getRoot().builder();
            NodeBuilder testNode = rootBuilder.child("test").child("disabled-logging-node");
            testNode.setProperty("largeProperty", createLargeString(2000)); // 2KB string
            
            // Merge the changes
            testMk.getNodeStore().merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Read the document using the document store
            NodeDocument doc = testMk.getNodeStore().getDocumentStore().find(NODES, "/test/disabled-logging-node");
            assertNotNull("Document should be found", doc);

            // Verify no warning was logged
            List<ch.qos.logback.classic.spi.ILoggingEvent> logEvents = listAppender.list;
            boolean foundWarning = false;
            for (ch.qos.logback.classic.spi.ILoggingEvent event : logEvents) {
                if (event.getLevel() == Level.WARN && 
                    event.getMessage().contains("Large document detected")) {
                    foundWarning = true;
                    break;
                }
            }
            assertFalse("No warning should be logged when threshold is 0", foundWarning);
            
            testMk.dispose();
        } finally {
            logbackLogger.detachAppender(listAppender);
        }
    }

    private String createLargeString(int sizeInBytes) {
        StringBuilder sb = new StringBuilder();
        String base = "0123456789"; // 10 characters
        int repetitions = sizeInBytes / 10;
        for (int i = 0; i < repetitions; i++) {
            sb.append(base);
        }
        // Add remaining characters to reach exact size
        int remaining = sizeInBytes % 10;
        if (remaining > 0) {
            sb.append(base.substring(0, remaining));
        }
        return sb.toString();
    }

    static final class TestStore extends MongoDocumentStore {
        TestStore(MongoConnection c, DocumentMK.Builder builder) {
            super(c.getMongoClient(), c.getDatabase(), builder);
        }
    }
}
