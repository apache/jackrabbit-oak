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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSplitDocTest;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestCollection;
import org.bson.conversions.Bson;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_MAX_REV_TIME_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;



/**
 * <code>MongoDocumentStoreTest</code>...
 */
public class MongoDocumentStoreTest extends AbstractMongoConnectionTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private TestStore store;

    private ExecutorService execService;

    private VersionGarbageCollector gc;
    
    private Clock clock;

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
    
//    @Test
//    public void splitDocGCHintNull() throws Exception {
//        MongoCollection<?> mc = store.getDBCollection(Collection.NODES);
//
//        assertTrue(hasIndex(mc, NodeDocument.SD_TYPE, NodeDocument.SD_MAX_REV_TIME_IN_SECS));
//
//        BasicDBObject keys = new BasicDBObject();
//        keys.put(SD_TYPE,1);
//        keys.put(SD_MAX_REV_TIME_IN_SECS, 1);
//        mc.dropIndex(keys);
//        assertFalse(hasIndex(mc,
//                NodeDocument.SD_TYPE, NodeDocument.SD_MAX_REV_TIME_IN_SECS));
//        final VersionGCStats stats = gc().get();
//
//
//        clock = new Clock.Virtual();
//        ExecutorService execService = Executors.newCachedThreadPool();
//        clock.waitUntil(System.currentTimeMillis());
//        Revision.setClock(clock);
//        DocumentNodeStore ns = builderProvider.newBuilder().clock(clock).setLeaseCheckMode(LeaseCheckMode.DISABLED)
//                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
//        VersionGarbageCollector gc = ns.getVersionGarbageCollector();
//
//        StringBuffer longpath = new StringBuffer();
//        while (longpath.length() < 380) {
//            longpath.append("thisisaverylongpath");
//        }
//
//        createCommitOnlyAndNoChildSplitDocument(ns, "parent1", "parent2", "child",longpath.toString());
//
//        // perform a change to make sure the sweep rev will be newer than
//        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
//        NodeBuilder builder = ns.getRoot().builder();
//        builder.child("qux");
//        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
//
//        ns.runBackgroundOperations();
//
//        // wait one hour
//        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));
//
//        int nodesBeforeGc = countNodeDocuments();
//        assertEquals(0, countStalePrev());
//     //   final VersionGCStats stats = gc().get();
//        int nodesAfterGc = countNodeDocuments();
//        assertEquals(3, countStalePrev());
//        assertEquals(3, nodesBeforeGc - nodesAfterGc);
//    //    assertEquals(3, stats.splitDocGCCount);
// 
//        Revision.resetClockToDefault();
//
//    }

    static final class TestStore extends MongoDocumentStore {
        TestStore(MongoConnection c, DocumentMK.Builder builder) {
            super(c.getMongoClient(), c.getDatabase(), builder);
        }
    }

//    private Future<VersionGCStats> gc() {
//        // run gc in a separate thread
//        return execService.submit(new Callable<VersionGCStats>() {
//            @Override
//            public VersionGCStats call() throws Exception {
//                return gc.gc(1, TimeUnit.MILLISECONDS);
//            }
//        });
//    }
//
//    private void createCommitOnlyAndNoChildSplitDocument(DocumentNodeStore ns, String parent1, String parent2,
//            String child, String longpath) throws CommitFailedException {
//        NodeBuilder b1 = ns.getRoot().builder();
//        b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent1).child(child).child("bar");
//        b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent2).child(child);
//        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
//
//        //Commit on a node which has a child and where the commit root
//        // is parent
//        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
//            b1 = ns.getRoot().builder();
//            //This updates a middle node i.e. one which has child bar
//            //Should result in SplitDoc of type PROP_COMMIT_ONLY
//            b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent1).child(child)
//                    .setProperty("prop", i);
//
//            //This should result in SplitDoc of type DEFAULT_NO_CHILD
//            b1.child("createCommitOnlyAndNoChildSplitDocument" + longpath).child(parent2).child(child)
//                    .setProperty("prop", i);
//            ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
//        }
//    }
//    
//
//    private void merge(DocumentNodeStore store, NodeBuilder builder) throws CommitFailedException {
//        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
//    }
//    
//    private int countStalePrev() {
//        int cnt = 0;
//        List<NodeDocument> nodes = store.query(NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE,
//                Integer.MAX_VALUE);
//        for (NodeDocument nodeDocument : nodes) {
//            cnt += nodeDocument.getStalePrev().size();
//        }
//        return cnt;
//    }
//
//    private int countNodeDocuments() {
//        return store.query(NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE).size();
//    }
}
