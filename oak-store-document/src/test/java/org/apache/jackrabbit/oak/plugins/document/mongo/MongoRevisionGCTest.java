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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoCollection;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.TestUtils;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_MAX_REV_TIME_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SD_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.getAllPreviousDocs;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.getMaxRangeHeight;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoRevisionGCTest extends AbstractMongoConnectionTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    // use a virtual clock, so we can fast-forward in time for revision gc
    private final Clock clock = new Clock.Virtual();

    @Override
    protected Clock getTestClock() throws InterruptedException {
        return clock;
    }

    @Override
    protected DocumentMK.Builder addToBuilder(DocumentMK.Builder mk) {
        // disable lease check because the test fiddles with the virtual clock
        // disable background operations by setting delay to zero. tests perform
        // background operations explicitly to control when changes become
        // visible to other cluster nodes
        return mk.setLeaseCheckMode(LeaseCheckMode.DISABLED).setAsyncDelay(0);
    }

    @Test
    public void gcWithoutCompoundIndex() throws Exception {
        DocumentNodeStore ns = mk.getNodeStore();
        // create some garbage
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        builder = ns.getRoot().builder();
        builder.child("foo").remove();
        merge(ns, builder);
        // make sure those changes are visible to other cluster nodes
        ns.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(2));

        MongoCollection<?> nodes = mongoConnection.getDatabase()
                .getCollection(NODES.toString());
        assertTrue(hasIndex(nodes, SD_TYPE, SD_MAX_REV_TIME_IN_SECS));

        // delete compound index
        BasicDBObject keys = new BasicDBObject();
        keys.put(SD_TYPE,1);
        keys.put(SD_MAX_REV_TIME_IN_SECS, 1);
        nodes.dropIndex(keys);
        assertFalse(hasIndex(nodes, SD_TYPE, SD_MAX_REV_TIME_IN_SECS));

        try {
            ns.getVersionGarbageCollector().gc(3, TimeUnit.HOURS);
            fail("gc must fail with exception");
        } catch (MongoQueryException e) {
            // expected
        }

        // get a garbage collector from a fresh document node store
        VersionGarbageCollector gc = builderProvider.newBuilder().setMongoDB(
                mongoConnection.getMongoClient(), mongoConnection.getDBName())
                .clock(clock).setClusterId(2)
                .getNodeStore().getVersionGarbageCollector();

        // clean up garbage older than one hour
        VersionGCStats stats = gc.gc(1, TimeUnit.HOURS);
        assertEquals(1, TestUtils.getDeletedDocGCCount(stats));

    }

    @Test // OAK-9700
    public void malformedPreviousDocument() throws Exception {
        DocumentNodeStore ns = mk.getNodeStore();
        NodeDocument prev = null;
        NodeBuilder b;
        for (;;) {
            b = ns.getRoot().builder();
            b.child("child");
            merge(ns, b);
            b = ns.getRoot().builder();
            b.child("child").remove();
            merge(ns, b);
            ns.runBackgroundOperations();
            NodeDocument doc = ns.getDocumentStore().find(NODES, Utils.getIdFromPath("/child"));
            assertNotNull(doc);
            for (Iterator<NodeDocument> it = getAllPreviousDocs(doc); it.hasNext(); ) {
                prev = it.next();
            }
            if (getMaxRangeHeight(doc) > 0) {
                break;
            }
        }

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));

        // fiddle with prev document in cache and replace it with an invalid one
        MongoDocumentStore store = (MongoDocumentStore) ns.getDocumentStore();
        NodeDocumentCache cache = store.getNodeDocumentCache();
        assertNotNull(prev);
        String prevId = prev.getId();
        assertNotNull(prevId);
        cache.invalidate(prevId);
        cache.get(prevId, () -> new NodeDocument(store, clock.getTime()));

        // must succeed without exception
        try {
            ns.getVersionGarbageCollector().gc(1, HOURS);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }
}