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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

/**
 * Tests for MongoMK GC
 */
public class MongoBlobGCTest extends AbstractMongoConnectionTest {
    private Clock clock;

    public HashSet<String> setUp(boolean deleteDirect) throws Exception {
        HashSet<String> set = new HashSet<String>();

        DocumentNodeStore s = mk.getNodeStore();
        NodeBuilder a = s.getRoot().builder();

        int number = 10;
        // track the number of the assets to be deleted
        List<Integer> processed = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 0; i < 5; i++) {
            int n = rand.nextInt(number);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
        for (int i = 0; i < number; i++) {
            Blob b = s.createBlob(randomStream(i, 4160));
            if (processed.contains(i)) {
                Iterator<String> idIter =
                        ((GarbageCollectableBlobStore) s.getBlobStore())
                                .resolveChunks(b.toString());
                while (idIter.hasNext()) {
                    set.add(idIter.next());
                }
            }
            a.child("c" + i).setProperty("x", b);
        }
        s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        if (deleteDirect) {
            for (int id : processed) {
                deleteFromMongo("c" + id);
            }
        } else {
            a = s.getRoot().builder();
            for (int id : processed) {
                a.child("c" + id).remove();
                s.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            long maxAge = 10; // hours
            // 1. Go past GC age and check no GC done as nothing deleted
            clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(maxAge));

            VersionGarbageCollector vGC = s.getVersionGarbageCollector();
            VersionGCStats stats = vGC.gc(0, TimeUnit.MILLISECONDS);
            Assert.assertEquals(processed.size(), stats.deletedDocGCCount);
        }

        return set;
    }

    private void deleteFromMongo(String nodeId) {
        DBCollection coll = mongoConnection.getDB().getCollection("nodes");
        BasicDBObject blobNodeObj = new BasicDBObject();
        blobNodeObj.put("_id", "1:/" + nodeId);
        coll.remove(blobNodeObj);
    }

    @Test
    public void gcDirectMongoDelete() throws Exception {
        HashSet<String> set = setUp(true);
        gc(set);
    }

    @Test
    public void gcVersionDelete() throws Exception {
        HashSet<String> set = setUp(false);
        gc(set);
    }

    private void gc(HashSet<String> set) throws IOException, Exception {
        DocumentNodeStore store = mk.getNodeStore();
        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new DocumentBlobReferenceRetriever(store),
                (GarbageCollectableBlobStore) store.getBlobStore(), "./target", 2048, true, 2, 0);
        gc.collectGarbage();

        Set<String> existing = iterate();
        boolean empty = Sets.intersection(set, existing).isEmpty();
        assertTrue(empty);
    }

    protected Set<String> iterate() throws Exception {
        GarbageCollectableBlobStore store = (GarbageCollectableBlobStore)
                mk.getNodeStore().getBlobStore();
        Iterator<String> cur = store.getAllChunkIds(0);

        Set<String> existing = Sets.newHashSet();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    @Override
    protected Clock getTestClock() throws InterruptedException {
        clock = new Clock.Virtual();
        clock.waitUntil(Revision.getCurrentTimestamp());
        return clock;
    }
}
