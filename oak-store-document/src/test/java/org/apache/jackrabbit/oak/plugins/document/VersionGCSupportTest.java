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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBVersionGCSupport;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Comparator.comparing;
import static java.util.List.of;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.StreamSupport.stream;
import static org.apache.jackrabbit.guava.common.collect.Comparators.isInOrder;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MEMORY;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MONGO;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.RDB_H2;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.stats.Clock.SIMPLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VersionGCSupportTest {

    private final DocumentStoreFixture fixture;
    private final DocumentStore store;
    private final VersionGCSupport gcSupport;
    private final List<String> ids = new ArrayList<>();

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<DocumentStoreFixture> fixtures() {
        List<DocumentStoreFixture> fixtures = new ArrayList<>(3);
        if (RDB_H2.isAvailable()) {
            fixtures.add(RDB_H2);
        }
        if (MONGO.isAvailable()) {
            fixtures.add(MONGO);
        }
        if (MEMORY.isAvailable()) {
            fixtures.add(MEMORY);
        }
        return fixtures;
    }

    public VersionGCSupportTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
        this.store = fixture.createDocumentStore();
        if (this.store instanceof MongoDocumentStore) {
            // Enforce primary read preference, otherwise tests may fail on a
            // replica set with a read preference configured to secondary.
            // Revision GC usually runs with a modified range way in the past,
            // which means changes made it to the secondary, but not in this
            // test using a virtual clock
            MongoTestUtils.setReadPreference(store, ReadPreference.primary());
        }
        if (this.store instanceof MongoDocumentStore) {
            this.gcSupport = new MongoVersionGCSupport((MongoDocumentStore) store);
        } else if (this.store instanceof RDBDocumentStore) {
            this.gcSupport = new RDBVersionGCSupport((RDBDocumentStore) store);
        } else {
            this.gcSupport = new VersionGCSupport(store);
        }
    }

    @After
    public void after() throws Exception {
        store.remove(NODES, ids);
        fixture.dispose();
    }

    @Test
    public void getPossiblyDeletedDocs() {
        long offset = SECONDS.toMillis(42);
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(offset + SECONDS.toMillis(i), 0, 1);
            String id = getIdFromPath("/doc-" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            NodeDocument.setModified(op, r);
            NodeDocument.setDeleted(op, r, true);
            store.create(NODES, of(op));
        }

        assertPossiblyDeleted(0, 41, 0);
        assertPossiblyDeleted(0, 42, 0);
        assertPossiblyDeleted(0, 44, 0);
        assertPossiblyDeleted(0, 45, 3);
        assertPossiblyDeleted(0, 46, 3);
        assertPossiblyDeleted(0, 49, 3);
        assertPossiblyDeleted(0, 50, 5);
        assertPossiblyDeleted(0, 51, 5);
        assertPossiblyDeleted(39, 60, 5);
        assertPossiblyDeleted(40, 60, 5);
        assertPossiblyDeleted(41, 60, 5);
        assertPossiblyDeleted(42, 60, 5);
        assertPossiblyDeleted(44, 60, 5);
        assertPossiblyDeleted(45, 60, 2);
        assertPossiblyDeleted(47, 60, 2);
        assertPossiblyDeleted(48, 60, 2);
        assertPossiblyDeleted(49, 60, 2);
        assertPossiblyDeleted(50, 60, 0);
        assertPossiblyDeleted(51, 60, 0);
    }

    @Test
    public void getPossiblyModifiedDocs() {
        long offset = SECONDS.toMillis(42);
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(offset + SECONDS.toMillis(i), 0, 1);
            String id = getIdFromPath("/doc-modified" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            NodeDocument.setModified(op, r);
            store.create(NODES, of(op));
        }

        assertModified(0, 41, 0);
        assertModified(0, 42, 0);
        assertModified(0, 44, 0);
        assertModified(0, 45, 3);
        assertModified(0, 46, 3);
        assertModified(0, 49, 3);
        assertModified(0, 50, 5);
        assertModified(0, 51, 5);
        assertModified(39, 60, 5);
        assertModified(40, 60, 5);
        assertModified(41, 60, 5);
        assertModified(42, 60, 5);
        assertModified(44, 60, 5);
        assertModified(45, 60, 2);
        assertModified(47, 60, 2);
        assertModified(48, 60, 2);
        assertModified(49, 60, 2);
        assertModified(50, 60, 0);
        assertModified(51, 60, 0);
    }

    @Test
    public void findOldest() {
        // see OAK-8476
        long secs = 123456;
        long offset = SECONDS.toMillis(secs);
        Revision r = new Revision(offset, 0, 1);
        String id = getIdFromPath("/doc-del");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        NodeDocument.setModified(op, r);
        NodeDocument.setDeleted(op, r, true);
        store.create(NODES, of(op));

        long reportedSecs = gcSupport.getOldestDeletedOnceTimestamp(SIMPLE, 1) / SECONDS.toMillis(1);
        assertTrue("diff (s) should be < 5: " + Math.abs(secs - reportedSecs), Math.abs(secs - reportedSecs) < 5);
    }

    @Test
    public void findOldestModified() {
        long secs = 1234567;
        long offset = SECONDS.toMillis(secs);
        Revision r = new Revision(offset, 0, 1);
        String id = getIdFromPath("/doc-modified");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        NodeDocument.setModified(op, r);
        store.create(NODES, of(op));

        NodeDocument oldestModifiedDoc = gcSupport.getOldestModifiedDoc(SIMPLE);
        String oldestModifiedDocId = oldestModifiedDoc.getId();
        long reportedSecs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        assertTrue("diff (s) should be < 5: " + Math.abs(secs - reportedSecs), Math.abs(secs - reportedSecs) < 5);
        assertEquals(id, oldestModifiedDocId);
    }

    private void assertPossiblyDeleted(long fromSeconds, long toSeconds, long num) {
        Iterable<NodeDocument> docs = gcSupport.getPossiblyDeletedDocs(SECONDS.toMillis(fromSeconds), SECONDS.toMillis(toSeconds));
        assertEquals(num, stream(docs.spliterator(), false).count());
    }

    private void assertModified(long fromSeconds, long toSeconds, long num) {
        Iterable<NodeDocument> docs = gcSupport.getModifiedDocs(SECONDS.toMillis(fromSeconds), SECONDS.toMillis(toSeconds), 10, MIN_ID_VALUE);
        docs.forEach(d -> System.out.println(d.getModified() + " " + d.getId()));
        assertEquals(num, stream(docs.spliterator(), false).count());
        assertTrue(isInOrder(docs, (o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2)));
    }
}
