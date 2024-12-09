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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.Long.MAX_VALUE;
import static java.util.Comparator.comparing;
import static java.util.List.of;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.guava.common.collect.Comparators.isInOrder;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NULL;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setModified;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.stats.Clock.SIMPLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VersionGCSupportTest extends AbstractDocumentStoreTest {

    private static final Set<String> EMPTY_STRING_SET = Collections.emptySet();

    private final DocumentStoreFixture fixture;
    private final DocumentStore store;
    private final VersionGCSupport gcSupport;
    private final List<String> ids;

    public VersionGCSupportTest(DocumentStoreFixture dsf) {
        super(dsf);

        this.fixture = super.dsf;
        this.store = super.ds;
        this.ids = super.removeMe;

        if (this.store instanceof MongoDocumentStore) {
            // Enforce primary read preference, otherwise tests may fail on a
            // replica set with a read preference configured to secondary.
            // Revision GC usually runs with a modified range way in the
            // past, which means changes made it to the secondary, but not in
            // this using a virtual clock
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

    @Test
    public void getPossiblyDeletedDocs() {
        long offset = SECONDS.toMillis(42);
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(offset + SECONDS.toMillis(i), 0, 1);
            String id = Utils.getIdFromPath("/doc-" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            NodeDocument.setModified(op, r);
            NodeDocument.setDeleted(op, r, true);
            store.create(Collection.NODES, of(op));
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
            setModified(op, r);
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
        String id = Utils.getIdFromPath("/doc-del");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        NodeDocument.setModified(op, r);
        NodeDocument.setDeleted(op, r, true);
        store.create(Collection.NODES, of(op));

        long reportedSecs = gcSupport.getOldestDeletedOnceTimestamp(SIMPLE, 1) / SECONDS.toMillis(1);
        assertTrue("diff (s) should be < 5: " + Math.abs(secs - reportedSecs), Math.abs(secs - reportedSecs) < 5);
    }

    @Test
    public void findDocument() {
        long secs = 1234567;
        long offset = SECONDS.toMillis(secs);
        Revision r = new Revision(offset, 0, 1);
        String id = getIdFromPath("/doc");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        setModified(op, r);
        store.create(NODES, of(op));

        NodeDocument doc = gcSupport.getDocument(id, of(ID)).orElse(NULL);
        assertEquals(id, doc.getId());
        assertEquals(1, doc.keySet().size());
    }

    @Test
    public void findDocumentWhenNotExist() {
        long secs = 1234567;
        long offset = SECONDS.toMillis(secs);
        Revision r = new Revision(offset, 0, 1);
        String id = getIdFromPath("/doc/3");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        setModified(op, r);
        store.create(NODES, of(op));

        NodeDocument doc = gcSupport.getDocument(getIdFromPath("/doc/4"), of()).orElse(NULL);
        assertNotEquals(id, doc.getId());
    }

    @Test
    public void findDocumentWithProjection() {
        long secs = 1234567;
        long offset = SECONDS.toMillis(secs);
        Revision r = new Revision(offset, 0, 1);
        String id = getIdFromPath("/doc/2");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        setModified(op, r);
        store.create(NODES, of(op));

        NodeDocument doc = gcSupport.getDocument(id, of(ID, MODIFIED_IN_SECS)).orElse(NULL);
        assertEquals(id, doc.getId());
        assertEquals(2, doc.keySet().size());
    }

    @Test
    public void findOldestModified() {
        long secs = 1234567;
        long offset = SECONDS.toMillis(secs);
        Revision r = new Revision(offset, 0, 1);
        String id = getIdFromPath("/doc-modified");
        ids.add(id);
        UpdateOp op = new UpdateOp(id, true);
        setModified(op, r);
        store.create(NODES, of(op));

        NodeDocument oldestModifiedDoc = gcSupport.getOldestModifiedDoc(SIMPLE).orElse(NULL);
        String oldestModifiedDocId = oldestModifiedDoc.getId();
        long reportedSecs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        assertTrue("diff (s) should be < 5: " + Math.abs(secs - reportedSecs), Math.abs(secs - reportedSecs) < 5);
        assertEquals(id, oldestModifiedDocId);
    }

    @Test
    public void findModifiedDocsWhenModifiedIsDifferent() {
        long secs = 42;
        long offset = SECONDS.toMillis(secs);
        int dsTimeRes = 5;
        long offsetForOthers = offset + SECONDS.toMillis(1 + dsTimeRes);

        List<UpdateOp> updateOps = new ArrayList<>();
        List<String> idsOfOldestDocs = new ArrayList<>();

        for (int i = 0; i < 5_000; i++) {
            Revision r = new Revision(offsetForOthers + (i * 5), 0, 1);
            String id = getIdFromPath("/x" + i);

            // make sure the oldest doc is "in between" with respect to IDS
            if (i >= 2000 && i < 2500) {
                r = new Revision(offset, 0, 1);
                id = getIdFromPath("/y" + i);
                idsOfOldestDocs.add(id);
            }

            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            updateOps.add(op);
        }
        // create 5_000 nodes
        store.create(NODES, updateOps);

        idsOfOldestDocs.sort(null);
        String minimalIdOfOldestDocs = idsOfOldestDocs.get(0);

        NodeDocument oldestModifiedDoc = gcSupport.getOldestModifiedDoc(SIMPLE).orElse(NULL);
        String oldestModifiedDocId = oldestModifiedDoc.getId();
        long oldestModifiedDocTs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        assertEquals("unexpected modified ts (" + fixture + ")", 40L, oldestModifiedDocTs);
        assertEquals("unexpected oldest id (" + fixture + ")", minimalIdOfOldestDocs, oldestModifiedDocId);
        oldestModifiedDocId = MIN_ID_VALUE;

        for (int i = 0; i < 5; i++) {
            Iterable<NodeDocument> modifiedDocs = gcSupport.getModifiedDocs(SECONDS.toMillis(oldestModifiedDocTs), MAX_VALUE, 1000, oldestModifiedDocId, EMPTY_STRING_SET, EMPTY_STRING_SET);
            assertTrue(isInOrder(modifiedDocs, (o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2)));
            long count = StreamSupport.stream(modifiedDocs.spliterator(), false).count();
            assertEquals(1000, count);
            for (NodeDocument modifiedDoc : modifiedDocs) {
                oldestModifiedDoc = modifiedDoc;
            }
            oldestModifiedDocId = oldestModifiedDoc.getId();
            oldestModifiedDocTs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        }
    }

    @Test
    public void findModifiedDocsWhenOldestDocIsPresent() {
        long offset = SECONDS.toMillis(42);
        List<UpdateOp> updateOps = new ArrayList<>(5_001);
        for (int i = 0; i < 5_001; i++) {
            Revision r = new Revision(offset, 0, 1);
            String id = getIdFromPath("/x" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            updateOps.add(op);
        }
        // create 5_001 nodes
        store.create(NODES, updateOps);

        NodeDocument oldestModifiedDoc = gcSupport.getOldestModifiedDoc(SIMPLE).orElse(NULL);
        String oldestModifiedDocId = oldestModifiedDoc.getId();
        long oldestModifiedDocTs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        assertEquals(40L, oldestModifiedDocTs);
        assertEquals("1:/x0", oldestModifiedDocId);
        oldestModifiedDocId = MIN_ID_VALUE;

        for(int i = 0; i < 5; i++) {
            Iterable<NodeDocument> modifiedDocs = gcSupport.getModifiedDocs(SECONDS.toMillis(oldestModifiedDocTs), MAX_VALUE, 1000, oldestModifiedDocId, EMPTY_STRING_SET, EMPTY_STRING_SET);
            assertTrue(isInOrder(modifiedDocs, (o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2)));
            long count = StreamSupport.stream(modifiedDocs.spliterator(), false).count();
            assertEquals(1000, count);
            for (NodeDocument modifiedDoc : modifiedDocs) {
                oldestModifiedDoc = modifiedDoc;
            }
            oldestModifiedDocId = oldestModifiedDoc.getId();
            oldestModifiedDocTs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        }

        // fetch last remaining document now
        Iterable<NodeDocument> modifiedDocs = gcSupport.getModifiedDocs(SECONDS.toMillis(oldestModifiedDocTs), MAX_VALUE, 1000, oldestModifiedDocId, EMPTY_STRING_SET, EMPTY_STRING_SET);
        assertEquals(1, StreamSupport.stream(modifiedDocs.spliterator(), false).count());
        assertTrue(isInOrder(modifiedDocs, (o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2)));
        oldestModifiedDoc = modifiedDocs.iterator().next();
        oldestModifiedDocId = oldestModifiedDoc.getId();
        oldestModifiedDocTs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);

        // all documents had been fetched, now we won't get any document
        modifiedDocs = gcSupport.getModifiedDocs(SECONDS.toMillis(oldestModifiedDocTs), MAX_VALUE, 1000, oldestModifiedDocId, EMPTY_STRING_SET, EMPTY_STRING_SET);
        assertEquals(0, StreamSupport.stream(modifiedDocs.spliterator(), false).count());

    }

    @Test
    public void findModifiedDocsWhenOldestDocIsAbsent() {

        NodeDocument oldestModifiedDoc = gcSupport.getOldestModifiedDoc(SIMPLE).orElse(NULL);
        String oldestModifiedDocId = MIN_ID_VALUE;
        long oldestModifiedDocTs = 0L;
        assertEquals(NULL, oldestModifiedDoc);

        long offset = SECONDS.toMillis(42);
        List<UpdateOp> updateOps = new ArrayList<>(5_000);
        for (int i = 0; i < 5_000; i++) {
            Revision r = new Revision(offset, 0, 1);
            String id = getIdFromPath("/x" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            updateOps.add(op);
        }
        // create 5_000 nodes
        store.create(NODES, updateOps);

        for(int i = 0; i < 5; i++) {
            Iterable<NodeDocument> modifiedDocs = gcSupport.getModifiedDocs(SECONDS.toMillis(oldestModifiedDocTs), MAX_VALUE, 1000, oldestModifiedDocId, EMPTY_STRING_SET, EMPTY_STRING_SET);
            assertTrue(isInOrder(modifiedDocs, (o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2)));
            long count = StreamSupport.stream(modifiedDocs.spliterator(), false).count();
            assertEquals(1000, count);
            for (NodeDocument modifiedDoc : modifiedDocs) {
                oldestModifiedDoc = modifiedDoc;
            }
            oldestModifiedDocId = oldestModifiedDoc.getId();
            oldestModifiedDocTs = ofNullable(oldestModifiedDoc.getModified()).orElse(0L);
        }

        // all documents had been fetched, now we won't get any document
        Iterable<NodeDocument> modifiedDocs = gcSupport.getModifiedDocs(SECONDS.toMillis(oldestModifiedDocTs), MAX_VALUE, 1000, oldestModifiedDocId, EMPTY_STRING_SET, EMPTY_STRING_SET);
        assertEquals(0, StreamSupport.stream(modifiedDocs.spliterator(), false).count());
    }

    @Test
    public void getModifiedDocsFromIdWithSameTimestampInMiddleOfBracket() {
        // Insert 5 documents with same timestamp
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(SECONDS.toMillis(928), 0, 1);
            String id = getIdFromPath("/test/" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            store.create(NODES, of(op));
        }

        // If we query with fromId = /test/2, it should return only documents 3 and 4
        String fromId = getIdFromPath("/test/2");
        Iterable<NodeDocument> result = gcSupport.getModifiedDocs(
                SECONDS.toMillis(925), SECONDS.toMillis(930), 10, fromId, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // It should return only 2 documents
        assertEquals(2, resultList.size());
        assertEquals(getIdFromPath("/test/3"), resultList.get(0).getId());
        assertEquals(getIdFromPath("/test/4"), resultList.get(1).getId());
    }

    @Test
    public void getModifiedDocsSequentiallyWithLimitSameTimestamp() {
        // Insert 5 documents with same timestamp
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(SECONDS.toMillis(925), 0, 1);
            String id = getIdFromPath("/test/" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            store.create(NODES, of(op));
        }

        // Fetch a maximum of 3 documents from the range 920 to 930.
        // This should return only documents 0 to 2 (inclusive)
        String fromId1 = MIN_ID_VALUE;
        Iterable<NodeDocument> result1 = gcSupport.getModifiedDocs(
                SECONDS.toMillis(920), SECONDS.toMillis(930), 3, fromId1, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList1 = new ArrayList<>();
        result1.forEach(resultList1::add);

        assertEquals(3, resultList1.size());
        assertEquals(getIdFromPath("/test/0"), resultList1.get(0).getId());
        assertEquals(getIdFromPath("/test/1"), resultList1.get(1).getId());
        assertEquals(getIdFromPath("/test/2"), resultList1.get(2).getId());

        // Let's simulate the next call takes the last processed document as the fromId and the last timestamp
        String fromId2 = resultList1.get(resultList1.size()-1).getId();
        long fromId2Ts = resultList1.get(resultList1.size()-1).getModified();
        assertEquals(getIdFromPath("/test/2"), fromId2);
        Iterable<NodeDocument> result2 = gcSupport.getModifiedDocs(
                SECONDS.toMillis(fromId2Ts), SECONDS.toMillis(930), 10, fromId2, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList2 = new ArrayList<>();
        result2.forEach(resultList2::add);

        // This should return only the last 2 documents
        assertEquals(2, resultList2.size());
        assertEquals(getIdFromPath("/test/3"), resultList2.get(0).getId());
        assertEquals(getIdFromPath("/test/4"), resultList2.get(1).getId());
    }

    @Test
    public void getModifiedDocsFromIdWithSameTimestampBeginningOfBracket() {
        // Insert 5 documents with same timestamp
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(SECONDS.toMillis(1000), 0, 1);
            String id = getIdFromPath("/test/" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            store.create(NODES, of(op));
        }

        // If we query with fromId="/test/2", it should return only documents 3 and 4.
        String fromId = getIdFromPath("/test/2");
        Iterable<NodeDocument> result = gcSupport.getModifiedDocs(
                SECONDS.toMillis(1000), SECONDS.toMillis(1100), 10, fromId, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // It should return only 2 documents
        assertEquals(2, resultList.size());
        assertEquals(getIdFromPath("/test/3"), resultList.get(0).getId());
        assertEquals(getIdFromPath("/test/4"), resultList.get(1).getId());
    }

    @Test
    public void getModifiedDocsFromIdWithSameTimestampEndOfBracket() {
        // Insert 5 documents with same timestamp
        for (int i = 0; i < 5; i++) {
            Revision r = new Revision(1004999L, 0, 1);
            String id = getIdFromPath("/test/" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            setModified(op, r);
            store.create(NODES, of(op));
        }

        // If we query with fromId="/test/2", it should return only documents 3 and 4.
        String fromId = getIdFromPath("/test/2");
        Iterable<NodeDocument> result = gcSupport.getModifiedDocs(
                SECONDS.toMillis(1000), SECONDS.toMillis(1005), 10, fromId, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // It should return only 2 documents
        assertEquals(2, resultList.size());
        assertEquals(getIdFromPath("/test/3"), resultList.get(0).getId());
        assertEquals(getIdFromPath("/test/4"), resultList.get(1).getId());
    }

    @Test
    public void getModifiedDocsFromIdWithDifferentTimestampsAlongBracket() {
        // Insert 30 documents with timestamps T=998 to T=1007
        // It will insert 3 documents with each timestamp
        for (int t = 998; t <= 1007; t++) {
            for (int n = 0; n < 3; n++) {
                String id = getIdFromPath("/test/" + t + "/" + n);
                ids.add(id);
                UpdateOp op = new UpdateOp(id, true);
                op.set(NodeDocument.MODIFIED_IN_SECS, t);
                store.create(NODES, of(op));
            }
        }

        String fromId1 = MIN_ID_VALUE;
        Iterable<NodeDocument> result1 = gcSupport.getModifiedDocs(
                SECONDS.toMillis(1000L), SECONDS.toMillis(1005L), 2, fromId1, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList1 = new ArrayList<>();
        result1.forEach(resultList1::add);

        // It should return only the first 2 documents in the range
        assertEquals(2, resultList1.size());
        assertEquals(getIdFromPath("/test/1000/0"), resultList1.get(0).getId());
        assertEquals(getIdFromPath("/test/1000/1"), resultList1.get(1).getId());

        // Let's simulate the next call takes the last processed document as the fromId for the next call
        String fromId2 = resultList1.get(resultList1.size()-1).getId();
        assertEquals(getIdFromPath("/test/1000/1"), fromId2);
        Iterable<NodeDocument> result2 = gcSupport.getModifiedDocs(
                SECONDS.toMillis(1000L), SECONDS.toMillis(1005L), 2, fromId2, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList2 = new ArrayList<>();
        result2.forEach(resultList2::add);

        // This should return only the last document with T=1000 and the first one with T=1001
        assertEquals(2, resultList2.size());
        assertEquals(getIdFromPath("/test/1000/2"), resultList2.get(0).getId());
        assertEquals(getIdFromPath("/test/1001/0"), resultList2.get(1).getId());

        // Let's simulate the next call takes the last processed document as the fromId and the last timestamp
        String fromId3 = resultList2.get(resultList1.size()-1).getId();
        long fromId3Ts = resultList2.get(resultList1.size()-1).getModified();
        assertEquals(getIdFromPath("/test/1001/0"), fromId3);
        Iterable<NodeDocument> result3 = gcSupport.getModifiedDocs(
                SECONDS.toMillis(fromId3Ts), SECONDS.toMillis(1005L), 7, fromId3, EMPTY_STRING_SET, EMPTY_STRING_SET);

        List<NodeDocument> resultList3 = new ArrayList<>();
        result3.forEach(resultList3::add);

        // This should return the next 7 documents with timestamps T=1001, T=1002 and T=1003
        assertEquals(7, resultList3.size());
        assertEquals(getIdFromPath("/test/1001/1"), resultList3.get(0).getId());
        assertEquals(getIdFromPath("/test/1001/2"), resultList3.get(1).getId());
        assertEquals(getIdFromPath("/test/1002/0"), resultList3.get(2).getId());
        assertEquals(getIdFromPath("/test/1002/1"), resultList3.get(3).getId());
        assertEquals(getIdFromPath("/test/1002/2"), resultList3.get(4).getId());
        assertEquals(getIdFromPath("/test/1003/0"), resultList3.get(5).getId());
        assertEquals(getIdFromPath("/test/1003/1"), resultList3.get(6).getId());
    }

    private void assertPossiblyDeleted(long fromSeconds, long toSeconds, long num) {
        Iterable<NodeDocument> docs = gcSupport.getPossiblyDeletedDocs(SECONDS.toMillis(fromSeconds), SECONDS.toMillis(toSeconds));
        assertEquals(num, StreamSupport.stream(docs.spliterator(), false).count());
    }

    private void assertModified(long fromSeconds, long toSeconds, long num) {
        Iterable<NodeDocument> docs = gcSupport.getModifiedDocs(SECONDS.toMillis(fromSeconds), SECONDS.toMillis(toSeconds), 10, MIN_ID_VALUE, EMPTY_STRING_SET, EMPTY_STRING_SET);
        assertEquals(num, StreamSupport.stream(docs.spliterator(), false).count());
        assertTrue(isInOrder(docs, (o1, o2) -> comparing(NodeDocument::getModified).thenComparing(Document::getId).compare(o1, o2)));
    }
}
