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

import java.util.List;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Lists;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MEMORY;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MONGO;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.RDB_H2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VersionGCSupportTest {

    private DocumentStoreFixture fixture;
    private DocumentStore store;
    private VersionGCSupport gcSupport;
    private List<String> ids = Lists.newArrayList();

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<DocumentStoreFixture> fixtures() {
        List<DocumentStoreFixture> fixtures = Lists.newArrayList();
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
        store.remove(Collection.NODES, ids);
        fixture.dispose();
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
            store.create(Collection.NODES, Lists.newArrayList(op));
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
        store.create(Collection.NODES, Lists.newArrayList(op));

        long reportedsecs = gcSupport.getOldestDeletedOnceTimestamp(Clock.SIMPLE, 1) / SECONDS.toMillis(1);
        assertTrue("diff (s) should be < 5: " + Math.abs(secs - reportedsecs), Math.abs(secs - reportedsecs) < 5);
    }

    private void assertPossiblyDeleted(long fromSeconds, long toSeconds, long num) {
        Iterable<NodeDocument> docs = gcSupport.getPossiblyDeletedDocs(SECONDS.toMillis(fromSeconds), SECONDS.toMillis(toSeconds));
        assertEquals(num, Iterables.size(docs));
    }
}
