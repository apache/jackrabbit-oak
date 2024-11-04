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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.plugins.document.MongoBlobGCTest.randomStream;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class BlobReferenceIteratorTest {

    private final DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentNodeStore store;

    private DocumentNodeStore store2;

    public BlobReferenceIteratorTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<Object[]> fixtures() {
        List<Object[]> fixtures = new ArrayList<>();
        fixtures.add(new Object[] { new DocumentStoreFixture.MemoryFixture() });

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture();
        if (rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }
        return fixtures;
    }

    @Before
    public void setUp() throws Exception {
        if (fixture instanceof DocumentStoreFixture.RDBFixture) {
            ((DocumentStoreFixture.RDBFixture) fixture).setRDBOptions(
                    new RDBOptions().tablePrefix("T" + Long.toHexString(System.currentTimeMillis())).dropTablesOnClose(true));
        }
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        store = newDocumentNodeStore(1);
        // enforce primary read preference, otherwise test fails on a replica
        // set with a read preference configured to secondary.
        MongoTestUtils.setReadPreference(store, ReadPreference.primary());
    }

    private DocumentNodeStore newDocumentNodeStore(int clusterId) {
        return new DocumentMK.Builder()
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .clock(clock)
                .setDocumentStore(wrap(fixture.createDocumentStore(clusterId)))
                .setClusterId(clusterId)
                .setAsyncDelay(0)
                .getNodeStore();
    }

    @After
    public void tearDown() throws Exception {
        store.dispose();
        if (store2 != null) {
            store2.dispose();
        }
        fixture.dispose();
        Revision.resetClockToDefault();
    }

    @Test
    public void testBlobIterator() throws Exception {
        List<ReferencedBlob> blobs = new ArrayList<>();

        // 1. Set some single value Binary property
        for (int i = 0; i < 10; i++) {
            NodeBuilder b1 = store.getRoot().builder();
            Blob b = store.createBlob(randomStream(i, 4096));
            b1.child("x").child("y" + 1).setProperty("b" + i, b);
            blobs.add(new ReferencedBlob(b, "/x/y" + 1));
            store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        List<ReferencedBlob> collectedBlobs = ImmutableList.copyOf(store.getReferencedBlobsIterator());
        assertEquals(blobs.size(), collectedBlobs.size());
        assertEquals(new HashSet<>(blobs), new HashSet<>(collectedBlobs));
    }

    @Test
    public void recreateNodeAfterRevisionGC() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());

        String id = Utils.getIdFromPath("/test");

        NodeBuilder builder = store.getRoot().builder();
        Blob b = builder.createBlob(new RandomStream(100, 42));
        builder.child("test").setProperty("binary", b);
        merge(store, builder);

        builder = store.getRoot().builder();
        builder.child("test").remove();
        merge(store, builder);

        // create a second node store
        store2 = newDocumentNodeStore(2);
        assertFalse(store2.getRoot().hasChildNode("test"));
        NodeDocument doc = store2.getDocumentStore().find(Collection.NODES, id);
        assertNotNull(doc);

        // wait 60 minutes
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(60));

        // clean up garbage docs older than 30 minutes
        VersionGarbageCollector gc = store.getVersionGarbageCollector();
        VersionGCStats stats = gc.gc(30, TimeUnit.MINUTES);
        assertThat(stats.deletedDocGCCount, Matchers.equalTo(1));

        // recreate node via store2
        builder = store2.getRoot().builder();
        builder.child("test").setProperty("binary", b);
        merge(store2, builder);

        // make sure we do not fetch from cache
        doc = store2.getDocumentStore().find(Collection.NODES, id, 0);
        assertNotNull(doc);
        assertTrue(doc.hasBinary());
    }

    private static DocumentStore wrap(DocumentStore ds) {
        return new DocumentStoreTestWrapper(ds);
    }

    private static class DocumentStoreTestWrapper extends DocumentStoreWrapper {

        private DocumentStoreTestWrapper(DocumentStore store) {
            super(store);
        }

        @Override
        public void dispose() {
            // ignore
        }
    }
}
