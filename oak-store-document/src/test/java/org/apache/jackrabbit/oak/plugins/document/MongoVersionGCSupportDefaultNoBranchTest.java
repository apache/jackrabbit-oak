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

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MONGO;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_NO_BRANCH;
import static org.apache.jackrabbit.oak.plugins.document.SplitOperations.forDocument;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MongoFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreTestHelper;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.prefetch.CountingMongoDatabase;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import com.mongodb.ReadPreference;

@RunWith(Parameterized.class)
public class MongoVersionGCSupportDefaultNoBranchTest {

    private static class Stats {
        private final VersionGCStats versionGCStats;
        private final int nodesDeleteMany;
        Stats(VersionGCStats stats, int nodesDeleteMany) {
            if (stats == null) {
                throw new IllegalArgumentException("stats must not be null");
            }
            if (nodesDeleteMany < 0) {
                throw new IllegalArgumentException("nodesDeleteMany must be positive");
            }
            this.versionGCStats = stats;
            this.nodesDeleteMany = nodesDeleteMany;
        }
    }

    private static Predicate<NodeDocument> splitDocsWithClusterId(final int clusterId) {
        return doc -> {
                if (!Utils.isPreviousDocId(doc.getId())) {
                    return false;
                }
                Path p = doc.getPath();
                p = p.getAncestor(1);
                Revision rev = Revision.fromString(p.getName());
                return rev.getClusterId() == clusterId;
            };
    }

    private static final Set<NodeDocument.SplitDocType> GC_TYPES = EnumSet.of(
            DEFAULT_LEAF, COMMIT_ROOT_ONLY, DEFAULT_NO_BRANCH);

    class MongoVersionGCSupportAccessor extends MongoVersionGCSupport {

        public MongoVersionGCSupportAccessor(MongoDocumentStore store) {
            super(store);
        }

        protected Iterable<NodeDocument> identifyGarbage(Set<SplitDocType> gcTypes,
                RevisionVector sweepRevs, long oldestRevTimeStamp) {
            return super.identifyGarbage(gcTypes, sweepRevs, oldestRevTimeStamp);
        }
    }
    // using AbstractTwoNodeTest as a helper rather than subclassing to simplify things
    private AbstractTwoNodeTest helper;
    private DocumentStoreFixture fixture;
    private DocumentStore store1;
    private DocumentStore store2;
    protected DocumentNodeStore ds1;
    protected DocumentNodeStore ds2;
    private VersionGCSupport gcSupport1;
    private CountingMongoDatabase db;
    private List<String> ids = new ArrayList<>();

    private Clock clock;
    private AtomicInteger offset = new AtomicInteger(0);

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<DocumentStoreFixture> fixtures() {
        List<DocumentStoreFixture> fixtures = new ArrayList<>();
        if (MONGO.isAvailable()) {
            fixtures.add(new MongoFixture() {
                @Override
                public DocumentStore createDocumentStore(Builder builder) {
                    try {
                        MongoConnection connection = MongoUtils.getConnection();
                        CountingMongoDatabase db = new CountingMongoDatabase(connection.getDatabase());
                        return new MongoDocumentStore(connection.getMongoClient(),db, builder);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        return fixtures;
    }

    public MongoVersionGCSupportDefaultNoBranchTest(final DocumentStoreFixture fixture) throws InterruptedException {
        this.fixture = fixture;
        helper = new AbstractTwoNodeTest(fixture);
        helper.setUp();
        this.store1 = helper.store1;
        this.store2 = helper.store2;
        this.clock = helper.clock;
        this.ds1 = helper.ds1;
        this.ds2 = helper.ds2;

        // Enforce primary read preference, otherwise tests may fail on a
        // replica set with a read preference configured to secondary.
        // Revision GC usually runs with a modified range way in the past,
        // which means changes made it to the secondary, but not in this
        // test using a virtual clock
        MongoTestUtils.setReadPreference(store1, ReadPreference.primary());
        MongoTestUtils.setReadPreference(store2, ReadPreference.primary());

        if (store1 instanceof MongoDocumentStore) {
            MongoDocumentStore mds = (MongoDocumentStore) store1;
            this.gcSupport1 = new MongoVersionGCSupportAccessor(mds);
            this.db = (CountingMongoDatabase) MongoDocumentStoreTestHelper.getDB(mds);
        } else {
            this.gcSupport1 = new VersionGCSupport(store1);
        }

        resetNodesDeleteMany();
    }

    private int resetNodesDeleteMany() {
        if (this.db != null) {
            return this.db.getCachedCountingCollection("nodes").resetNodesDeleteMany();
        } else {
            return 0;
        }
    }

    private int getNodesDeleteMany() {
        if (this.db != null) {
            return this.db.getCachedCountingCollection("nodes").getNodesDeleteMany();
        } else {
            return 0;
        }
    }

    @After
    public void after() throws Exception {
        store1.remove(Collection.NODES, ids);
        helper.tearDown();
        fixture.dispose();
    }

    public Stats deleteSplitDocuments(VersionGCSupport gcSupport, RevisionVector sweepRevs, long oldestRevTimeStamp) {
        int a = getNodesDeleteMany();
        VersionGCStats stats = new VersionGCStats();
        ((VersionGCSupport)gcSupport).deleteSplitDocuments(GC_TYPES, sweepRevs, oldestRevTimeStamp, stats);;
        int b = getNodesDeleteMany() - a;
        return new Stats(stats, b);
    }

    @Test
    public void testBothInstancesSplit_none_none() throws Exception {
        doTestBothInstancesSplit(0, 0);
    }

    @Test
    public void testBothInstancesSplit_none_one() throws Exception {
        doTestBothInstancesSplit(0, 1);
    }

    @Test
    public void testBothInstancesSplit_one_none() throws Exception {
        doTestBothInstancesSplit(1, 0);
    }

    @Test
    public void testBothInstancesSplit_two_three() throws Exception {
        doTestBothInstancesSplit(2, 3);
        assertEquals(4, getNodesDeleteMany());
    }

    @Test
    public void testBothInstancesSplit_three_two() throws Exception {
        doTestBothInstancesSplit(3, 2);
        assertEquals(4, getNodesDeleteMany());
    }

    @Test
    public void testBothInstancesSplit_many_many() throws Exception {
        doTestBothInstancesSplit(7, 9);
        assertEquals(4, getNodesDeleteMany());
    }

    public void doTestBothInstancesSplit(int numSplit1, int numSplit2) throws Exception {
        final int totalSplits = numSplit1 + numSplit2;

        waitALittle();

        modify(ds1, (b) -> b.child("foo"));

        waitALittle();

        assertNumSplitDocs(store1, "/foo", 0);
        assertNumSplitDocs(store2, "/foo", 0);

        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();

        for(int i = 0; i < numSplit1; i++) {
            splitPathNoBranch(ds1, "/foo", ds2);
        }
        for(int i = 0; i < numSplit2; i++) {
            splitPathNoBranch(ds2, "/foo", ds1);
        }

        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();

        waitALittle();
        if (numSplit1 > 0) {
            modify(ds1, (b) -> b.setProperty("sweeptrigger", offset.getAndIncrement()));
        }
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        if (numSplit2 > 0) {
            modify(ds2, (b) -> b.setProperty("sweeptrigger", offset.getAndIncrement()));
        }
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();
        long oldestRevTimeStamp = clock.getTime();
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();

        assertNumSplitDocs(store1, "/foo", totalSplits);
        assertNumSplitDocs(store2, "/foo", totalSplits);

        RevisionVector sweepRevs = ds1.getSweepRevisions();
        Iterable<NodeDocument> garbage = gcSupport1.identifyGarbage(GC_TYPES, sweepRevs, oldestRevTimeStamp);
        assertNotNull(garbage);
        assertEquals(totalSplits, Iterables.size(garbage));
        assertEquals(numSplit1, Iterables.size(Iterables.filter(garbage, splitDocsWithClusterId(1)::test)));
        assertEquals(numSplit2, Iterables.size(Iterables.filter(garbage, splitDocsWithClusterId(2)::test)));

        Stats stats = deleteSplitDocuments(gcSupport1, sweepRevs, oldestRevTimeStamp);
        assertNotNull(stats);
        assertEquals(totalSplits, stats.versionGCStats.splitDocGCCount);
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        assertNumSplitDocs(store1, "/foo", 0);
        assertNumSplitDocs(store2, "/foo", 0);

        waitALittle();
        oldestRevTimeStamp = clock.getTime();
        sweepRevs = ds1.getSweepRevisions();
        stats = deleteSplitDocuments(gcSupport1, sweepRevs, oldestRevTimeStamp);
        assertNotNull(stats);
        assertEquals(0, stats.versionGCStats.splitDocGCCount);
        assertEquals(1, stats.nodesDeleteMany);
    }

    public void modify(DocumentNodeStore ds, Function<NodeBuilder,NodeBuilder> builderFunction) throws CommitFailedException {
        NodeBuilder builder = ds.getRoot().builder();
        builderFunction.apply(builder);
        TestUtils.merge(ds, builder);
    }

    @Test
    public void testLastDefaultNoBranchDeletionRevs() throws Exception {
        final int NUM_BOTH_SPLITS = 10;
        for(int i = 0; i < NUM_BOTH_SPLITS; i++) {
            doTestBothInstancesSplit(1,2);
        }
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        int removesCount = resetNodesDeleteMany();
        // there should be 4 deleteMany calls : first call results in 3, second in 1 => 4
        assertEquals(4 * NUM_BOTH_SPLITS, removesCount);
        for(int i = 0; i < 10; i++) {
            doTestBothInstancesSplit(1,0);
        }
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        removesCount = resetNodesDeleteMany();
        // there should be between 3 and 4 deleteMany calls due to avoiding calls if nothing changed
        // but seems every other (more or less) iteration there is still a change in _lastRev
        // so the condition is strict >3 and <4
        assertTrue(3 * NUM_BOTH_SPLITS < removesCount);
        assertTrue(4 * NUM_BOTH_SPLITS > removesCount);
        for(int i = 0; i < 10; i++) {
            doTestBothInstancesSplit(0, 1);
        }
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        removesCount = resetNodesDeleteMany();
        // there should be between 3 and 4 deleteMany calls due to avoiding calls if nothing changed
        // but seems every other (more or less) iteration there is still a change in _lastRev
        // so the condition is strict >3 and <4
        assertTrue(3 * NUM_BOTH_SPLITS < removesCount);
        assertTrue(4 * NUM_BOTH_SPLITS > removesCount);

        assertNumSplitDocs(store1, "/foo", 0);
        assertNumSplitDocs(store2, "/foo", 0);

        waitALittle();
        long oldestRevTimeStamp = clock.getTime();
        @NotNull
        RevisionVector sweepRevs = ds1.getSweepRevisions();
        Stats stats = deleteSplitDocuments(gcSupport1, sweepRevs, oldestRevTimeStamp);
        assertNotNull(stats);
        assertEquals(0, stats.versionGCStats.splitDocGCCount);
        assertEquals(1, stats.nodesDeleteMany);
    }

    private void waitALittle() throws InterruptedException {
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
    }

    private void assertNumSplitDocs(DocumentStore ds, String path, int expected) throws Exception {
        assertEquals(expected, getNumSplitDocuments(ds, path));
    }

    private void splitPathNoBranch(DocumentNodeStore ns, String path, DocumentNodeStore... otherStores)
            throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder nb = builder;
        for (String name : PathUtils.elements(path)) {
            nb = nb.child(name);
        }
        nb.child("noBranchChild"); // DEFAULT_NO_BRANCH requires a child, otherwise it could be DEFAULT_LEAF
        nb.setProperty("o", offset.getAndIncrement());
        TestUtils.merge(ns, builder);
        nb.setProperty("o", offset.getAndIncrement());
        TestUtils.merge(ns, builder);
        nb.setProperty("o", offset.getAndIncrement());
        TestUtils.merge(ns, builder);

        DocumentStore store = ns.getDocumentStore();
        NodeDocument doc = store.find(NODES, getIdFromPath(path));
        List<UpdateOp> ops = forDocument(doc, ns, ns.getHeadRevision(),
                TestUtils.NO_BINARY, 2);
        assertFalse(ops.isEmpty());
        store.createOrUpdate(NODES, ops);

        ns.runBackgroundOperations();
        for (DocumentNodeStore dns : otherStores) {
            dns.runBackgroundOperations();
        }
    }

    private static int getNumSplitDocuments(DocumentStore store, String path)
            throws Exception {
        NodeDocument doc = store.find(NODES, getIdFromPath(path), -1);
        assertNotNull(doc);
        return Iterators.size(doc.getAllPreviousDocs());
    }
}
