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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JournalDiffLoaderTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void fromCurrentJournalEntry() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).getNodeStore();
        DocumentNodeState s1 = ns.getRoot();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        DocumentNodeState s2 = ns.getRoot();

        assertEquals(newHashSet("foo"), changeChildNodes(ns, s1, s2));
    }

    @Test
    public void fromSingleJournalEntry() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).getNodeStore();
        DocumentNodeState s1 = ns.getRoot();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        DocumentNodeState s2 = ns.getRoot();
        ns.runBackgroundOperations();

        assertEquals(newHashSet("foo"), changeChildNodes(ns, s1, s2));
    }

    @Test
    public void fromJournalAndCurrentEntry() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).getNodeStore();
        DocumentNodeState s1 = ns.getRoot();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        ns.runBackgroundOperations();

        builder = ns.getRoot().builder();
        builder.child("bar");
        merge(ns, builder);
        DocumentNodeState s2 = ns.getRoot();

        assertEquals(newHashSet("foo", "bar"), changeChildNodes(ns, s1, s2));
    }

    @Test
    public void fromMultipleJournalEntries() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).getNodeStore();
        DocumentNodeState s1 = ns.getRoot();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        ns.runBackgroundOperations();

        builder = ns.getRoot().builder();
        builder.child("bar");
        merge(ns, builder);
        ns.runBackgroundOperations();

        builder = ns.getRoot().builder();
        builder.child("baz");
        merge(ns, builder);
        ns.runBackgroundOperations();

        DocumentNodeState s2 = ns.getRoot();

        assertEquals(newHashSet("foo", "bar", "baz"), changeChildNodes(ns, s1, s2));
    }

    @Test
    public void fromPartialJournalEntry() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).getNodeStore();
        DocumentNodeState s1 = ns.getRoot();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        ns.runBackgroundOperations();

        builder = ns.getRoot().builder();
        builder.child("bar");
        merge(ns, builder);

        DocumentNodeState s2 = ns.getRoot();

        builder = ns.getRoot().builder();
        builder.child("baz");
        merge(ns, builder);
        ns.runBackgroundOperations();

        // will also report 'baz' because that change is also
        // present in the second journal entry
        assertEquals(newHashSet("foo", "bar", "baz"), changeChildNodes(ns, s1, s2));
    }

    @Test
    public void fromExternalChange() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder().setClusterId(1)
                .clock(clock).setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2)
                .clock(clock).setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        DocumentNodeState s1 = ns1.getRoot();
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo");
        merge(ns1, builder);

        builder = ns2.getRoot().builder();
        builder.child("bar");
        merge(ns2, builder);
        ns2.runBackgroundOperations();

        // create journal entry for ns1 and pick up changes from ns2
        ns1.runBackgroundOperations();

        builder = ns1.getRoot().builder();
        builder.child("baz");
        merge(ns1, builder);

        DocumentNodeState s2 = ns1.getRoot();
        assertEquals(newHashSet("foo", "bar", "baz"), changeChildNodes(ns1, s1, s2));
    }

    @Test
    public void withPath() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        ns.runBackgroundOperations();
        DocumentNodeState before = (DocumentNodeState) ns.getRoot().getChildNode("foo");
        builder = ns.getRoot().builder();
        builder.child("bar");
        merge(ns, builder);
        ns.runBackgroundOperations();
        builder = ns.getRoot().builder();
        builder.child("foo").child("a").child("b").child("c");
        merge(ns, builder);
        ns.runBackgroundOperations();
        builder = ns.getRoot().builder();
        builder.child("bar").child("a").child("b").child("c");
        merge(ns, builder);
        ns.runBackgroundOperations();
        DocumentNodeState after = (DocumentNodeState) ns.getRoot().getChildNode("foo");

        CacheStats cs = getMemoryDiffStats(ns);
        assertNotNull(cs);
        cs.resetStats();
        Set<String> changes = changeChildNodes(ns, before, after);
        assertEquals(1, changes.size());
        assertTrue(changes.contains("a"));
        // must only push /foo, /foo/a, /foo/a/b and /foo/a/b/c into cache
        assertEquals(4, cs.getElementCount());
    }

    // OAK-5228
    @Test
    public void useJournal() throws Exception {
        final AtomicInteger journalQueryCounter = new AtomicInteger();
        DocumentStore ds = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                if (collection == Collection.JOURNAL) {
                    journalQueryCounter.incrementAndGet();
                }
                return super.query(collection, fromKey, toKey, limit);
            }
        };
        DocumentNodeStore ns1 = builderProvider.newBuilder()
                .setClusterId(1).clock(clock).setLeaseCheck(false)
                .setDocumentStore(ds).setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setClusterId(2).clock(clock).setLeaseCheck(false)
                .setDocumentStore(ds).setAsyncDelay(0).getNodeStore();

        NodeBuilder b1 = ns1.getRoot().builder();
        NodeBuilder foo = b1.child("foo");
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD + 1; i++) {
            foo.child("n" + i);
        }
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(10));

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("foo").child("bar");
        merge(ns2, b2);
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();

        // collect journal entry created for /foo/nX
        new JournalGarbageCollector(ns1, TimeUnit.MINUTES.toMillis(5)).gc();

        // the next modification updates the root revision
        // for clusterId 1 past the removed journal entry
        b1 = ns1.getRoot().builder();
        b1.child("qux");
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        // remember before state
        DocumentNodeState fooBefore = (DocumentNodeState) ns1.getRoot().getChildNode("foo");

        b2 = ns2.getRoot().builder();
        b2.child("foo").child("baz");
        merge(ns2, b2);
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();

        b1 = ns1.getRoot().builder();
        b1.child("foo").child("bar").remove();
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        DocumentNodeState fooAfter = (DocumentNodeState) ns1.getRoot().getChildNode("foo");
        journalQueryCounter.set(0);
        final Set<String> changes = Sets.newHashSet();
        fooAfter.compareAgainstBaseState(fooBefore, new DefaultNodeStateDiff() {
            @Override
            public boolean childNodeChanged(String name,
                                            NodeState before,
                                            NodeState after) {
                changes.add(name);
                return true;
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                changes.add(name);
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                changes.add(name);
                return true;
            }
        });
        assertThat(changes, containsInAnyOrder("bar", "baz"));
        assertTrue("must use JournalDiffLoader",
                journalQueryCounter.get() > 0);
    }

    @Test
    public void emptyBranchCommit() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .clock(clock).setAsyncDelay(0).disableBranches().getNodeStore();
        DocumentStore store = ns.getDocumentStore();
        DocumentNodeState before = ns.getRoot();
        String id = Utils.getIdFromPath("/node-0");
        NodeBuilder builder = ns.getRoot().builder();
        int i = 0;
        while (store.find(Collection.NODES, id) == null) {
            NodeBuilder child = builder.child("node-" + i++);
            for (int j = 0; j < 20; j++) {
                child.setProperty("p-" + j, "value");
            }
        }
        merge(ns, builder);
        DocumentNodeState after = ns.getRoot();
        new JournalDiffLoader(before, after, ns).call();
    }

    private static CacheStats getMemoryDiffStats(DocumentNodeStore ns) {
        for (CacheStats cs : ns.getDiffCache().getStats()) {
            if (cs.getName().equals("Document-MemoryDiff")) {
                return cs;
            }
        }
        return null;
    }

    private static Set<String> changeChildNodes(DocumentNodeStore store,
                                                AbstractDocumentNodeState before,
                                                AbstractDocumentNodeState after) {
        String diff = new JournalDiffLoader(before, after, store).call();
        final Set<String> changes = newHashSet();
        DiffCache.parseJsopDiff(diff, new DiffCache.Diff() {
            @Override
            public boolean childNodeAdded(String name) {
                fail();
                return true;
            }

            @Override
            public boolean childNodeChanged(String name) {
                changes.add(name);
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name) {
                fail();
                return true;
            }
        });
        return changes;
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
