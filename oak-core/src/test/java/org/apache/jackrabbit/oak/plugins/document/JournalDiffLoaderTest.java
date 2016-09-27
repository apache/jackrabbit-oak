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

import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JournalDiffLoaderTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void fromCurrentJournalEntry() throws Exception {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).getNodeStore();
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
                .setAsyncDelay(0).getNodeStore();
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
                .setAsyncDelay(0).getNodeStore();
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
                .setAsyncDelay(0).getNodeStore();
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
                .setAsyncDelay(0).getNodeStore();
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
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

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
