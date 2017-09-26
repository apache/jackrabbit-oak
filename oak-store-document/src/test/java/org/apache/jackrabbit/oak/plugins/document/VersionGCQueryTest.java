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

import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionGCQueryTest {

    @Rule
    public final DocumentMKBuilderProvider provider = new DocumentMKBuilderProvider();

    private Clock clock;
    private Set<String> prevDocIds = Sets.newHashSet();
    private DocumentStore store;
    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                if (collection == Collection.NODES && Utils.isPreviousDocId(key)) {
                    prevDocIds.add(key);
                }
                return super.find(collection, key);
            }
        };
        ns = provider.newBuilder().setDocumentStore(store)
                .setAsyncDelay(0).clock(clock).getNodeStore();
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    @Test
    public void noQueryForFirstLevelPrevDocs() throws Exception {
        // create some garbage
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            InputStream s = new RandomStream(10 * 1024, 42);
            PropertyState p = new BinaryPropertyState("p", ns.createBlob(s));
            builder.child("test").child("node-" + i).setProperty(p);
        }
        merge(builder);
        // overwrite with other binaries to force document splits
        builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            InputStream s = new RandomStream(10 * 1024, 17);
            PropertyState p = new BinaryPropertyState("p", ns.createBlob(s));
            builder.child("test").child("node-" + i).setProperty(p);
        }
        merge(builder);
        ns.runBackgroundOperations();
        builder = ns.getRoot().builder();
        builder.child("test").remove();
        merge(builder);
        ns.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        VersionGarbageCollector gc = new VersionGarbageCollector(
                ns, new VersionGCSupport(store));
        prevDocIds.clear();
        VersionGCStats stats = gc.gc(30, TimeUnit.MINUTES);
        assertEquals(11, stats.deletedDocGCCount);
        assertEquals(10, stats.splitDocGCCount);
        assertEquals(0, prevDocIds.size());
        assertEquals(1, Iterables.size(Utils.getAllDocuments(store)));
    }

    @Test
    public void queryDeepPreviousDocs() throws Exception {
        // create garbage until we have intermediate previous docs
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        merge(builder);
        String id = Utils.getIdFromPath("/test");
        while (!Iterables.any(store.find(Collection.NODES, id).getPreviousRanges().values(), INTERMEDIATE)) {
            InputStream s = new RandomStream(10 * 1024, 42);
            PropertyState p = new BinaryPropertyState("p", ns.createBlob(s));
            builder = ns.getRoot().builder();
            builder.child("test").setProperty(p);
            merge(builder);
            builder = ns.getRoot().builder();
            builder.child("test").remove();
            merge(builder);
            ns.runBackgroundOperations();
        }
        int numPrevDocs = Iterators.size(store.find(Collection.NODES, id).getAllPreviousDocs());
        assertEquals(1, Iterators.size(Utils.getRootDocument(store).getAllPreviousDocs()));

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        VersionGarbageCollector gc = new VersionGarbageCollector(
                ns, new VersionGCSupport(store));
        prevDocIds.clear();
        VersionGCStats stats = gc.gc(30, TimeUnit.MINUTES);
        assertEquals(1, stats.deletedDocGCCount);
        // GC also cleans up the previous doc on root
        assertEquals(numPrevDocs + 1, stats.splitDocGCCount);
        // but only does find calls for previous docs of /test
        assertEquals(numPrevDocs, prevDocIds.size());
        // at the end only the root document remains
        assertEquals(1, Iterables.size(Utils.getAllDocuments(store)));
    }

    private NodeState merge(NodeBuilder builder) throws CommitFailedException {
        return ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static final Predicate<Range> INTERMEDIATE =
            new Predicate<Range>() {
        @Override
        public boolean apply(Range input) {
            return input.height > 0;
        }
    };
}
