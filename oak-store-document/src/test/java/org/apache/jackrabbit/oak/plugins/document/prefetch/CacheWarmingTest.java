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
package org.apache.jackrabbit.oak.plugins.document.prefetch;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.CountingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;

import com.mongodb.client.MongoDatabase;

public class CacheWarmingTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private @Nullable MongoConnection mongoConnection;

    private CountingMongoDatabase db;

    @Test
    public void noop1() {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).build();
        ns.prefetch(null);
    }

    @Test
    public void noop2() {
        DocumentMK mk = builderProvider.newBuilder().open();
        DocumentStore s = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        ns.prefetch(null);
    }

    protected Clock getTestClock() throws InterruptedException {
        return Clock.SIMPLE;
    }

    private DocumentStore newMongoDocumentStore() throws InterruptedException {
        mongoConnection = connectionFactory.getConnection();
        assertNotNull(mongoConnection);
        db = new CountingMongoDatabase(mongoConnection.getDatabase());
        MongoUtils.dropCollections(db);
        DocumentMK.Builder builder = new DocumentMK.Builder()
                .clock(getTestClock()).setAsyncDelay(0);
        MongoDocumentStore store = new MongoDocumentStore(mongoConnection.getMongoClient(), db, builder);
//        mk = builder.setDocumentStore(store).open();
        return store;
    }

    @Test
    public void simple_nocleanCaches_noprefetch() throws Exception {
        doSimple(false, false);
    }

    @Test
    public void simple_cleanCaches_noprefetch() throws Exception {
        doSimple(true, false);
    }

    @Test
    public void simple_cleanCaches_withprefetch() throws Exception {
        doSimple(true, true);
    }

    private void doSimple(boolean cleanCaches, boolean prefetch)
            throws InterruptedException, CommitFailedException {
        System.out.println("=== doSimple( cleanCaches = " + cleanCaches + ", prefetch = " + prefetch + " )");
        DocumentStore ds = newMongoDocumentStore();
        final CountingDocumentStore cds = new CountingDocumentStore(ds);
        logAndReset("after init      ", cds);
        DocumentNodeStore store = builderProvider.newBuilder().setDocumentStore(cds).getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        SortedSet<String> children = new TreeSet<String>();
        // create a bunch of nodes
        // make it 4 levels deep to avoid things like 'readChildren' to be able to use optimizations such as query()
        for (int i = 0; i < 5*1024; i++) {
            String name = "c" + i;
            children.add("/" + name + "/" + name + "/" + name + "/" + name);
            builder.child(name).child(name).child(name).child(name);
        }
        logAndReset("before merge    ", cds);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        logAndReset("after merge     ", cds);
        if (cleanCaches) {
            // clear the cache via mbean
            store.getMBean().cleanAllCaches();
            // also clear the documentstore cache
            store.getDocumentStore().invalidateCache();
            logAndReset("after invalidate", cds);
        }
        if (prefetch) {
            final List<String> paths = new ArrayList<>(children);
            final java.util.Collection<String> withParents = withParents(paths);
            withParents.remove("/");
            store.prefetch(withParents);
            logAndReset("after prefetch  ", cds);
        }
        // read the children again
        DocumentNodeState root = store.getRoot();
        for (String aChild : root.getChildNodeNames()) {
            assertTrue(root.getChildNode(aChild).getChildNode(aChild).getChildNode(aChild).getChildNode(aChild).exists());
        }
        logAndReset("end             ", cds);
    }

    public static java.util.Collection<String> withParents(java.util.Collection<String> paths) {
        Set<String> s = new HashSet<>();
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            while (p != null) {
                s.add(p.toString());
                p = p.getParent();
            }
        }
        return new ArrayList<>(s);
    }

    private void logAndReset(String context, CountingDocumentStore cds) {
        System.out.println(context
                + " -> createOrUpdateCalls = " + cds.getNumCreateOrUpdateCalls(Collection.NODES)
                + ", findCalls = " + cds.getNumFindCalls(Collection.NODES)
                + ", queryCalls = " + cds.getNumQueryCalls(Collection.NODES)
                + ", removalCalls = " + cds.getNumRemoveCalls(Collection.NODES)
                + ", rawFindCalls = " + db.getCachedCountingCollection("nodes").getFindCounter()
                );
        db.getCachedCountingCollection("nodes").resetFindCounter();
        cds.resetCounters();
    }
}
