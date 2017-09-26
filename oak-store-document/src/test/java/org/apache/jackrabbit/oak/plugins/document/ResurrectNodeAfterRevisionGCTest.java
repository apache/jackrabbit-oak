/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResurrectNodeAfterRevisionGCTest
        extends AbstractMultiDocumentStoreTest {

    private Clock c;
    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    public ResurrectNodeAfterRevisionGCTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Before
    public void prepare() throws Exception {
        removeMe.add(getIdFromPath("/"));
        removeMe.add(getIdFromPath("/foo"));
        removeMe.add(getIdFromPath("/baz"));
        removeMe.add(getIdFromPath("/foo/bar"));
        for (String id : removeMe) {
            ds.remove(Collection.NODES, id);
        }
        for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(ds)) {
            ds.remove(Collection.CLUSTER_NODES, doc.getId());
        }
        c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        Revision.setClock(c);
        ns1 = new DocumentMK.Builder().setAsyncDelay(0).clock(c)
                .setClusterId(1).setDocumentStore(wrap(ds1)).getNodeStore();
        ns2 = new DocumentMK.Builder().setAsyncDelay(0).clock(c)
                .setClusterId(2).setDocumentStore(wrap(ds2)).getNodeStore();
    }

    @After
    public void disposeNodeStores() {
        ns1.dispose();
        ns2.dispose();
        Revision.resetClockToDefault();
    }

    @Test
    public void resurrectAfterGC() throws Exception {
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo").child("bar");
        builder.child("baz");
        merge(ns1, builder);
        builder = ns1.getRoot().builder();
        builder.child("foo").child("bar").setProperty("p", 0);
        merge(ns1, builder);
        ns1.runBackgroundOperations();

        ns2.runBackgroundOperations();
        assertTrue(ns2.getRoot().getChildNode("foo").exists());
        assertTrue(ns2.getRoot().getChildNode("foo").getChildNode("bar").exists());
        assertEquals(0, ns2.getRoot().getChildNode("foo").getChildNode("bar").getLong("p"));

        builder = ns1.getRoot().builder();
        // removing both will result in a commit root on 0:/
        builder.child("foo").remove();
        builder.child("baz").remove();
        merge(ns1, builder);
        ns1.runBackgroundOperations();

        ns2.runBackgroundOperations();
        assertFalse(ns2.getRoot().getChildNode("foo").exists());
        NodeDocument doc = ds2.find(Collection.NODES, getIdFromPath("/foo/bar"));
        assertNotNull(doc);
        NodeState state = doc.getNodeAtRevision(ns2, ns2.getHeadRevision(), null);
        assertNull(state);

        c.waitUntil(c.getTime() + TimeUnit.MINUTES.toMillis(5));
        VersionGarbageCollector gc = ns1.getVersionGarbageCollector();
        VersionGCStats stats = gc.gc(1, TimeUnit.MINUTES);
        assertEquals(3, stats.deletedDocGCCount);

        builder = ns1.getRoot().builder();
        builder.child("foo").child("bar");
        merge(ns1, builder);
        builder = ns1.getRoot().builder();
        // setting 'x' puts the commit root on /foo
        builder.child("foo").setProperty("x", 0);
        builder.child("foo").child("bar").setProperty("p", 1);
        merge(ns1, builder);
        builder = ns1.getRoot().builder();
        builder.child("foo").child("bar").setProperty("p", 2);
        merge(ns1, builder);
        ns1.runBackgroundOperations();

        ns2.runBackgroundOperations();
        assertTrue(ns2.getRoot().getChildNode("foo").exists());
        assertTrue(ns2.getRoot().getChildNode("foo").getChildNode("bar").exists());
        assertEquals(2, ns2.getRoot().getChildNode("foo").getChildNode("bar").getLong("p"));
    }

    @Test
    public void resurrectInvalidate() throws Exception {
        resurrectInvalidate(new Invalidate() {
            @Override
            public void perform(DocumentStore store, Iterable<String> ids) {
                store.invalidateCache(ids);
            }
        });
    }

    @Test
    public void resurrectInvalidateAll() throws Exception {
        resurrectInvalidate(new Invalidate() {
            @Override
            public void perform(DocumentStore store, Iterable<String> ids) {
                store.invalidateCache();
            }
        });
    }

    @Test
    public void resurrectInvalidateIndividual() throws Exception {
        resurrectInvalidate(new Invalidate() {
            @Override
            public void perform(DocumentStore store, Iterable<String> ids) {
                for (String id : ids) {
                    store.invalidateCache(Collection.NODES, id);
                }
            }
        });
    }

    @Test
    public void resurrectInvalidateWithModified() throws Exception {
        resurrectInvalidateWithModified(new Invalidate() {
            @Override
            public void perform(DocumentStore store, Iterable<String> ids) {
                store.invalidateCache(ids);
            }
        });
    }

    @Test
    public void resurrectInvalidateAllWithModified() throws Exception {
        resurrectInvalidateWithModified(new Invalidate() {
            @Override
            public void perform(DocumentStore store, Iterable<String> ids) {
                store.invalidateCache();
            }
        });
    }

    @Test
    public void resurrectInvalidateIndividualWithModified() throws Exception {
        resurrectInvalidateWithModified(new Invalidate() {
            @Override
            public void perform(DocumentStore store, Iterable<String> ids) {
                for (String id : ids) {
                    store.invalidateCache(Collection.NODES, id);
                }
            }
        });
    }

    private void resurrectInvalidateWithModified(Invalidate inv)
            throws Exception {
        UpdateOp op = new UpdateOp(getIdFromPath("/foo"), true);
        op.set("p", 0);
        op.set(NodeDocument.MODIFIED_IN_SECS, 50);
        assertTrue(ds1.create(Collection.NODES, Lists.newArrayList(op)));
        NodeDocument doc = ds2.find(Collection.NODES, op.getId());
        assertNotNull(doc);
        assertEquals(0L, doc.get("p"));
        assertEquals(50L, (long) doc.getModified());

        ds1.remove(Collection.NODES, op.getId());
        // recreate with different value for 'p'
        op.set("p", 1);
        op.set(NodeDocument.MODIFIED_IN_SECS, 55);
        assertTrue(ds1.create(Collection.NODES, Lists.newArrayList(op)));

        inv.perform(ds2, Lists.newArrayList(op.getId()));
        doc = ds2.find(Collection.NODES, op.getId());
        assertNotNull(doc);
        assertEquals(1L, doc.get("p"));
        assertEquals(55L, (long) doc.getModified());
    }

    private void resurrectInvalidate(Invalidate inv) throws Exception {
        UpdateOp op = new UpdateOp(getIdFromPath("/foo"), true);
        op.set("p", 0);
        assertTrue(ds1.create(Collection.NODES, Lists.newArrayList(op)));
        NodeDocument doc = ds2.find(Collection.NODES, op.getId());
        assertNotNull(doc);
        assertEquals(0L, doc.get("p"));

        ds1.remove(Collection.NODES, op.getId());
        // recreate with different value for 'p'
        op.set("p", 1);
        assertTrue(ds1.create(Collection.NODES, Lists.newArrayList(op)));

        inv.perform(ds2, Lists.newArrayList(op.getId()));
        doc = ds2.find(Collection.NODES, op.getId());
        assertNotNull(doc);
        assertEquals(1L, doc.get("p"));
    }

    interface Invalidate {
        void perform(DocumentStore store, Iterable<String> ids);
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static DocumentStore wrap(DocumentStore store) {
        return new DSWrapper(store);
    }

    private static class DSWrapper extends DocumentStoreWrapper {

        DSWrapper(DocumentStore store) {
            super(store);
        }

        @Override
        public void dispose() {
            // ignore
        }
    }
}
