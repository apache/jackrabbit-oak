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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CheckpointsTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    
    private Clock clock;

    private DocumentNodeStore store;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        store = builderProvider.newBuilder().clock(clock).getNodeStore();
    }

    @Test
    public void testCheckpointPurge() throws Exception {
        long expiryTime = 1000;
        String r1 = store.getHeadRevision().toString();
        store.checkpoint(expiryTime);
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep().toString());

        //Trigger expiry by forwarding the clock to future
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

    @Test
    public void testCheckpointPurgeByCount() throws Exception {
        long expiryTime = TimeUnit.HOURS.toMillis(1);
        String head = store.getHeadRevision().toString();
        for(int i = 0; i < Checkpoints.CLEANUP_INTERVAL; i++){
            store.checkpoint(expiryTime);
            store.setRoot(new RevisionVector(Revision.newRevision(store.getClusterId())));
        }
        assertEquals(head, store.getCheckpoints().getOldestRevisionToKeep().toString());
        assertEquals(Checkpoints.CLEANUP_INTERVAL, store.getCheckpoints().size());

        //Trigger expiry by forwarding the clock to future
        clock.waitUntil(clock.getTime() + expiryTime);

        //Now creating the next checkpoint should trigger
        //cleanup
        store.checkpoint(expiryTime);
        assertEquals(1, store.getCheckpoints().size());
    }

    @Test
    public void multipleCheckpointOnSameRevision() throws Exception{
        long e1 = TimeUnit.HOURS.toMillis(1);
        long e2 = TimeUnit.HOURS.toMillis(3);

        //Create CP with higher expiry first and then one with
        //lower expiry
        String r2 = store.getHeadRevision().toString();
        String c2 = store.checkpoint(e2);

        //Do some commit to change headRevision
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x");
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String c1 = store.checkpoint(e1);

        clock.waitUntil(clock.getTime() + e1 + 1);

        //The older checkpoint was for greater duration so checkpoint
        //must not be GC
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep().toString());
        // after getOldestRevisionToKeep() only one must be remaining
        assertEquals(1, store.getCheckpoints().size());
        assertNull(store.retrieve(c1));
        assertNotNull(store.retrieve(c2));
    }

    @Test
    public void testGetOldestRevisionToKeep() throws Exception {
        long et1 = 1000, et2 = et1 + 1000;

        String r1 = store.getHeadRevision().toString();
        Revision c1 = Revision.fromString(store.checkpoint(et1));

        //Do some commit to change headRevision
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x");
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String r2 = store.getHeadRevision().toString();
        Revision c2 = Revision.fromString(store.checkpoint(et2));
        assertNotEquals(c1, c2);

        // r1 is older
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep().toString());

        long starttime = clock.getTime();

        //Trigger expiry by forwarding the clock to future e1
        clock.waitUntil(starttime + et1 + 1);
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep().toString());

        //Trigger expiry by forwarding the clock to future e2
        //This time no valid checkpoint
        clock.waitUntil(starttime + et2 + 1);
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

    // OAK-4552
    @Test
    public void testGetOldestRevisionToKeep2() throws Exception {
        long lifetime = TimeUnit.HOURS.toMillis(1);

        String r1 = store.getHeadRevision().toString();
        String c1 = store.checkpoint(lifetime);

        // Do some commit to change headRevision
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x");
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String r2 = store.getHeadRevision().toString();
        String c2 = store.checkpoint(lifetime);
        assertNotEquals(r1, r2);
        assertNotEquals(c1, c2);

        // r1 is older
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep().toString());
    }

    @Test
    public void checkpointRemove() throws Exception{
        long lifetime = TimeUnit.HOURS.toMillis(1);
        String r1 = store.getHeadRevision().toString();
        String cp1 = store.checkpoint(lifetime);

        //Do some commit to change headRevision
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("x");
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String r2 = store.getHeadRevision().toString();
        String cp2 = store.checkpoint(lifetime);

        assertEquals(2, store.getCheckpoints().size());
        assertEquals(r1, store.getCheckpoints().getOldestRevisionToKeep().toString());

        store.release(cp1);

        assertEquals(1, store.getCheckpoints().size());
        assertEquals(r2, store.getCheckpoints().getOldestRevisionToKeep().toString());

        store.release(cp2);
        assertEquals(0, store.getCheckpoints().size());
        assertNull(store.getCheckpoints().getOldestRevisionToKeep());
    }

    @Test
    public void readOldFormat() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        DocumentStore docStore = store.getDocumentStore();
        Map<String, String> empty = Collections.emptyMap();
        Revision r = Revision.fromString(
                store.checkpoint(Integer.MAX_VALUE, empty));

        Document doc = docStore.find(Collection.SETTINGS, "checkpoint");
        assertNotNull(doc);
        @SuppressWarnings("unchecked")
        Map<Revision, String> data = (Map<Revision, String>) doc.get("data");
        assertNotNull(data);
        assertEquals(1, data.size());
        assertTrue(data.containsKey(r));

        // manually update checkpoint data with old format
        UpdateOp update = new UpdateOp("checkpoint", false);
        long expires = clock.getTime() + 1000 + Integer.MAX_VALUE;
        update.setMapEntry("data", r, String.valueOf(expires));
        assertNotNull(docStore.findAndUpdate(Collection.SETTINGS, update));

        Checkpoints.Info info = store.getCheckpoints().getCheckpoints().get(r);
        assertNotNull(info);
        assertEquals(expires, info.getExpiryTime());
    }

    @Test
    public void expiryOverflow() throws Exception {
        clock.waitUntil(System.currentTimeMillis());
        Map<String, String> empty = Collections.emptyMap();
        Revision r = Revision.fromString(
                store.checkpoint(Long.MAX_VALUE, empty));
        Checkpoints.Info info = store.getCheckpoints().getCheckpoints().get(r);
        assertNotNull(info);
        assertEquals(Long.MAX_VALUE, info.getExpiryTime());
    }

    @Test
    public void userInfoNamedExpires() throws Exception {
        Map<String, String> props = ImmutableMap.of("expires", "today");
        Revision r = Revision.fromString(
                store.checkpoint(Integer.MAX_VALUE, props));
        Map<String, String> info = store.checkpointInfo(r.toString());
        assertNotNull(info);
        assertEquals(props, info);
    }

    @Test
    public void parseInfo() {
        long expires = System.currentTimeMillis();
        // initial 1.0 format: only expiry time
        Checkpoints.Info info = Checkpoints.Info.fromString(String.valueOf(expires));
        assertEquals(expires, info.getExpiryTime());
        // 1.2 format: json with expiry and info map
        String infoString = "{\"expires\":\"" + expires + "\",\"foo\":\"bar\"}";
        info = Checkpoints.Info.fromString(infoString);
        assertEquals(expires, info.getExpiryTime());
        assertEquals(Collections.singleton("foo"), info.get().keySet());
        assertEquals("bar", info.get().get("foo"));
        // 1.4 format: json with expiry, revision vector and info map
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(1, 0, 2);
        RevisionVector rv = new RevisionVector(r1, r2);
        infoString = "{\"expires\":\"" + expires +
                "\",\"rv\":\"" + rv.toString() +
                "\",\"foo\":\"bar\"}";
        info = Checkpoints.Info.fromString(infoString);
        assertEquals(expires, info.getExpiryTime());
        assertEquals(Collections.singleton("foo"), info.get().keySet());
        assertEquals("bar", info.get().get("foo"));
        assertEquals(rv, info.getCheckpoint());
        assertEquals(infoString, info.toString());
    }

    @Test
    public void crossClusterNodeCheckpoint() throws Exception {
        // use an async delay to ensure DocumentNodeStore.suspendUntil() works
        // but set it to a high value and control background ops manually in
        // this test
        final int asyncDelay = (int) TimeUnit.MINUTES.toMillis(1);
        DocumentStore store = new MemoryDocumentStore();
        final DocumentNodeStore ns1 = builderProvider.newBuilder().setClusterId(1)
                .setDocumentStore(store).setAsyncDelay(asyncDelay).getNodeStore();
        final DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).setAsyncDelay(asyncDelay).getNodeStore();

        // create node on ns1
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("foo");
        ns1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // make visible on ns2
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        // create checkpoint on ns1
        String cp1 = ns1.checkpoint(Long.MAX_VALUE);
        // retrieve checkpoint on ns2
        NodeState root = ns2.retrieve(cp1);
        assertNotNull(root);
        assertTrue(root.hasChildNode("foo"));
        ns2.release(cp1);

        // create node on ns1
        builder = ns1.getRoot().builder();
        builder.child("bar");
        ns1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // create checkpoint when 'bar' is not yet visible to ns2
        final String cp2 = ns1.checkpoint(Long.MAX_VALUE);
        // retrieve checkpoint on ns2
        final NodeState state[] = new NodeState[1];
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                state[0] = ns2.retrieve(cp2);
            }
        });
        t.start();
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        t.join();
        assertNotNull(state[0]);
        assertTrue(state[0].hasChildNode("bar"));
    }

    @Test
    public void crossClusterCheckpointNewClusterNode() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder().setClusterId(1)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        // create 'foo' on ns1
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("foo");
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // checkpoint sees 'foo' but not 'bar'
        String checkpoint = ns1.checkpoint(Long.MAX_VALUE);

        // create 'bar' on ns1
        b1 = ns1.getRoot().builder();
        b1.child("bar");
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // make visible
        ns1.runBackgroundOperations();

        // now start second node store
        DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("baz");
        ns2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState root = ns2.retrieve(checkpoint);
        assertNotNull(root);
        assertTrue(root.hasChildNode("foo"));
        assertFalse(root.hasChildNode("bar"));
        assertFalse(root.hasChildNode("baz"));
    }

    @Test
    public void crossClusterReadOldCheckpoint() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder().setClusterId(1)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("foo");
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns1.runBackgroundOperations();

        // manually create a check point in 1.2 format
        Revision headRev = Revision.fromString(ns1.getHeadRevision().toString());
        long expires = Long.MAX_VALUE;
        String data = "{\"expires\":\"" + expires + "\"}";
        UpdateOp update = new UpdateOp("checkpoint", false);
        update.setMapEntry("data", headRev, data);
        store.createOrUpdate(Collection.SETTINGS, update);

        // now start second node store
        DocumentNodeStore ns2 = builderProvider.newBuilder().setClusterId(2)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("baz");
        ns2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState root = ns2.retrieve(headRev.toString());
        assertNotNull(root);
        assertTrue(root.hasChildNode("foo"));
        assertFalse(root.hasChildNode("baz"));
    }

    @Test
    public void sameClusterReadOldCheckpoint() throws Exception {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = builderProvider.newBuilder().setClusterId(1)
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("foo");
        ns1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns1.runBackgroundOperations();

        // manually create a check point in 1.2 format
        Revision headRev = Revision.fromString(ns1.getHeadRevision().toString());
        long expires = Long.MAX_VALUE;
        String data = "{\"expires\":\"" + expires + "\"}";
        UpdateOp update = new UpdateOp("checkpoint", false);
        update.setMapEntry("data", headRev, data);
        store.createOrUpdate(Collection.SETTINGS, update);

        // create another node
        NodeBuilder b2 = ns1.getRoot().builder();
        b2.child("bar");
        ns1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState root = ns1.retrieve(headRev.toString());
        assertNotNull(root);
        assertTrue(root.hasChildNode("foo"));
        assertFalse(root.hasChildNode("bar"));
    }
}
