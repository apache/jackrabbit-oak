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

package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Iterables.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheInvalidationIT extends AbstractMongoConnectionTest {

    private DocumentNodeStore c1;
    private DocumentNodeStore c2;
    private int initialCacheSizeC1;

    @Before
    public void prepareStores() throws Exception {
        // TODO start with clusterNodeId 2, because 1 has already been
        // implicitly allocated in the base class
        c1 = createNS(2);
        c2 = createNS(3);
        initialCacheSizeC1 = getCurrentCacheSize(c1);
    }

    private int createScenario() throws CommitFailedException {
        //          a
        //        / | \
        //       /  c  \
        //      b       d
        //     /|\      |
        //    / | \     h
        //   e  f  g
        String[] paths = {
                "/a",
                "/a/c",
                "/a/b",
                "/a/b/e",
                "/a/b/f",
                "/a/b/g",
                "/a/d",
                "/a/d/h",
        };
        NodeBuilder root = getRoot(c1).builder();
        createTree(root, paths);
        c1.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(initialCacheSizeC1 + paths.length, getCurrentCacheSize(c1));

        runBgOps(c1, c2);
        return paths.length;
    }

    @Test
    public void testCacheInvalidation() throws CommitFailedException {
        final int totalPaths = createScenario();

        NodeBuilder b2 = getRoot(c2).builder();
        builder(b2, "/a/d").setProperty("foo", "bar");
        c2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Push pending changes at /a
        c2.runBackgroundOperations();

        //Refresh the head for c1
        c1.runBackgroundOperations();

        //Only 2 entries /a and /a/d would be invalidated
        // '/' would have been added to cache in start of backgroundRead
        //itself
        assertEquals(initialCacheSizeC1 + totalPaths - 2, size(ds(c1).getNodeDocumentCache().keys()));
    }

    @Test
    public void testCacheInvalidationHierarchicalNotExist()
            throws CommitFailedException {

        NodeBuilder b2 = getRoot(c2).builder();
        // we create x/other, so that x is known to have a child node
        b2.child("x").child("other");
        b2.child("y");
        c2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        c2.runBackgroundOperations();
        c1.runBackgroundOperations();

        // we check for the existence of "x/futureX", which
        // should create a negative entry in the cache
        NodeState x = getRoot(c1).getChildNode("x");
        assertTrue(x.exists());
        assertFalse(x.getChildNode("futureX").exists());
        // we don't check for the existence of "y/futureY"
        NodeState y = getRoot(c1).getChildNode("y");
        assertTrue(y.exists());

        // now we add both "futureX" and "futureY"
        // in the other cluster node
        b2.child("x").child("futureX").setProperty("z", "1");
        c2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        b2.child("y").child("futureY").setProperty("z", "2");
        c2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        c2.runBackgroundOperations();
        c1.runBackgroundOperations();

        // both nodes should now be visible
        assertTrue(getRoot(c1).getChildNode("y").getChildNode("futureY").exists());
        assertTrue(getRoot(c1).getChildNode("x").getChildNode("futureX").exists());

    }

    private int getCurrentCacheSize(DocumentNodeStore ds){
        return size(ds(ds).getNodeDocumentCache().keys());
    }

    private static void refreshHead(DocumentNodeStore store) {
        ds(store).find(Collection.NODES, Utils.getIdFromPath("/"), 0);
    }


    private static MongoDocumentStore ds(DocumentNodeStore ns) {
        return (MongoDocumentStore) ns.getDocumentStore();
    }

    private static void createTree(NodeBuilder node, String[] paths) {
        for (String path : paths) {
            createPath(node, path);
        }
    }

    private static NodeBuilder builder(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private static void createPath(NodeBuilder node, String path) {
        for (String element : PathUtils.elements(path)) {
            node = node.child(element);
        }
    }

    private static NodeState getRoot(NodeStore store) {
        return store.getRoot();
    }

    @After
    public void closeStores() {
        if (c2 != null) {
            c2.dispose();
        }
        if (c1 != null) {
            c1.dispose();
        }
    }

    private static void runBgOps(DocumentNodeStore... stores) {
        for (DocumentNodeStore ns : stores) {
            ns.runBackgroundOperations();
        }
    }

    private DocumentNodeStore createNS(int clusterId) throws Exception {
        MongoConnection mc = connectionFactory.getConnection();
        return new DocumentMK.Builder()
                          .setMongoDB(mc.getDB())
                          .setClusterId(clusterId)
                          //Set delay to 0 so that effect of changes are immediately reflected
                          .setAsyncDelay(0)
                          .setBundlingDisabled(true)
                          .setLeaseCheck(false)
                          .getNodeStore();
    }

}
