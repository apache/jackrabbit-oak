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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.mongo.CacheInvalidator.InvalidationResult;
import static org.junit.Assert.assertEquals;

public class CacheInvalidationIT extends AbstractMongoConnectionTest {

    private DocumentNodeStore c1;
    private DocumentNodeStore c2;

    @Before
    public void prepareStores() throws Exception {
        c1 = createNS(1);
        c2 = createNS(2);
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
        final int totalPaths = paths.length + 1; //1 extra for root
        NodeBuilder root = getRoot(c1).builder();
        createTree(root,paths);
        c1.merge(root, EmptyHook.INSTANCE, null);

        assertEquals(totalPaths, Iterables.size(ds(c1).getCacheEntries()));

        runBgOps(c1,c2);
        return totalPaths;
    }

    @Test
    public void testCacheInvalidation() throws CommitFailedException {
        final int totalPaths = createScenario();

        NodeBuilder b2 = getRoot(c2).builder();
        builder(b2,"/a/d").setProperty("foo", "bar");
        c2.merge(b2, EmptyHook.INSTANCE, null);

        //Push pending changes at /a
        c2.runBackgroundOperations();

        //Refresh the head for c1
        c1.runBackgroundOperations();

        //Only 2 entries /a and /a/d would be invalidated
        // '/' would have been added to cache in start of backgroundRead
        //itself
        assertEquals(totalPaths - 2,Iterables.size(ds(c1).getCacheEntries()));
    }

    @Test
    public void testCacheInvalidation_Hierarchical() throws CommitFailedException {
        final int totalPaths = createScenario();

        NodeBuilder b2 = getRoot(c2).builder();
        builder(b2,"/a/c").setProperty("foo", "bar");
        c2.merge(b2, EmptyHook.INSTANCE, null);

        //Push pending changes at /a
        c2.runBackgroundOperations();

        //Refresh the head for c1
        refreshHead(c1);

        InvalidationResult result = CacheInvalidator.createHierarchicalInvalidator(ds(c1)).invalidateCache();

        //Only 2 entries /a and /a/d would be invalidated
        // '/' would have been added to cache in start of backgroundRead
        //itself
        assertEquals(2, result.invalidationCount);

        //All excluding /a and /a/d would be updated. Also we exclude / from processing
        assertEquals(totalPaths - 3, result.upToDateCount);

        //3 queries would be fired for [/] [/a] [/a/b, /a/c, /a/d]
        assertEquals(2, result.queryCount);

        //Query would only have been done for first two levels /a and /a/b, /a/c, /a/d
        assertEquals(4, result.cacheEntriesProcessedCount);
    }

    @Test
    public void testCacheInvalidation_Linear() throws CommitFailedException {
        final int totalPaths = createScenario();

        NodeBuilder b2 = getRoot(c2).builder();
        builder(b2,"/a/c").setProperty("foo", "bar");
        c2.merge(b2, EmptyHook.INSTANCE, null);

        //Push pending changes at /a
        c2.runBackgroundOperations();

        //Refresh the head for c1
        refreshHead(c1);

        InvalidationResult result = CacheInvalidator.createLinearInvalidator(ds(c1)).invalidateCache();

        //Only 2 entries /a and /a/d would be invalidated
        // '/' would have been added to cache in start of backgroundRead
        //itself
        assertEquals(2, result.invalidationCount);

        //All excluding /a and /a/d would be updated
        assertEquals(totalPaths - 2, result.upToDateCount);

        //Only one query would be fired
        assertEquals(1, result.queryCount);

        //Query would be done for all the cache entries
        assertEquals(totalPaths, result.cacheEntriesProcessedCount);

    }

    private void refreshHead(DocumentNodeStore store){
        ds(store).find(Collection.NODES, Utils.getIdFromPath("/"), 0);
    }


    private static MongoDocumentStore ds(DocumentNodeStore ns){
        return (MongoDocumentStore) ns.getDocumentStore();
    }

    private void createTree(NodeBuilder node, String[] paths){
        for(String path : paths){
            createPath(node,path);
        }
    }

    private static NodeBuilder builder(NodeBuilder builder,String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private void createPath(NodeBuilder node, String path){
        for(String element : PathUtils.elements(path)){
            node = node.child(element);
        }
    }

    private static NodeState getRoot(NodeStore store) {
        return store.getRoot();
    }

    @After
    public void closeStores(){
        c1.dispose();
        c2.dispose();
    }

    private void runBgOps(DocumentNodeStore... stores){
        for(DocumentNodeStore ns : stores){
            ns.runBackgroundOperations();
        }
    }

    private DocumentNodeStore createNS(int clusterId) throws Exception {
        MongoConnection mc = MongoUtils.getConnection();
        return new DocumentMK.Builder()
                          .setMongoDB(mc.getDB())
                          .setClusterId(clusterId)
                          .setAsyncDelay(0) //Set delay to 0 so that effect of changes are immediately reflected
                          .getNodeStore();
    }

}
