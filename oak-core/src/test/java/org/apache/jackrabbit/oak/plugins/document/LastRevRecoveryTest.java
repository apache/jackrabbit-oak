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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class LastRevRecoveryTest {
    private DocumentNodeStore ds1;
    private DocumentNodeStore ds2;
    private MemoryDocumentStore sharedStore;

    @Before
    public void setUp(){
        sharedStore = new MemoryDocumentStore();
        ds1 = new DocumentMK.Builder()
                .setClusterId(1)
                .setAsyncDelay(0)
                .setDocumentStore(sharedStore)
                .getNodeStore();

        ds2 = new DocumentMK.Builder()
                .setClusterId(2)
                .setAsyncDelay(0)
                .setDocumentStore(sharedStore)
                .getNodeStore();
    }


    @Test
    public void testRecover() throws Exception {
        //1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();

        //lastRev are persisted directly for new nodes. In case of
        // updates they are persisted via background jobs

        //1.2 Get last rev populated for root node for ds2
        ds2.runBackgroundOperations();
        NodeBuilder b2 = ds2.getRoot().builder();
        b2.child("x").setProperty("f1","b1");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds2.runBackgroundOperations();

        //2. Add a new node /x/y/z
        b2 = ds2.getRoot().builder();
        b2.child("x").child("y").child("z").setProperty("foo", "bar");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Refresh DS1
        ds1.runBackgroundOperations();

        NodeDocument z1 = getDocument(ds1, "/x/y/z");
        NodeDocument y1 = getDocument(ds1, "/x/y");
        NodeDocument x1 = getDocument(ds1, "/x");

        Revision zlastRev2 = z1.getLastRev().get(2);
        assertNotNull(zlastRev2);

        //lastRev should not be updated for C #2
        assertNull(y1.getLastRev().get(2));

        LastRevRecoveryAgent recovery = new LastRevRecoveryAgent(ds1);

        //Do not pass y1 but still y1 should be updated
        recovery.recover(Iterators.forArray(x1,z1), 2);

        //Post recovery the lastRev should be updated for /x/y and /x
        assertEquals(zlastRev2, getDocument(ds1, "/x/y").getLastRev().get(2));
        assertEquals(zlastRev2, getDocument(ds1, "/x").getLastRev().get(2));
        assertEquals(zlastRev2, getDocument(ds1, "/").getLastRev().get(2));
    }

    private NodeDocument getDocument(DocumentNodeStore nodeStore, String path) {
        return nodeStore.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath(path));
    }
}
