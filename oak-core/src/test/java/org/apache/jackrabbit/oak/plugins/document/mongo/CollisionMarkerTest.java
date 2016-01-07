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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test for OAK-3344
 */
public class CollisionMarkerTest extends AbstractMongoConnectionTest {

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        mk = newDocumentMK(mongoConnection.getDB(), 2);
        ns1 = mk.getNodeStore();
    }

    @After
    public void tearDown() {
        ns2.dispose();
    }

    @Test
    public void conditionalCollisionUpdate() throws Exception {
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test");
        b1.child("node");
        merge(ns1, b1);

        ns1.runBackgroundOperations();
        // initialize second node store after background ops
        // on ns1. this makes sure ns2 sees all changes done so far
        ns2 = newDocumentMK(connectionFactory.getConnection().getDB(), 3).getNodeStore();

        b1 = ns1.getRoot().builder();
        b1.child("node").child("foo");
        b1.child("test").setProperty("p", 1);
        merge(ns1, b1);
        Revision head = ns1.getHeadRevision().getRevision(ns1.getClusterId());

        NodeBuilder b2 = ns2.getRoot().builder();
        b2.child("node").child("bar");
        b2.child("test").setProperty("p", 2);
        try {
            merge(ns2, b2);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }

        String rootId = Utils.getIdFromPath("/");
        NodeDocument root = ns2.getDocumentStore().find(NODES, rootId);
        assertFalse("root document must not have a collision marker for a" +
                " committed revision", root.getValueMap(COLLISIONS).containsKey(head));
    }

    private static DocumentMK newDocumentMK(DB db, int clusterId) {
        DocumentMK mk = new DocumentMK.Builder().setAsyncDelay(0)
                .setLeaseCheck(false)
                .setMongoDB(db)
                .setClusterId(clusterId)
                .open();
        // do not retry on conflicts
        mk.getNodeStore().setMaxBackOffMillis(0);
        return mk;
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
