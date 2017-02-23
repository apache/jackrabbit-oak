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

import java.util.Collections;
import java.util.List;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for OAK-3882
 */
public class CollisionWithSplitTest extends AbstractMongoConnectionTest {

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        mk = newDocumentMK(mongoConnection.getDB(), 2);
        ns1 = mk.getNodeStore();
    }

    @Before
    public void setup() throws Exception {
        ns2 = newDocumentMK(connectionFactory.getConnection().getDB(), 3).getNodeStore();
    }

    @After
    public void tearDown() {
        ns2.dispose();
    }

    @Test
    public void collisionAfterSplit() throws Exception {
        final int NUM_NODES = 10;
        NodeBuilder b1 = ns1.getRoot().builder();
        for (int i = 0; i < NUM_NODES; i++) {
            b1.child("node-" + i);
        }
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        Revision conflictRev = null;
        for (int i = 0; i < NUM_NODES; i++) {
            b1 = ns1.getRoot().builder();
            b1.setProperty("p", i);
            b1.child("node-" + i).remove();
            merge(ns1, b1);
            if (i == 0) {
                // assume a conflict is detected while writing on ns2
                // -> remember this revision
                conflictRev = ns1.getHeadRevision().getRevision(ns1.getClusterId());
            }
        }
        assertNotNull(conflictRev);

        // run document split on ns1
        DocumentStore store = ns1.getDocumentStore();
        NodeDocument doc = Utils.getRootDocument(store);
        List<UpdateOp> ops = SplitOperations.forDocument(doc, ns1,
                ns1.getHeadRevision(), NO_BINARY, NUM_NODES);
        assertFalse(ops.isEmpty());
        for (UpdateOp op : ops) {
            if (!op.isNew() ||
                    !store.create(NODES, Collections.singletonList(op))) {
                store.createOrUpdate(NODES, op);
            }
        }

        // attempt to set a property on a removed node
        String id = Utils.getIdFromPath("/node-0");
        UpdateOp op = new UpdateOp(id, false);
        Revision ourRev = ns2.newRevision();
        op.setMapEntry("p", ourRev, "v");
        NodeDocument.setModified(op, ourRev);
        NodeDocument.setCommitRoot(op, ourRev, 0);
        ns2.getDocumentStore().findAndUpdate(NODES, op);

        // now try to set a collision marker for the
        // committed revision on ns2
        doc = ns2.getDocumentStore().find(NODES, id);
        assertTrue(doc.getLocalCommitRoot().containsKey(conflictRev));
        Collision c = new Collision(doc, conflictRev, op, ourRev, ns2);
        assertEquals("Collision must match our revision (" + ourRev + "). " +
                "The conflict revision " + conflictRev + " is already committed.",
                ourRev, c.mark(ns2.getDocumentStore()));
    }

    private static DocumentMK newDocumentMK(DB db, int clusterId) {
        return new DocumentMK.Builder().setAsyncDelay(0)
                .setMongoDB(db)
                .setClusterId(clusterId)
                .open();
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
