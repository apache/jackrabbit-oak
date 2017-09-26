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

import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfo;
import org.apache.jackrabbit.oak.plugins.document.mongo.replica.ReplicaSetInfoMock;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore.DocumentReadPreference;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceIT extends AbstractMongoConnectionTest {

    private MongoDocumentStore mongoDS;

    private DocumentMK mk2;

    private Clock clock;

    private ReplicaSetInfoMock replica;

    private ReplicaSetInfoMock.RevisionBuilder primary;

    private ReplicaSetInfoMock.RevisionBuilder secondary;

    @Override
    public void setUpConnection() throws Exception {
        clock = new Clock.Virtual();
        setRevisionClock(clock);
        setClusterNodeInfoClock(clock);
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        replica = ReplicaSetInfoMock.create(clock);
        mk = new DocumentMK.Builder()
                .clock(clock)
                .setClusterId(1)
                .setMongoDB(mongoConnection.getDB())
                .setLeaseCheck(false)
                .open();
        mongoDS = (MongoDocumentStore) mk.getDocumentStore();

        // use a separate connection for cluster node 2
        MongoConnection mongoConnection2 = connectionFactory.getConnection();
        mk2 = new DocumentMK.Builder()
                .clock(clock)
                .setClusterId(2)
                .setMongoDB(mongoConnection2.getDB())
                .setLeaseCheck(false)
                .open();
    }

    @Before
    public void createReplicaSet() {
        replica = ReplicaSetInfoMock.create(clock);

        primary = replica.addInstance(ReplicaSetInfo.MemberState.PRIMARY, "p1");
        secondary = replica.addInstance(ReplicaSetInfo.MemberState.SECONDARY, "s1");
        mongoDS.setReplicaInfo(replica);
    }

    @After
    public void tearDown() {
        // reset readWrite mode before shutting down
        mongoDS.setReadWriteMode("");
        mk2.dispose();
    }

    @Test
    public void testPreferenceConversion() throws Exception{
        primary.addRevisions(200);
        secondary.addRevisions(0);
        replica.updateRevisions();
        clock.waitUntil(500);
        assertEquals(300, replica.getLag());

        //For cacheAge < replicationLag result should be primary
        assertEquals(DocumentReadPreference.PRIMARY, mongoDS.getReadPreference(0));
        assertEquals(DocumentReadPreference.PRIMARY,
                mongoDS.getReadPreference((int) (replica.getLag() - 100)));

        //For Integer.MAX_VALUE it should be secondary as caller intends that value is stable
        assertEquals(DocumentReadPreference.PREFER_SECONDARY,
                mongoDS.getReadPreference(Integer.MAX_VALUE));

        //For all other cases depends on age
        assertEquals(DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH,
                mongoDS.getReadPreference(-1));
        assertEquals(DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH,
                mongoDS.getReadPreference((int) (replica.getLag() + 100)));
    }

    @Test
    public void testMongoReadPreferencesDefault() throws Exception{
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", null, DocumentReadPreference.PRIMARY));

        assertEquals(ReadPreference.primaryPreferred(),
                mongoDS.getMongoReadPreference(NODES,"foo", null, DocumentReadPreference.PREFER_PRIMARY));

        //By default Mongo read preference is primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", null, DocumentReadPreference.PREFER_SECONDARY));

        //Change the default and assert again
        mongoDS.getDBCollection(NODES).getDB().setReadPreference(ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(),
                mongoDS.getMongoReadPreference(NODES,"foo", null, DocumentReadPreference.PREFER_SECONDARY));

        //for case where parent age cannot be determined the preference should be primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", null, DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

        //For collection other than NODES always primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(SETTINGS,"foo", null, DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

    }

    @Test
    public void testMongoReadPreferences() throws Exception {
        ReadPreference testPref = ReadPreference.secondary();
        mongoDS.getDBCollection(NODES).getDB().setReadPreference(testPref);

        NodeStore extNodeStore = mk2.getNodeStore();
        NodeBuilder b1 = extNodeStore.getRoot().builder();
        b1.child("x").child("y").setProperty("xyz", "123");
        extNodeStore.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // wait until the change is visible
        NodeStore nodeStore = mk.getNodeStore();
        while (true) {
            if (nodeStore.getRoot().hasChildNode("x")) {
                break;
            } else {
                Thread.sleep(100);
            }
        }

        // the change hasn't been replicated yet, primary must be used
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES, null, "/x/y", DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

        // make the secondary up-to-date
        DocumentNodeState ns = (DocumentNodeState) nodeStore.getRoot().getChildNode("x").getChildNode("y");
        RevisionVector lastSeenRev = ns.getLastRevision().update(new Revision(Revision.getCurrentTimestamp(), 0, 1)); // add revision for the local cluster node

        primary.set(lastSeenRev);
        secondary.set(lastSeenRev);
        replica.updateRevisions();

        // change has been replicated by now, it's fine to use secondary
        assertEquals(testPref,
                mongoDS.getMongoReadPreference(NODES, null, "/x/y", DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));
    }

    @Test
    public void testMongoReadPreferencesForLocalChanges() throws Exception {
        //Change the default
        ReadPreference testPref = ReadPreference.secondary();
        mongoDS.getDBCollection(NODES).getDB().setReadPreference(testPref);

        NodeStore nodeStore = mk.getNodeStore();
        NodeBuilder b1 = nodeStore.getRoot().builder();
        b1.child("x").child("y");
        nodeStore.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        mongoDS.invalidateCache();

        // the local change hasn't been replicated yet, primary must be used
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES, null, "/x/y", DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

        // make the secondary up-to-date
        long now = Revision.getCurrentTimestamp();
        primary.addRevision(now, 0, 1, false);
        primary.addRevision(now, 0, 2, false);
        secondary.addRevision(now, 0, 1, false);
        secondary.addRevision(now, 0, 2, false);
        replica.updateRevisions();

        // local change has been replicated by now, it's fine to use secondary
        for (int i = 0; i < 400; i++) {
            assertEquals(testPref,
                    mongoDS.getMongoReadPreference(NODES, null, "/x/y", DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));
            Thread.sleep(5);
        }
    }

    @Test
    public void testReadWriteMode() throws Exception{
        assertEquals(ReadPreference.primary(), mongoDS.getConfiguredReadPreference(NODES));

        mongoDS.setReadWriteMode("readPreference=secondary&w=2&safe=true&j=true");

        assertEquals(ReadPreference.secondary(), mongoDS.getDBCollection(NODES).getReadPreference());
        assertEquals(2, mongoDS.getDBCollection(NODES).getWriteConcern().getW());
        assertTrue(mongoDS.getDBCollection(NODES).getWriteConcern().getJ());

        assertEquals(ReadPreference.secondary(), mongoDS.getConfiguredReadPreference(NODES));
    }
}
