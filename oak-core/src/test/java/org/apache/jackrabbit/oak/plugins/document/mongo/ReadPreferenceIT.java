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

import java.util.concurrent.TimeUnit;

import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore.DocumentReadPreference;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceIT extends AbstractMongoConnectionTest {

    private MongoDocumentStore mongoDS;

    private Clock clock;

    private long replicationLag;

    @Override
    public void setUpConnection() throws Exception {
        clock = new Clock.Virtual();
        replicationLag = TimeUnit.SECONDS.toMillis(10);
        mongoConnection = connectionFactory.getConnection();
        mk = new DocumentMK.Builder()
                .setMaxReplicationLag(replicationLag, TimeUnit.MILLISECONDS)
                .setMongoDB(mongoConnection.getDB())
                .setClusterId(1)
                .setLeaseCheck(false)
                .open();
        mongoDS = (MongoDocumentStore) mk.getDocumentStore();
    }

    @Test
    public void testPreferenceConversion() throws Exception{
        //For cacheAge < replicationLag result should be primary
        assertEquals(DocumentReadPreference.PRIMARY, mongoDS.getReadPreference(0));
        assertEquals(DocumentReadPreference.PRIMARY,
                mongoDS.getReadPreference((int) (replicationLag - 100)));

        //For Integer.MAX_VALUE it should be secondary as caller intends that value is stable
        assertEquals(DocumentReadPreference.PREFER_SECONDARY,
                mongoDS.getReadPreference(Integer.MAX_VALUE));

        //For all other cases depends on age
        assertEquals(DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH,
                mongoDS.getReadPreference(-1));
        assertEquals(DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH,
                mongoDS.getReadPreference((int) (replicationLag + 100)));
    }

    @Test
    public void testMongoReadPreferencesDefault() throws Exception{
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PRIMARY));

        assertEquals(ReadPreference.primaryPreferred(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PREFER_PRIMARY));

        //By default Mongo read preference is primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PREFER_SECONDARY));

        //Change the default and assert again
        mongoDS.getDBCollection(NODES).getDB().setReadPreference(ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PREFER_SECONDARY));

        //for case where parent age cannot be determined the preference should be primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

        //For collection other than NODES always primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(SETTINGS,"foo", DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

    }

    @Test
    public void testMongoReadPreferencesWithAge() throws Exception{
        //Change the default
        ReadPreference testPref = ReadPreference.secondary();
        mongoDS.getDBCollection(NODES).getDB().setReadPreference(testPref);

        NodeStore nodeStore = mk.getNodeStore();
        NodeBuilder b1 = nodeStore.getRoot().builder();
        b1.child("x").child("y");
        nodeStore.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String id = Utils.getIdFromPath("/x/y");
        String parentId = Utils.getParentId(id);
        mongoDS.invalidateCache(NODES,id);

        //For modifiedTime < replicationLag primary must be used
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,parentId, DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));

        //Going into future to make parent /x old enough
        clock.waitUntil(Revision.getCurrentTimestamp() + replicationLag);
        mongoDS.setClock(clock);

        //For old modified nodes secondaries should be preferred
        assertEquals(testPref,
                mongoDS.getMongoReadPreference(NODES, parentId, DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH));
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
