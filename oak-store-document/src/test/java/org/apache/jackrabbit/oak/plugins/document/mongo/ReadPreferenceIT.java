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
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore.DocumentReadPreference;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceIT extends AbstractMongoConnectionTest {

    private MongoDocumentStore mongoDS;

    private DocumentMK mk2;

    private Clock clock;

    @Override
    public void setUpConnection() {
        clock = new Clock.Virtual();
        setRevisionClock(clock);
        setClusterNodeInfoClock(clock);
        mongoConnection = connectionFactory.getConnection();
        assertNotNull(mongoConnection);
        MongoUtils.dropCollections(mongoConnection.getDBName());
        mk = new DocumentMK.Builder()
                .clock(clock)
                .setClusterId(1)
                .setClientSessionDisabled(true)
                .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName())
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .open();
        mongoDS = (MongoDocumentStore) mk.getDocumentStore();

        // use a separate connection for cluster node 2
        MongoConnection mongoConnection2 = connectionFactory.getConnection();
        mk2 = new DocumentMK.Builder()
                .clock(clock)
                .setClusterId(2)
                .setClientSessionDisabled(true)
                .setMongoDB(mongoConnection2.getMongoClient(), mongoConnection2.getDBName())
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .open();
    }

    @After
    public void tearDown() {
        // reset readWrite mode before shutting down
        mongoDS.setReadWriteMode("");
        mk2.dispose();
    }

    @Test
    public void testMongoReadPreferencesDefault() {
        // start with read preference set to primary
        mongoDS.setReadWriteMode(rwMode(ReadPreference.primary()));

        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PRIMARY));

        assertEquals(ReadPreference.primaryPreferred(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PREFER_PRIMARY));

        //By default Mongo read preference is primary
        assertEquals(ReadPreference.primary(),
                mongoDS.getMongoReadPreference(NODES,"foo", DocumentReadPreference.PREFER_SECONDARY));

        //Change the default and assert again
        mongoDS.setReadWriteMode(rwMode(ReadPreference.secondary()));
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
    public void testReadWriteMode() {
        mongoDS.setReadWriteMode(rwMode(ReadPreference.primary()));
        assertEquals(ReadPreference.primary(), mongoDS.getConfiguredReadPreference(NODES));

        mongoDS.setReadWriteMode("readPreference=secondary&w=2&safe=true&j=true");

        assertEquals(ReadPreference.secondary(), mongoDS.getDBCollection(NODES).getReadPreference());
        assertEquals(2, mongoDS.getDBCollection(NODES).getWriteConcern().getW());
        Boolean journal = mongoDS.getDBCollection(NODES).getWriteConcern().getJournal();
        assertNotNull(journal);
        assertTrue(journal);

        assertEquals(ReadPreference.secondary(), mongoDS.getConfiguredReadPreference(NODES));
    }

    @Test
    public void getMongoReadPreference() {
        String id = getIdFromPath("/does/not/exist");
        mongoDS.setReadWriteMode(rwMode(ReadPreference.secondaryPreferred()));
        mongoDS.find(NODES, id);
        ReadPreference readPref = mongoDS.getMongoReadPreference(NODES, id,
                DocumentReadPreference.PREFER_SECONDARY_IF_OLD_ENOUGH);
        assertEquals(ReadPreference.primary(), readPref);
    }

    private static String rwMode(ReadPreference preference) {
        return "readpreference=" + preference.getName();
    }
}
