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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.WriteConcern;

import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoConnectionTest {

    @Test
    public void hasWriteConcern() throws Exception {
        assertFalse(MongoConnection.hasWriteConcern("mongodb://localhost:27017/foo"));
        assertTrue(MongoConnection.hasWriteConcern("mongodb://localhost:27017/foo?w=1"));
    }

    @Test
    public void hasReadConcern() throws Exception {
        assertFalse(MongoConnection.hasReadConcern("mongodb://localhost:27017/foo"));
        assertTrue(MongoConnection.hasReadConcern("mongodb://localhost:27017/foo?readconcernlevel=majority"));
    }

    @Test
    public void sufficientWriteConcern() throws Exception {
        sufficientWriteConcernReplicaSet(WriteConcern.ACKNOWLEDGED, false);
        sufficientWriteConcernReplicaSet(WriteConcern.JOURNALED, false);
        sufficientWriteConcernReplicaSet(WriteConcern.MAJORITY, true);
        sufficientWriteConcernReplicaSet(WriteConcern.FSYNC_SAFE, false);
        sufficientWriteConcernReplicaSet(WriteConcern.FSYNCED, false);
        sufficientWriteConcernReplicaSet(WriteConcern.JOURNAL_SAFE, false);
        sufficientWriteConcernReplicaSet(WriteConcern.NORMAL, false);
        sufficientWriteConcernReplicaSet(WriteConcern.REPLICA_ACKNOWLEDGED, true);
        sufficientWriteConcernReplicaSet(WriteConcern.REPLICAS_SAFE, true);
        sufficientWriteConcernReplicaSet(WriteConcern.SAFE, false);
        sufficientWriteConcernReplicaSet(WriteConcern.UNACKNOWLEDGED, false);

        sufficientWriteConcernSingleNode(WriteConcern.ACKNOWLEDGED, true);
        sufficientWriteConcernSingleNode(WriteConcern.JOURNALED, true);
        sufficientWriteConcernSingleNode(WriteConcern.MAJORITY, true);
        sufficientWriteConcernSingleNode(WriteConcern.FSYNC_SAFE, true);
        sufficientWriteConcernSingleNode(WriteConcern.FSYNCED, true);
        sufficientWriteConcernSingleNode(WriteConcern.JOURNAL_SAFE, true);
        sufficientWriteConcernSingleNode(WriteConcern.NORMAL, false);
        sufficientWriteConcernSingleNode(WriteConcern.REPLICA_ACKNOWLEDGED, true);
        sufficientWriteConcernSingleNode(WriteConcern.REPLICAS_SAFE, true);
        sufficientWriteConcernSingleNode(WriteConcern.SAFE, true);
        sufficientWriteConcernSingleNode(WriteConcern.UNACKNOWLEDGED, false);
    }

    @Test
    public void sufficientReadConcern() throws Exception {
        sufficientReadConcernReplicaSet(ReadConcern.DEFAULT, false);
        sufficientReadConcernReplicaSet(ReadConcern.LOCAL, false);
        sufficientReadConcernReplicaSet(ReadConcern.MAJORITY, true);

        sufficientReadConcernSingleNode(ReadConcern.DEFAULT, true);
        sufficientReadConcernSingleNode(ReadConcern.LOCAL, true);
        sufficientReadConcernSingleNode(ReadConcern.MAJORITY, true);
    }

    @Test
    public void socketKeepAlive() throws Exception {
        assumeTrue(MongoUtils.isAvailable());
        MongoClientOptions.Builder options = MongoConnection.getDefaultBuilder();
        options.socketKeepAlive(true);
        MongoConnection c = new MongoConnection(MongoUtils.URL, options);
        try {
            assertTrue(c.getDB().getMongo().getMongoOptions().isSocketKeepAlive());
        } finally {
            c.close();
        }
        // default is without keep-alive
        c = new MongoConnection(MongoUtils.URL);
        try {
            assertFalse(c.getDB().getMongo().getMongoOptions().isSocketKeepAlive());
        } finally {
            c.close();
        }
    }

    private void sufficientWriteConcernReplicaSet(WriteConcern w,
                                                  boolean sufficient) {
        sufficientWriteConcern(w, true, sufficient);
    }

    private void sufficientWriteConcernSingleNode(WriteConcern w,
                                                      boolean sufficient) {
        sufficientWriteConcern(w, false, sufficient);
    }

    private void sufficientWriteConcern(WriteConcern w,
                                        boolean replicaSet,
                                        boolean sufficient) {
        DB db = mockDB(ReadConcern.DEFAULT, w, replicaSet);
        assertEquals(sufficient, MongoConnection.hasSufficientWriteConcern(db));
    }

    private void sufficientReadConcernReplicaSet(ReadConcern r,
                                                 boolean sufficient) {
        sufficientReadConcern(r, true, sufficient);
    }

    private void sufficientReadConcernSingleNode(ReadConcern r,
                                                 boolean sufficient) {
        sufficientReadConcern(r, false, sufficient);
    }
    private void sufficientReadConcern(ReadConcern r,
                                       boolean replicaSet,
                                       boolean sufficient) {
        DB db = mockDB(r, replicaSet ? WriteConcern.MAJORITY : WriteConcern.W1, replicaSet);
        assertEquals(sufficient, MongoConnection.hasSufficientReadConcern(db));
    }

    private DB mockDB(ReadConcern r,
                      WriteConcern w,
                      boolean replicaSet) {
        ReplicaSetStatus status;
        if (replicaSet) {
            status = mock(ReplicaSetStatus.class);
        } else {
            status = null;
        }
        DB db = mock(DB.class);
        Mongo mongo = mock(Mongo.class);
        when(db.getMongo()).thenReturn(mongo);
        when(db.getWriteConcern()).thenReturn(w);
        when(db.getReadConcern()).thenReturn(r);
        when(mongo.getReplicaSetStatus()).thenReturn(status);
        return db;
    }
}
