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

import java.util.HashMap;
import java.util.Map;

import com.mongodb.WriteConcern;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;

public class ReplicaSetDefaultWriteConcernIT {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetDefaultWriteConcernIT.class);

    @Rule
    public MongodProcessFactory mongodProcessFactory = new MongodProcessFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private final Map<Integer, MongodProcess> executables = new HashMap<>();

    @Before
    public void before() {
        try {
            executables.putAll(mongodProcessFactory.startReplicaSet("rs", 3));
            // New Mongo Driver seems stricter about the replica set status. We need to ensure
            // that the primary is ready (writable) before running the test.
            String uri = "mongodb://" + MongodProcessFactory.localhost(executables.keySet());
            try (MongoClient client = MongoClients.create(uri)) {
                MongoDatabase db = client.getDatabase("admin");
                boolean primaryReady = false;
                LOG.info("Waiting for primary to be ready...");
                // Use the hello command: https://www.mongodb.com/docs/v6.0/reference/command/hello/
                Document hello = db.runCommand(new BsonDocument("hello", new BsonInt32(1)));
                if (hello.getBoolean("isWritablePrimary", false)) {
                    LOG.info("Primary is ready");
                } else {
                    assumeTrue(primaryReady);
                }
            }
        } catch (Exception e) {
            assumeNoException(e);
        }
    }

    @Test
    public void majorityWriteConcern() {
        String uri = "mongodb://" + MongodProcessFactory.localhost(executables.keySet());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setMongoDB(uri, MongoUtils.DB, 0).build();
        DocumentStore store = ns.getDocumentStore();
        assertTrue(store instanceof MongoDocumentStore);
        MongoDocumentStore mds = (MongoDocumentStore) store;
        WriteConcern wc = mds.getDBCollection(Collection.NODES).getWriteConcern();
        assertEquals(WriteConcern.MAJORITY, wc);
    }
}
