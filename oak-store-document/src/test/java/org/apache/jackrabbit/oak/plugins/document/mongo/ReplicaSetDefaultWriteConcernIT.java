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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.WriteConcern;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.LeaseCheckMode;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

public class ReplicaSetDefaultWriteConcernIT {

    @Rule
    public MongodProcessFactory mongodProcessFactory = new MongodProcessFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Map<Integer, MongodProcess> executables = new HashMap<>();

    @Before
    public void before() {
        try {
            executables.putAll(mongodProcessFactory.startReplicaSet("rs", 3));
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
