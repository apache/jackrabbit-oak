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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

@RunWith(Parameterized.class)
public abstract class AbstractDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDocumentStoreTest.class);

    protected DocumentStore ds;

    public AbstractDocumentStoreTest(DocumentStore ds) {
        this.ds = ds;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        Collection<Object[]> result = new ArrayList<Object[]>();
        result.add(new Object[] { new MemoryDocumentStore() });
        result.add(new Object[] { new RDBDocumentStore(new DocumentMK.Builder()) });
        DocumentStore md = getMongoDS();
        if (md != null) {
            result.add(new Object[] { md });
        }
        return result;
    }

    private static DocumentStore getMongoDS() {
        String instance = "mongodb://localhost:27017/oak";
        try {
            MongoConnection connection = new MongoConnection(instance);
            DB mongoDB = connection.getDB();
            return new MongoDocumentStore(mongoDB, new DocumentMK.Builder());
        } catch (Exception e) {
            LOG.info("Mongo instance not available at " + instance + ", skipping tests...");
            return null;
        }
    }
}
