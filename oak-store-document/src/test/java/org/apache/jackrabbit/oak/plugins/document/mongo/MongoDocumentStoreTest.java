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

import java.util.Map;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.JournalEntry;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.hasIndex;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * <code>MongoDocumentStoreTest</code>...
 */
public class MongoDocumentStoreTest extends AbstractMongoConnectionTest {

    private TestStore store;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        DocumentMK.Builder builder = new DocumentMK.Builder();
        store = new TestStore(mongoConnection.getDB(), builder);
        builder.setDocumentStore(store);
        mk = builder.setMongoDB(mongoConnection.getDB()).open();
    }

    @Test
    public void defaultIndexes() {
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), Document.ID));
        assertFalse(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.SD_TYPE));
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.SD_TYPE, NodeDocument.SD_MAX_REV_TIME_IN_SECS));
        if (new MongoStatus(mongoConnection.getDB()).isVersion(3, 2)) {
            assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE, NodeDocument.MODIFIED_IN_SECS));
        } else {
            assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE));
        }
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.HAS_BINARY_FLAG));
        assertTrue(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.MODIFIED_IN_SECS, Document.ID));
        assertFalse(hasIndex(store.getDBCollection(Collection.NODES), NodeDocument.MODIFIED_IN_SECS));
        assertTrue(hasIndex(store.getDBCollection(Collection.JOURNAL), JournalEntry.MODIFIED));
    }

    @Test
    public void oak6423() throws Exception {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        DocumentMK.Builder builder = new DocumentMK.Builder();
        TestStore s = new TestStore(c.getDB(), builder);
        if (new MongoStatus(mongoConnection.getDB()).isVersion(3, 2)) {
            assertFalse(hasIndex(s.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE));
        } else {
            assertFalse(hasIndex(s.getDBCollection(Collection.NODES), NodeDocument.DELETED_ONCE, NodeDocument.MODIFIED_IN_SECS));
        }
    }

    @Test
    public void getStats() throws Exception {
        Map<String, String> info = mk.getNodeStore().getDocumentStore().getStats();
        assertThat(info.keySet(), hasItem("nodes.count"));
        assertThat(info.keySet(), hasItem("clusterNodes.count"));
        assertThat(info.keySet(), hasItem("journal.count"));
        assertThat(info.keySet(), hasItem("settings.count"));
    }

    static final class TestStore extends MongoDocumentStore {
        TestStore(DB db, DocumentMK.Builder builder) {
            super(db, builder);
        }
    }
}
