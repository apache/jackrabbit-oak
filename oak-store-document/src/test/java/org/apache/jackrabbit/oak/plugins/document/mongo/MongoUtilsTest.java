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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type.GENERIC;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type.TRANSIENT;
import static org.apache.jackrabbit.oak.plugins.document.MongoUtils.isAvailable;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoUtils.getDocumentStoreExceptionTypeFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class MongoUtilsTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @BeforeClass
    public static void mongoAvailable() {
        assumeTrue(isAvailable());
    }

    @Test
    public void createIndex() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        c.getDatabase().drop();
        MongoCollection collection = c.getDatabase().getCollection("test");
        MongoUtils.createIndex(collection, "foo", true, false, true);
        MongoUtils.createIndex(collection, "bar", false, true, false);
        MongoUtils.createIndex(collection, new String[]{"baz", "qux"},
                new boolean[]{true, false}, false, false);

        assertTrue(MongoUtils.hasIndex(collection, "_id"));
        assertTrue(MongoUtils.hasIndex(collection, "foo"));
        assertFalse(MongoUtils.hasIndex(collection, "foo", "bar"));
        assertTrue(MongoUtils.hasIndex(collection, "baz", "qux"));

        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().into(indexes);
        assertEquals(4, indexes.size());
        for (Document info : indexes) {
            Document key = (Document) info.get("key");
            if (key.keySet().contains("foo")) {
                assertEquals(1, key.keySet().size());
                assertEquals(1, key.get("foo"));
                assertEquals(Boolean.TRUE, info.get("sparse"));
            } else if (key.keySet().contains("bar")) {
                assertEquals(1, key.keySet().size());
                assertEquals(-1, key.get("bar"));
                assertEquals(Boolean.TRUE, info.get("unique"));
            } else if (key.keySet().contains("baz")) {
                assertEquals(2, key.keySet().size());
                assertEquals(1, key.get("baz"));
                assertEquals(-1, key.get("qux"));
            }
        }

        c.getDatabase().drop();
    }

    @Test
    public void createPartialIndex() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        c.getDatabase().drop();
        MongoStatus status = new MongoStatus(c.getMongoClient(), c.getDBName());
        assumeTrue(status.isVersion(3, 2));

        MongoCollection collection = c.getDatabase().getCollection("test");

        MongoUtils.createPartialIndex(collection, new String[]{"foo", "bar"},
                new boolean[]{true, true}, "{foo:true}");
        assertTrue(MongoUtils.hasIndex(collection, "_id"));
        assertTrue(MongoUtils.hasIndex(collection, "foo", "bar"));

        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().into(indexes);
        assertEquals(2, indexes.size());
        for (Document info : indexes) {
            Document key = (Document) info.get("key");
            assertNotNull(key);
            if (key.keySet().contains("foo")) {
                assertEquals(2, key.keySet().size());
                assertEquals(1, key.get("foo"));
                assertEquals(1, key.get("bar"));
                Document filter = (Document) info.get("partialFilterExpression");
                assertNotNull(filter);
                assertEquals(Boolean.TRUE, filter.get("foo"));
            }
        }

        c.getDatabase().drop();
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkArguments() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoCollection collection = c.getDatabase().getCollection("test");
        MongoUtils.createIndex(collection, new String[]{"foo", "bar"},
                new boolean[]{true}, false, true);
    }

    @Test
    public void documentStoreExceptionType() {
        assertEquals(GENERIC, getDocumentStoreExceptionTypeFor(new IOException()));
        assertEquals(GENERIC, getDocumentStoreExceptionTypeFor(new MongoException("message")));
        assertEquals(GENERIC, getDocumentStoreExceptionTypeFor(newMongoCommandException(42)));
        assertEquals(GENERIC, getDocumentStoreExceptionTypeFor(new DuplicateKeyException(response(11000), new ServerAddress(), null)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(newWriteConcernException(11600)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(newWriteConcernException(11601)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(newWriteConcernException(11602)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(newMongoCommandException(11600)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(newMongoCommandException(11601)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(newMongoCommandException(11602)));
        assertEquals(TRANSIENT, getDocumentStoreExceptionTypeFor(new MongoSocketException("message", new ServerAddress())));
    }

    @Test
    public void isCollectionEmpty() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        c.getDatabase().drop();

        String collectionName = Collection.NODES.toString();
        MongoStatus status = new MongoStatus(c.getMongoClient(), c.getDBName());

        // consider empty when collection doesn't exist
        MongoCollection<BasicDBObject> nodes = c.getDatabase()
                .getCollection(collectionName, BasicDBObject.class);
        assertTrue(MongoUtils.isCollectionEmpty(nodes, null));
        if (status.isClientSessionSupported()) {
            try (ClientSession s = c.getMongoClient().startSession()) {
                assertTrue(MongoUtils.isCollectionEmpty(nodes, s));
            }
        }

        // insert a document
        nodes.insertOne(new BasicDBObject("p", "v"));

        // check again
        assertFalse(MongoUtils.isCollectionEmpty(nodes, null));
        if (status.isClientSessionSupported()) {
            try (ClientSession s = c.getMongoClient().startSession()) {
                assertFalse(MongoUtils.isCollectionEmpty(nodes, s));
            }
        }

        // remove any document
        nodes.deleteMany(new BasicDBObject());

        // must be empty again
        assertTrue(MongoUtils.isCollectionEmpty(nodes, null));
        if (status.isClientSessionSupported()) {
            try (ClientSession s = c.getMongoClient().startSession()) {
                assertTrue(MongoUtils.isCollectionEmpty(nodes, s));
            }
        }
    }

    private static MongoCommandException newMongoCommandException(int code) {
        return new MongoCommandException(response(code), new ServerAddress());
    }

    private static WriteConcernException newWriteConcernException(int code) {
        return new WriteConcernException(response(code), new ServerAddress(), null);
    }

    private static BsonDocument response(int code) {
        BsonDocument response = new BsonDocument();
        response.put("code", new BsonInt32(code));
        response.put("errmsg", new BsonString("message"));
        return response;
    }
}
