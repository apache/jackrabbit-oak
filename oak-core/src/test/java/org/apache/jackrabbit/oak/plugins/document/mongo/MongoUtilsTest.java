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

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.MongoUtils.isAvailable;
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
        c.getDB().dropDatabase();
        DBCollection collection = c.getDB().getCollection("test");
        MongoUtils.createIndex(collection, "foo", true, false, true);
        MongoUtils.createIndex(collection, "bar", false, true, false);
        MongoUtils.createIndex(collection, new String[]{"baz", "qux"},
                new boolean[]{true, false}, false, false);

        assertTrue(MongoUtils.hasIndex(collection, "_id"));
        assertTrue(MongoUtils.hasIndex(collection, "foo"));
        assertFalse(MongoUtils.hasIndex(collection, "foo", "bar"));
        assertTrue(MongoUtils.hasIndex(collection, "baz", "qux"));

        assertEquals(4, collection.getIndexInfo().size());
        for (DBObject info : collection.getIndexInfo()) {
            DBObject key = (DBObject) info.get("key");
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

        c.getDB().dropDatabase();
    }

    @Test
    public void createPartialIndex() {
        MongoConnection c = connectionFactory.getConnection();
        c.getDB().dropDatabase();
        MongoStatus status = new MongoStatus(c.getDB());
        assumeTrue(status.isVersion(3, 2));

        DBCollection collection = c.getDB().getCollection("test");

        MongoUtils.createPartialIndex(collection, new String[]{"foo", "bar"},
                new boolean[]{true, true}, "{foo:true}");
        assertTrue(MongoUtils.hasIndex(collection, "_id"));
        assertTrue(MongoUtils.hasIndex(collection, "foo", "bar"));

        assertEquals(2, collection.getIndexInfo().size());
        for (DBObject info : collection.getIndexInfo()) {
            DBObject key = (DBObject) info.get("key");
            assertNotNull(key);
            if (key.keySet().contains("foo")) {
                assertEquals(2, key.keySet().size());
                assertEquals(1, key.get("foo"));
                assertEquals(1, key.get("bar"));
                DBObject filter = (DBObject) info.get("partialFilterExpression");
                assertNotNull(filter);
                assertEquals(Boolean.TRUE, filter.get("foo"));
            }
        }

        c.getDB().dropDatabase();
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkArguments() {
        MongoConnection c = connectionFactory.getConnection();
        DBCollection collection = c.getDB().getCollection("test");
        MongoUtils.createIndex(collection, new String[]{"foo", "bar"},
                new boolean[]{true}, false, true);
    }
}
