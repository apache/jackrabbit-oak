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
package org.apache.jackrabbit.mongomk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoGridFSBlobStore;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;

import com.mongodb.DB;

/**
 * Base class for {@code MongoDB} tests that need the MongoMK.
 */
public class BaseMongoMicroKernelTest extends AbstractMongoConnectionTest {

    public static MicroKernel mk;

    @Before
    public void setUp() throws Exception {
        DB db = mongoConnection.getDB();
        MongoNodeStore nodeStore = new MongoNodeStore(db);
        MongoAssert.setNodeStore(nodeStore);
        BlobStore blobStore = new MongoGridFSBlobStore(db);
        mk = new MongoMicroKernel(mongoConnection, nodeStore, blobStore);
    }

    protected MongoNodeStore getNodeStore() {
        MongoMicroKernel mongoMk = (MongoMicroKernel)mk;
        return (MongoNodeStore)mongoMk.getNodeStore();
    }

    protected JSONObject getObjectArrayEntry(JSONArray array, int pos) {
        assertTrue(pos >= 0 && pos < array.size());
        Object entry = array.get(pos);
        if (entry instanceof JSONObject) {
            return (JSONObject) entry;
        }
        throw new AssertionError("failed to resolve JSONObject array entry at pos " + pos + ": " + entry);
    }

    protected JSONArray parseJSONArray(String json) throws AssertionError {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(json);
            assertTrue(obj instanceof JSONArray);
            return (JSONArray) obj;
        } catch (Exception e) {
            throw new AssertionError("not a valid JSON array: " + e.getMessage());
        }
    }

    protected JSONObject parseJSONObject(String json) throws AssertionError {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(json);
            assertTrue(obj instanceof JSONObject);
            return (JSONObject) obj;
        } catch (Exception e) {
            throw new AssertionError("not a valid JSON object: " + e.getMessage());
        }
    }

    protected void assertNodesExist(String revision, String...paths) {
        doAssertNodes(true, revision, paths);
    }

    protected void assertNodesNotExist(String revision, String...paths) {
        doAssertNodes(false, revision, paths);
    }

    protected void assertPropExists(String rev, String path, String property) {
        String nodes = mk.getNodes(path, rev, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyExists(obj, property);
    }

    protected void assertPropNotExists(String rev, String path, String property) {
        String nodes = mk.getNodes(path, rev, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        if (nodes == null) {
            return;
        }
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, property);
    }

    protected void assertPropValue(String rev, String path, String property, String value) {
        String nodes = mk.getNodes(path, rev, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, property, value);
    }

    protected void assertPropertyExists(JSONObject obj, String relPath, Class<?> type)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertTrue(type.isInstance(val));
    }

    protected void assertPropertyExists(JSONObject obj, String relPath)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);
    }

    protected void assertPropertyNotExists(JSONObject obj, String relPath)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNull(val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, String expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);
        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Double expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Long expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);
        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Boolean expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertEquals(expected, val);
    }

    private void doAssertNodes(boolean checkExists, String revision, String...paths) {
        for (String path : paths) {
            boolean exists = mk.nodeExists(path, revision);
            if (checkExists) {
                assertTrue(path + " does not exist", exists);
            } else {
                assertFalse(path + " should not exist", exists);
            }
        }
    }

    protected JSONObject resolveObjectValue(JSONObject obj, String relPath) {
        Object val = resolveValue(obj, relPath);
        if (val instanceof JSONObject) {
            return (JSONObject) val;
        }
        throw new AssertionError("failed to resolve JSONObject value at " + relPath + ": " + val);
    }

    private Object resolveValue(JSONObject obj, String relPath) {
        String names[] = relPath.split("/");
        Object val = obj;
        for (String name : names) {
            if (! (val instanceof JSONObject)) {
                throw new AssertionError("not found: " + relPath);
            }
            val = ((JSONObject) val).get(name);
        }
        return val;
    }

}