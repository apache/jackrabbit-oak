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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * <code>DocumentMKTestBase</code> provides utility methods for DocumentMK tests.
 */
public abstract class DocumentMKTestBase {

    protected abstract DocumentMK getDocumentMK();

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

    protected void assertChildNodeCount(String path,
                                        String revision,
                                        long numChildNodes) {
        JSONObject json = parseJSONObject(getDocumentMK().getNodes(
                path, revision, 0, 0, -1, null));
        assertPropertyValue(json, ":childNodeCount", numChildNodes);
    }

    protected void assertPropExists(String rev, String path, String property) {
        String nodes = getDocumentMK().getNodes(path, rev, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyExists(obj, property);
    }

    protected void assertPropNotExists(String rev, String path, String property) {
        String nodes = getDocumentMK().getNodes(path, rev, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        if (nodes == null) {
            return;
        }
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, property);
    }

    protected void assertPropValue(String rev, String path, String property, String value) {
        String nodes = getDocumentMK().getNodes(path, rev, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
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
            boolean exists = getDocumentMK().nodeExists(path, revision);
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

    protected static Object resolveValue(JSONObject obj, String relPath) {
        String[] names = relPath.split("/");
        Object val = obj;
        for (String name : names) {
            if (!(val instanceof JSONObject)) {
                throw new AssertionError("not found: " + relPath);
            }
            val = ((JSONObject) val).get(name);
        }
        return val;
    }

    protected String addNodes(String rev, String...nodes) {
        String newRev = rev;
        for (String node : nodes) {
            newRev = getDocumentMK().commit("", "+\"" + node + "\":{}", newRev, "");
        }
        return newRev;
    }

    protected String removeNodes(String rev, String... nodes) {
        for (String node : nodes) {
            rev = getDocumentMK().commit("", "-\"" + node + "\"", rev, null);
        }
        return rev;
    }

    protected void runBackgroundUpdate(DocumentNodeStore store) {
        store.runBackgroundUpdateOperations();
    }

    protected void runBackgroundRead(DocumentNodeStore store) {
        store.runBackgroundReadOperations();
    }
}
