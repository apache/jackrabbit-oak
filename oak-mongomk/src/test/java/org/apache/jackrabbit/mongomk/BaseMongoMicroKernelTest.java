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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.BlobStoreMongo;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.NodeStoreMongo;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;

/**
 * Base class for {@code MongoDB} tests that need the MongoMK.
 */
public class BaseMongoMicroKernelTest extends BaseMongoTest {

    public static MicroKernel mk;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
        BlobStore blobStore = new BlobStoreMongo(mongoConnection);
        mk = new MongoMicroKernel(nodeStore, blobStore);
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