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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Stopwatch;
import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Tests for add node and property operations.
 */
public class DocumentMKCommitAddTest extends BaseDocumentMKTest {

    @Test
    public void addSingleNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        String nodes = mk.getNodes("/", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, ":childNodeCount", 1L);
    }

    @Test
    public void addNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\": {}, \"c\": {}, \"d\" : {} }", null, null);

        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));
    }

    @Test
    public void addNodeWithNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\": { \"c\" : { \"d\" : {} } } }", null, null);

        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/b/c/d", null));
    }

    @Test
    public void addNodeWithNestedChildren2() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        mk.commit("/a", "+\"b\" : {}", null, null);
        mk.commit("/a/b", "+\"c\" : {}", null, null);
        mk.commit("/a", "+\"d\" : {}", null, null);

        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/d", null));
    }

    @Test
    public void addNodeWithParanthesis() throws Exception {
        mk.commit("/", "+\"Test({0})\" : {}", null, null);
        String nodes = mk.getNodes("/Test({0})", null, 0, 0, -1, null);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, ":childNodeCount", 0L);
    }


    @Test
    public void addIntermediataryNodes() throws Exception {
        String rev1 = mk.commit("/", "+\"a\" : { \"b\" : { \"c\": {} }}", null, null);
        String rev2 = mk.commit("/", "+\"a/d\" : {} +\"a/b/e\" : {}", null, null);

        assertTrue(mk.nodeExists("/a/b/c", rev1));
        assertFalse(mk.nodeExists("/a/b/e", rev1));
        assertFalse(mk.nodeExists("/a/d", rev1));

        assertTrue(mk.nodeExists("/a/b/c", rev2));
        assertTrue(mk.nodeExists("/a/b/e", rev2));
        assertTrue(mk.nodeExists("/a/d", rev2));
    }

    @Test
    public void addDuplicateNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        try {
            mk.commit("/", "+\"a\" : {}", null, null);
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void setSingleProperty() throws Exception {
        mk.commit("/", "+\"a\" : {} ^\"a/key1\" : \"value1\"", null, null);

        String nodes = mk.getNodes("/", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, ":childNodeCount", 1L);
        nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
    }

    @Test
    public void setMultipleProperties() throws Exception {
        mk.commit("/", "+\"a\" : {} ^\"a/key1\" : \"value1\"", null, null);
        mk.commit("/", "^\"a/key2\" : 2", null, null);
        mk.commit("/", "^\"a/key3\" : false", null, null);
        mk.commit("/", "^\"a/key4\" : 0.25", null, null);

        String nodes = mk.getNodes("/", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, ":childNodeCount", 1L);
        nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
        assertPropertyValue(obj, "key2", 2L);
        assertPropertyValue(obj, "key3", false);
        assertPropertyValue(obj, "key4", 0.25);
    }

    // See http://www.mongodb.org/display/DOCS/Legal+Key+Names
    @Test
    public void setPropertyIllegalKey() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        mk.commit("/", "^\"a/_id\" : \"value\"", null, null);
        String nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "_id", "value");

        mk.commit("/", "^\"a/ke.y1\" : \"value\"", null, null);
        nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "ke.y1", "value");

        mk.commit("/", "^\"a/ke.y.1\" : \"value\"", null, null);
        nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "ke.y.1", "value");

        mk.commit("/", "^\"a/$key1\" : \"value\"", null, null);
        nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "$key1", "value");
    }

    @Test
    public void setPropertyWithoutAddingNode() throws Exception {
        try {
            mk.commit("/", "^\"a/key1\" : \"value1\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void setOverwritingProperty() throws Exception {
        String rev1 = mk.commit("/", "+\"a\" : {} ^\"a/key1\" : \"value1\"", null, null);

        // Commit with rev1
        String rev2 = mk.commit("/", "^\"a/key1\" : \"value2\"", rev1, null);

        // try to commit with rev1 again (to overwrite rev2)
        try {
            mk.commit("/", "^\"a/key1\" : \"value3\"", rev1, null);
            fail("commit must fail with conflicting change");
        } catch (DocumentStoreException e) {
            // expected
        }

        // now overwrite with correct base revision
        mk.commit("/", "^\"a/key1\" : \"value3\"", rev2, null);

        String nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value3");
   }

    // This is a test to make sure commit time stays the same as time goes on.
    @Test
    @Ignore("OAK-461")
    public void commitTime() throws Exception {
        boolean debug = false;
        final Stopwatch watch = Stopwatch.createUnstarted();
        for (int i = 0; i < 1000; i++) {
            watch.start();
            String diff = "+\"a"+i+"\" : {} +\"b"+i+"\" : {} +\"c"+i+"\" : {}";
            if (debug) {
                System.out.println("Committing: " + diff);
            }
            mk.commit("/", diff, null, null);
            watch.stop();
            if (debug) {
                System.out.println("Committed in " + watch.elapsed(TimeUnit.MILLISECONDS) + "ms");
            }
        }
        if (debug) {
            System.out.println("Final Result:" + watch);
        }
    }

    @Test
    public void existingNodesMerged() throws Exception {
        String rev = mk.commit("/", "+\"a\" : {}", null, null);
        mk.commit("/", "+\"a/b\" : {}", null, null);
        mk.commit("/", "^\"a/key1\" : \"value1\"", null, null);

        // Commit to rev before key1 and b were added
        mk.commit("/", "^\"a/key2\" : \"value2\"", rev, null);

        // Check that key1 and b were merged
        String nodes = mk.getNodes("/", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, ":childNodeCount", 1L);
        nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
        assertPropertyValue(obj, "key2", "value2");
    }
}