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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for commit with emphasis on move operations.
 */
@Ignore
public class DocumentMKCommitMoveTest extends BaseDocumentMKTest {

    @Test
    public void moveNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        assertTrue(mk.nodeExists("/a", null));

        mk.commit("/", ">\"a\" : \"b\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/b", null));
    }

    @Test
    public void moveUnderSourcePath() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {} }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));

        try {
            mk.commit("/", ">\"b\" : \"a\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void moveNodeWithChild() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {} }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));

        mk.commit("/", ">\"a\" : \"c\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
        assertTrue(mk.nodeExists("/c/b", null));
    }

    @Test
    public void moveNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {},  \"c\" : {}, \"d\" : {}}", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));

        mk.commit("/", ">\"a\" : \"e\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/a/c", null));
        assertFalse(mk.nodeExists("/a/d", null));
        assertTrue(mk.nodeExists("/e", null));
        assertTrue(mk.nodeExists("/e/b", null));
        assertTrue(mk.nodeExists("/e/c", null));
        assertTrue(mk.nodeExists("/e/d", null));
    }

    @Test
    public void moveNodeWithNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/b/c/d", null));

        mk.commit("/", ">\"a\" : \"e\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/a/b/c", null));
        assertFalse(mk.nodeExists("/a/b/c/d", null));
        assertTrue(mk.nodeExists("/e", null));
        assertTrue(mk.nodeExists("/e/b", null));
        assertTrue(mk.nodeExists("/e/b/c", null));
        assertTrue(mk.nodeExists("/e/b/c/d", null));

        mk.commit("/", ">\"e/b\" : \"f\"", null, null);
        assertTrue(mk.nodeExists("/e", null));
        assertFalse(mk.nodeExists("/e/b", null));
        assertFalse(mk.nodeExists("/e/b/c", null));
        assertFalse(mk.nodeExists("/e/b/c/d", null));
        assertTrue(mk.nodeExists("/f", null));
        assertTrue(mk.nodeExists("/f/c", null));
        assertTrue(mk.nodeExists("/f/c/d", null));
    }

    @Test
    public void moveNodeWithProperties() throws Exception {
        mk.commit("/", "+\"a\" : { \"key1\" : \"value1\" }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        String nodes = mk.getNodes("/a", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");

        mk.commit("/", ">\"a\" : \"c\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/c", null));
        nodes = mk.getNodes("/c", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
    }

    @Test
    public void moveFromNonExistentNode() throws Exception {
        try {
            mk.commit("/", ">\"b\" : \"c\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void moveToAnExistentNode() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {} }", null, null);
        mk.commit("/", "+\"c\" : {}", null, null);

        try {
            mk.commit("/", ">\"c\" : \"a/b\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void addNodeAndMove() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"a/b\": {}\n"
                     + ">\"a/b\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/c", null));
    }

    @Test
    public void addNodeAndMove2() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"a/b\": {}\n", null, null);
        mk.commit("/", ">\"a/b\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/c", null));
    }

    @Test
    public void addNodeWithChildrenAndMove() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"a/b\":{ \"c\" : {}, \"d\" : {} }\n"
                     + ">\"a/b\":\"e\"", null, null);

        assertTrue(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/a/b/c", null));
        assertFalse(mk.nodeExists("/a/b/d", null));

        assertTrue(mk.nodeExists("/e", null));
        assertTrue(mk.nodeExists("/e/c", null));
        assertTrue(mk.nodeExists("/e/d", null));
    }

    @Test
    public void addNodeWithNestedChildrenAndMove() {
        mk.commit("/", "+\"a\":{ \"b\" : { \"c\" : { } } }", null, null);
        mk.commit("/", "+\"a/b/c/d\":{}\n"
                     + ">\"a\":\"e\"", null, null);

        assertFalse(mk.nodeExists("/a/b/c/d", null));
        assertTrue(mk.nodeExists("/e/b/c/d", null));
    }

    @Test
    public void addNodeAndMoveParent() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"a/b\":{}\n" +
                        ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
        assertTrue(mk.nodeExists("/c/b", null));
    }

    @Test
    public void removeNodeAndMove() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);

        try {
            mk.commit("/", "-\"a/b\"\n"
                         + ">\"a/b\":\"c\"", null, null);
            fail("Expected expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void removeNodeWithNestedChildrenAndMove() {
        mk.commit("/", "+\"a\":{ \"b\" : { \"c\" : { \"d\" : {} } } }", null, null);
        mk.commit("/", "-\"a/b/c/d\"\n"
                     + ">\"a\" : \"e\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/e/b/c", null));
        assertFalse(mk.nodeExists("/e/b/c/d", null));
    }

    @Test
    public void removeNodeAndMoveParent() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);
        mk.commit("/", "-\"a/b\"\n"
                     + ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
        assertFalse(mk.nodeExists("/c/b", null));
    }

    @Test
    public void setPropertyAndMove() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "^\"a/key1\": \"value1\"\n" +
                        ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/c", null));

        String nodes = mk.getNodes("/c", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
    }

    @Test
    public void setNestedPropertyAndMove() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);
        mk.commit("/", "^\"a/b/key1\": \"value1\"\n" +
                        ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
        assertTrue(mk.nodeExists("/c/b", null));

        String nodes = mk.getNodes("/c/b", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
    }

    @Test
    public void modifyParentAddPropertyAndMove() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"b\" : {}\n"
                     + "^\"a/key1\": \"value1\"\n"
                     + ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/b", null));
        assertTrue(mk.nodeExists("/c", null));

        String nodes = mk.getNodes("/c", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "key1", "value1");
    }

    @Test
    public void removePropertyAndMove() {
        mk.commit("/", "+\"a\":{ \"b\" : { \"key1\" : \"value1\" } }", null, null);
        mk.commit("/", "^\"a/b/key1\": null\n"
                     + ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
        assertTrue(mk.nodeExists("/c/b", null));

        String nodes = mk.getNodes("/c/b", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "key1");
    }

    @Test
    public void removeNestedPropertyAndMove() {
        mk.commit("/", "+\"a\":{ \"key1\" : \"value1\"}", null, null);
        mk.commit("/", "^\"a/key1\" : null\n"
                     + ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/c", null));

        String nodes = mk.getNodes("/c", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "key1");
    }

    @Test
    public void modifyParentRemovePropertyAndMove() {
        mk.commit("/", "+\"a\":{ \"key1\" : \"value1\"}", null, null);
        mk.commit("/", "+\"b\" : {}\n"
                     + "^\"a/key1\" : null\n"
                     + ">\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/b", null));
        assertTrue(mk.nodeExists("/c", null));

        String nodes = mk.getNodes("/c", null, 0 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "key1");
    }

    @Test
    public void moveAndMoveBack() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", ">\"a\":\"x\">\"x\":\"a\"", null, null);
        assertNodesExist(null, "/a");
    }

    @Test
    public void moveAndMoveBackWithChildren() {
        mk.commit("/", "+\"a\":{\"b\":{}}", null, null);
        mk.commit("/", ">\"a\":\"x\">\"x\":\"a\"", null, null);
        assertNodesExist(null, "/a", "/a/b");
    }

    @Test
    public void moveAndMoveBackWithAddedChildren() {
        mk.commit("/", "+\"a\":{\"b\":{}}", null, null);
        mk.commit("/", ">\"a\":\"x\"+\"x/c\":{}>\"x\":\"a\"", null, null);
        assertNodesExist(null, "/a", "/a/b", "/a/c");
    }

    @Test
    public void moveAndMoveBackWithSetProperties() {
        mk.commit("/", "+\"a\":{\"b\":{}}", null, null);
        mk.commit("/", ">\"a\":\"x\"^\"x/p\":1>\"x\":\"a\"", null, null);
        assertNodesExist(null, "/a", "/a/b");
        assertPropExists(null, "/a", "p");
    }
}