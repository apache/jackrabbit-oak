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

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests with emphasis on remove node and property operations.
 */
public class DocumentMKCommitRemoveTest extends BaseDocumentMKTest {

    @Test
    public void removeSingleNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        assertNodesExist(null, "/a");

        mk.commit("/", "-\"a\"", null, null);
        assertNodesNotExist(null, "/a");
    }

    @Test
    public void removeNonExistentNode() throws Exception {
        try {
            mk.commit("/", "-\"a\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    @Ignore
    public void removeNodeTwice() throws Exception {
        String base = mk.commit("", "+\"/a\":{}", null, null);
        mk.commit("", "-\"/a\"", base, null);
        assertTrue(mk.nodeExists("/a", base));
        mk.commit("", "-\"/a\"", base, null);
    }

    @Test
    public void removeAndAddNode() throws Exception {
        String base = mk.commit("", "+\"/a\":{}", null, null);
        String rev = mk.commit("", "-\"/a\"", base, null);
        assertTrue(mk.nodeExists("/a", base));
        assertFalse(mk.nodeExists("/a", rev));
        mk.commit("", "+\"/a\":{}", rev, null);
    }

    @Test
    public void removeNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {},  \"c\" : {}, \"d\" : {}}", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));

        mk.commit("/", "-\"a/b\"", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));
    }

    @Test
    public void removeNodeWithNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/b/c/d", null));

        mk.commit("/", "-\"a\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/a/b/c", null));
        assertFalse(mk.nodeExists("/a/b/c/d", null));
    }
}