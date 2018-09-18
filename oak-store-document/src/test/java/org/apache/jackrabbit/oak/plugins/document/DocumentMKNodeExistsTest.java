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

import org.apache.jackrabbit.oak.plugins.document.impl.SimpleNodeScenario;
import org.junit.Test;

/**
 * Tests for nodeExists.
 */
public class DocumentMKNodeExistsTest extends BaseDocumentMKTest {

    @Test
    public void simple() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        String revisionId = scenario.create();

        boolean exists = mk.nodeExists("/a", revisionId);
        assertTrue(exists);

        exists = mk.nodeExists("/a/b", revisionId);
        assertTrue(exists);

        revisionId = scenario.deleteA();

        exists = mk.nodeExists("/a", revisionId);
        assertFalse(exists);

        exists = mk.nodeExists("/a/b", revisionId);
        assertFalse(exists);
    }

    @Test
    public void withoutRevisionId() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        scenario.create();

        boolean exists = mk.nodeExists("/a", null);
        assertTrue(exists);

        scenario.deleteA();

        exists = mk.nodeExists("/a", null);
        assertFalse(exists);
    }

    @Test
    public void withInvalidRevisionId() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        scenario.create();

        try {
            mk.nodeExists("/a", "123456789");
            fail("Expected: Invalid revision id exception");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void parentDelete() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        scenario.create();

        boolean exists = mk.nodeExists("/a/b", null);
        assertTrue(exists);

        scenario.deleteA();
        exists = mk.nodeExists("/a/b", null);
        assertFalse(exists);
    }

    @Test
    public void grandParentDelete() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }", null,
                "Add /a/b/c/d");

        mk.commit("/a", "-\"b\"", null, "Remove /b");

        boolean exists = mk.nodeExists("/a/b/c/d", null);
        assertFalse(exists);
    }

    @Test
    public void existsInHeadRevision() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, "Add /a");
        mk.commit("/a", "+\"b\" : {}", null, "Add /a/b");

        boolean exists = mk.nodeExists("/a", null);
        assertTrue("The node a is not found in the head revision!", exists);
    }

    @Test
    public void existsInOldRevNotInNewRev() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        String rev1 = scenario.create();
        String rev2 = scenario.deleteA();

        boolean exists = mk.nodeExists("/a", rev1);
        assertTrue(exists);

        exists = mk.nodeExists("/a", rev2);
        assertFalse(exists);
    }

    @Test
    public void siblingDelete() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        scenario.create();

        scenario.deleteB();
        boolean exists = mk.nodeExists("/a/b", null);
        assertFalse(exists);

        exists = mk.nodeExists("/a/c", null);
        assertTrue(exists);
    }
}