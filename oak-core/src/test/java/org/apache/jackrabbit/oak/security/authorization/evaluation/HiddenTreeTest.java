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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to make sure hidden trees are never exposed.
 */
public class HiddenTreeTest extends AbstractOakCoreTest {
    private static final String HIDDEN_NAME = ":index";

    private Tree parent;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        String hiddenParentPath = "/oak:index/nodetype";
        parent = root.getTree(hiddenParentPath);
        assertTrue(parent.exists());
    }

    @Test
    public void testHasHiddenTree() {
        assertFalse(parent.hasChild(HIDDEN_NAME));
    }

    @Test
    public void testGetHiddenTree() {
        Tree hidden = parent.getChild(HIDDEN_NAME);
        assertNotNull(hidden);
        assertFalse(hidden.exists());
        assertEquals(0, hidden.getChildrenCount(1));
    }

    @Test
    public void testOrderBeforeOnHiddenTree() {
        try {
            Tree hidden = parent.getChild(HIDDEN_NAME);
            hidden.orderBefore("someother");
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // success
        }
    }

    @Test
    public void testSetOrderableChildNodesOnHiddenTree() {
        try {
            Tree hidden = parent.getChild(HIDDEN_NAME);
            hidden.setOrderableChildren(true);
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // success
        }

        try {
            Tree hidden = parent.getChild(HIDDEN_NAME);
            hidden.setOrderableChildren(false);
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // success
        }
    }

    @Test
    public void testGetHiddenChildren() {
        Iterable<Tree> children = parent.getChildren();
        assertFalse(children.iterator().hasNext());
    }

    @Test
    public void testGetHiddenChildrenCount() {
        assertEquals(0, parent.getChildrenCount(1));
    }

    @Test
    public void testCreateHiddenChild() {
        try {
            Tree hidden = parent.addChild(":hiddenChild");
            root.commit();
            fail();
        } catch (Exception e) {
            // success
        }
    }
}