/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.commit.DefaultConflictHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DefaultConflictHandlerTest {

    private static final String OUR_VALUE = "foo";
    private static final String THEIR_VALUE = "bar";

    private RootImpl ourRoot;
    private Root theirRoot;

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = new Oak().createContentSession();

        // Add test content
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        tree.setProperty("b", 2);
        tree.setProperty("c", 3);
        tree.addChild("x");
        tree.addChild("y");
        tree.addChild("z");
        root.commit();

        ourRoot = (RootImpl) session.getLatestRoot();
        theirRoot = session.getLatestRoot();
    }

    @After
    public void tearDown() {
        ourRoot = null;
        theirRoot = null;
    }

    @Test
    public void testAddExistingPropertyOurs() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("p");
        assertNotNull(p);
        assertEquals(OUR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testChangeDeletedPropertyOurs() throws CommitFailedException {
        theirRoot.getTree("/").removeProperty("a");
        ourRoot.getTree("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNotNull(p);
        assertEquals(OUR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testChangeChangedPropertyOurs() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTree("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNotNull(p);
        assertEquals(OUR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testDeleteChangedPropertyOurs() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTree("/").removeProperty("a");

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNull(p);
    }

    @Test
    public void testAddExistingNodeOurs() throws CommitFailedException {
        theirRoot.getTree("/").addChild("n").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/").addChild("n").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTree("/n");
        assertNotNull(n);
        assertEquals(OUR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testChangeDeletedNodeOurs() throws CommitFailedException {
        theirRoot.getTree("/x").remove();
        ourRoot.getTree("/x").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTree("/x");
        assertNotNull(n);
        assertEquals(OUR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testDeleteChangedNodeOurs() throws CommitFailedException {
        theirRoot.getTree("/x").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/x").remove();

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTree("/x");
        assertNull(n);
    }

    @Test
    public void testAddExistingPropertyTheirs() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("p");
        assertNotNull(p);
        assertEquals(THEIR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testChangeDeletedPropertyTheirs() throws CommitFailedException {
        theirRoot.getTree("/").removeProperty("a");
        ourRoot.getTree("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNull(p);
    }

    @Test
    public void testChangeChangedPropertyTheirs() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTree("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNotNull(p);
        assertEquals(THEIR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testDeleteChangedPropertyTheirs() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTree("/").removeProperty("a");

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNotNull(p);
        assertEquals(THEIR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testAddExistingNodeTheirs() throws CommitFailedException {
        theirRoot.getTree("/").addChild("n").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/").addChild("n").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        Tree n = ourRoot.getTree("/n");
        assertNotNull(n);
        assertEquals(THEIR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testChangeDeletedNodeTheirs() throws CommitFailedException {
        theirRoot.getTree("/x").remove();
        ourRoot.getTree("/x").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        Tree n = ourRoot.getTree("/x");
        assertNull(n);
    }

    @Test
    public void testDeleteChangedNodeTheirs() throws CommitFailedException {
        theirRoot.getTree("/x").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/x").remove();

        theirRoot.commit();
        ourRoot.setConflictHandler(DefaultConflictHandler.THEIRS);
        ourRoot.commit();

        Tree n = ourRoot.getTree("/x");
        assertNotNull(n);
        assertEquals(THEIR_VALUE, n.getProperty("p").getValue(STRING));
    }

}
