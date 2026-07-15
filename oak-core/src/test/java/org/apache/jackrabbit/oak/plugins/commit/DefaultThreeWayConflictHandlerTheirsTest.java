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
package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultThreeWayConflictHandlerTheirsTest {

    private static final String OUR_VALUE = "our value";
    private static final String THEIR_VALUE = "their value";

    private Root ourRoot;
    private Root theirRoot;

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = new Oak()
                .with(new OpenSecurityProvider())
                .with(DefaultThreeWayConflictHandler.THEIRS)
                .createContentSession();

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

        ourRoot = session.getLatestRoot();
        theirRoot = session.getLatestRoot();
    }

    @After
    public void tearDown() {
        ourRoot = null;
        theirRoot = null;
    }

    @Test
    public void testAddExistingProperty() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("p", THEIR_VALUE);
        theirRoot.getTree("/").setProperty("q", THEIR_VALUE);
        ourRoot.getTree("/").setProperty("p", OUR_VALUE);
        ourRoot.getTree("/").setProperty("q", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("p");
        assertNotNull(p);
        assertEquals(THEIR_VALUE, p.getValue(STRING));

        PropertyState q = ourRoot.getTree("/").getProperty("q");
        assertNotNull(q);
        assertEquals(THEIR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testChangeDeletedProperty() throws CommitFailedException {
        theirRoot.getTree("/").removeProperty("a");
        ourRoot.getTree("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNull(p);
    }

    @Test
    public void testChangeChangedProperty() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTree("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNotNull(p);
        assertEquals(THEIR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testDeleteDeletedProperty() throws CommitFailedException {
        theirRoot.getTree("/").removeProperty("a");
        ourRoot.getTree("/").removeProperty("a");

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNull(p);
    }

    @Test
    public void testDeleteChangedProperty() throws CommitFailedException {
        theirRoot.getTree("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTree("/").removeProperty("a");

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTree("/").getProperty("a");
        assertNotNull(p);
        assertEquals(THEIR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testAddExistingNode() throws CommitFailedException {
        theirRoot.getTree("/").addChild("n").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/").addChild("n").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTree("/n");
        assertNotNull(n);
        assertEquals(THEIR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testChangeDeletedNode() throws CommitFailedException {
        theirRoot.getTree("/x").remove();
        ourRoot.getTree("/x").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTree("/x");
        assertFalse(n.exists());
    }

    @Test
    public void testDeleteChangedNode() throws CommitFailedException {
        theirRoot.getTree("/x").setProperty("p", THEIR_VALUE);
        ourRoot.getTree("/x").remove();

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTree("/x");
        assertTrue(n.exists());
        assertEquals(THEIR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testDeleteDeletedNode() throws CommitFailedException {
        theirRoot.getTree("/x").remove();
        ourRoot.getTree("/x").remove();

        theirRoot.commit();
        ourRoot.commit();

        assertFalse(ourRoot.getTree("/x").exists());
    }
}
