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
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DefaultConflictHandlerOursTest {

    private static final String OUR_VALUE = "our value";
    private static final String THEIR_VALUE = "their value";

    private RootImpl ourRoot;
    private Root theirRoot;

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = new Oak()
                .with(new OpenSecurityProvider())
                .with(DefaultConflictHandler.OURS)
                .createContentSession();

        // Add test content
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");
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
    public void testAddExistingProperties() throws CommitFailedException {
        theirRoot.getTreeOrNull("/").setProperty("p", THEIR_VALUE);
        theirRoot.getTreeOrNull("/").setProperty("q", THEIR_VALUE);
        ourRoot.getTreeOrNull("/").setProperty("p", OUR_VALUE);
        ourRoot.getTreeOrNull("/").setProperty("q", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTreeOrNull("/").getProperty("p");
        assertNotNull(p);
        assertEquals(OUR_VALUE, p.getValue(STRING));

        PropertyState q = ourRoot.getTreeOrNull("/").getProperty("q");
        assertNotNull(q);
        assertEquals(OUR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testChangeDeletedProperty() throws CommitFailedException {
        theirRoot.getTreeOrNull("/").removeProperty("a");
        ourRoot.getTreeOrNull("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTreeOrNull("/").getProperty("a");
        assertNotNull(p);
        assertEquals(OUR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testChangeChangedProperty() throws CommitFailedException {
        theirRoot.getTreeOrNull("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTreeOrNull("/").setProperty("a", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTreeOrNull("/").getProperty("a");
        assertNotNull(p);
        assertEquals(OUR_VALUE, p.getValue(STRING));
    }

    @Test
    public void testDeleteChangedProperty() throws CommitFailedException {
        theirRoot.getTreeOrNull("/").setProperty("a", THEIR_VALUE);
        ourRoot.getTreeOrNull("/").removeProperty("a");

        theirRoot.commit();
        ourRoot.commit();

        PropertyState p = ourRoot.getTreeOrNull("/").getProperty("a");
        assertNull(p);
    }

    @Test
    public void testAddExistingNode() throws CommitFailedException {
        theirRoot.getTreeOrNull("/").addChild("n").setProperty("p", THEIR_VALUE);
        ourRoot.getTreeOrNull("/").addChild("n").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTreeOrNull("/n");
        assertNotNull(n);
        assertEquals(OUR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testChangeDeletedNode() throws CommitFailedException {
        theirRoot.getTreeOrNull("/x").remove();
        ourRoot.getTreeOrNull("/x").setProperty("p", OUR_VALUE);

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTreeOrNull("/x");
        assertNotNull(n);
        assertEquals(OUR_VALUE, n.getProperty("p").getValue(STRING));
    }

    @Test
    public void testDeleteChangedNode() throws CommitFailedException {
        theirRoot.getTreeOrNull("/x").setProperty("p", THEIR_VALUE);
        ourRoot.getTreeOrNull("/x").remove();

        theirRoot.commit();
        ourRoot.commit();

        Tree n = ourRoot.getTreeOrNull("/x");
        assertNull(n);
    }

}
