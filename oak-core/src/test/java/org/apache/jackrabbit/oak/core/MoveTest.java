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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Additional move tests for complete test coverage.
 */
public class MoveTest {

    private ContentSession session = new Oak().with(new OpenSecurityProvider()).createContentSession();

    @Before
    public void setUp() throws CommitFailedException {
        // Add test content
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.addChild("a");
        tree.addChild("b");
        tree.addChild("c").addChild("d");
        root.commit();
    }

    @After
    public void tearDown() {
        session = null;
    }

    @Test
    public void nonExistentSource() {
        Root root = session.getLatestRoot();
        assertFalse(root.move("/x", "/y"));
    }

    @Test
    public void nonExistentDestinationParent() {
        Root root = session.getLatestRoot();
        assertFalse(root.move("/a", "/x/y"));
    }

    @Test
    public void destinationExists() {
        Root root = session.getLatestRoot();
        assertFalse(root.move("/a", "/b"));
    }

    @Test
    public void rename() throws Exception {
        Root root = session.getLatestRoot();
        Tree d = root.getTree("/a");
        assertTrue(root.move("/a", "/x"));
        assertEquals("/x", d.getPath());
        root.commit();
        assertEquals("/x", d.getPath());
    }

    @Test
    public void moveOne() throws Exception {
        Root root = session.getLatestRoot();
        Tree d = root.getTree("/c/d");
        assertTrue(root.move("/c/d", "/a/d"));
        assertEquals("/a/d", d.getPath());
        root.commit();
        assertEquals("/a/d", d.getPath());
    }

    @Test
    public void moveSubTree() throws Exception {
        Root root = session.getLatestRoot();
        Tree d = root.getTree("/c/d");
        assertTrue(root.move("/c", "/a/c"));
        assertEquals("/a/c/d", d.getPath());
        root.commit();
        assertEquals("/a/c/d", d.getPath());
    }

    @Test
    public void moveMoved() throws Exception {
        Root root = session.getLatestRoot();
        Tree d = root.getTree("/c/d");
        assertTrue(root.move("/c", "/a/c"));
        assertTrue(root.move("/a", "/x"));
        assertEquals("/x/c/d", d.getPath());
        root.commit();
        assertEquals("/x/c/d", d.getPath());
    }

    @Test
    public void moveBack() throws Exception {
        Root root = session.getLatestRoot();
        Tree d = root.getTree("/c/d");
        assertTrue(root.move("/c/d", "/a/x"));
        assertTrue(root.move("/a/x", "/c/d"));
        assertEquals("/c/d", d.getPath());
        root.commit();
        assertEquals("/c/d", d.getPath());
    }

    @Test
    public void moveBackSubTree() throws Exception {
        Root root = session.getLatestRoot();
        Tree d = root.getTree("/c/d");
        assertTrue(root.move("/c", "/x"));
        assertTrue(root.move("/x", "/c"));
        assertEquals("/c/d", d.getPath());
        root.commit();
        assertEquals("/c/d", d.getPath());
    }
}
