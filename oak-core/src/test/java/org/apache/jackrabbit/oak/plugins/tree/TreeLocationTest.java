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
package org.apache.jackrabbit.oak.plugins.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TreeLocationTest extends OakBaseTest {

    private Root root;
    private TreeLocation nullLocation;

    public TreeLocationTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = createContentSession();

        // Add test content
        root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        tree.setProperty("b", 2);
        tree.setProperty("c", 3);
        tree.addChild("x");
        tree.addChild("y");
        tree.addChild("z").addChild("1").addChild("2").setProperty("p", "v");
        root.commit();

        nullLocation = TreeLocation.create(root).getParent();
    }

    @After
    public void tearDown() {
        root = null;
    }

    @Test
    public void testNullLocation() {
        TreeLocation xyz = nullLocation.getChild("x").getChild("y").getChild("z");
        Assert.assertEquals("x/y/z", xyz.getPath());
        assertEquals("x/y", xyz.getParent().getPath());
        assertEquals("x", xyz.getParent().getParent().getPath());
        assertEquals(nullLocation, xyz.getParent().getParent().getParent());
    }

    @Test
    public void testParentOfRoot() {
        TreeLocation rootLocation = TreeLocation.create(root);
        assertEquals(nullLocation, rootLocation.getParent());
    }

    @Test
    public void testNodeLocation() {
        TreeLocation x = TreeLocation.create(root, "/x");
        assertNotNull(x.getTree());

        TreeLocation xyz = x.getChild("y").getChild("z");
        assertEquals("/x/y/z", xyz.getPath());
        assertNull(xyz.getTree());

        TreeLocation xy = xyz.getParent();
        assertEquals("/x/y", xy.getPath());
        assertNull(xy.getTree());

        assertEquals(x.getTree(), xy.getParent().getTree());
    }

    @Test
    public void testPropertyLocation() {
        TreeLocation a = TreeLocation.create(root, "/a");
        assertNotNull(a.getProperty());

        TreeLocation abc = a.getChild("b").getChild("c");
        assertEquals("/a/b/c", abc.getPath());
        assertNull(abc.getProperty());

        TreeLocation ab = abc.getParent();
        assertEquals("/a/b", ab.getPath());
        assertNull(ab.getProperty());

        assertEquals(a.getProperty(), ab.getParent().getProperty());
    }

    @Test
    public void getDeepLocation() {
        TreeLocation p = TreeLocation.create(root, "/z/1/2/p");
        assertNotNull(p.getProperty());
        assertEquals("/z/1/2/p", p.getPath());

        TreeLocation n = TreeLocation.create(root, "/z/1/2/3/4");
        assertNull(n.getTree());
        assertNull(n.getProperty());
        assertEquals("/z/1/2/3/4", n.getPath());

        TreeLocation two = n.getParent().getParent();
        assertNotNull(two.getTree());
        assertEquals("/z/1/2", two.getPath());
    }
}
