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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

public class TreeTest extends AbstractOakCoreTest {

    private Root testRoot;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);
        setupPermission("/a/bb", testPrincipal, false, PrivilegeConstants.JCR_READ);

        testRoot = getTestRoot();
    }

    @Test
    public void testHasChild() throws Exception {
        Tree rootTree = testRoot.getTree("/");

        assertTrue(rootTree.hasChild("a"));
        assertFalse(rootTree.hasChild(AccessControlConstants.REP_POLICY));

        Tree a = rootTree.getChild("a");
        assertTrue(a.hasChild("b"));
        assertFalse(a.hasChild("bb"));

        Tree b = a.getChild("b");
        assertTrue(b.hasChild("c"));
    }

    @Test
    public void testGetChild() throws Exception {
        Tree rootTree = testRoot.getTree("/");
        assertTrue(rootTree.exists());

        Tree a = rootTree.getChild("a");
        assertTrue(a.exists());

        Tree b = a.getChild("b");
        assertTrue(b.exists());
        assertTrue(b.getChild("c").exists());

        assertFalse(a.getChild("bb").exists());
    }

    @Test
    public void testPolicyChild() throws Exception {
        assertTrue(root.getTree('/' + AccessControlConstants.REP_POLICY).exists());

        // 'testUser' must not have access to the policy node
        Tree rootTree = testRoot.getTree("/");

        assertFalse(rootTree.hasChild(AccessControlConstants.REP_POLICY));
        assertFalse(rootTree.getChild(AccessControlConstants.REP_POLICY).exists());
    }

    @Test
	public void testGetChildrenCount() throws Exception {
        long cntRoot = root.getTree("/").getChildrenCount(Long.MAX_VALUE);
        long cntA = root.getTree("/a").getChildrenCount(Long.MAX_VALUE);

        // 'testUser' may only see 'regular' child nodes -> count must be adjusted.
        assertEquals(cntRoot-1, testRoot.getTree("/").getChildrenCount(Long.MAX_VALUE));
        assertEquals(cntA - 1, testRoot.getTree("/a").getChildrenCount(Long.MAX_VALUE));

        // for the following nodes the cnt must not differ
        List<String> paths = ImmutableList.of("/a/b", "/a/b/c");
        for (String path : paths) {
            assertEquals(
                    root.getTree(path).getChildrenCount(Long.MAX_VALUE),
                    testRoot.getTree(path).getChildrenCount(Long.MAX_VALUE));
        }
    }

    @Test
    public void testGetChildren() throws Exception {
        for (Tree t : testRoot.getTree("/a").getChildren()) {
            if (!"b".equals(t.getName())) {
                fail("Child " + t.getName() + " should not be accessible.");
            }
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-842">OAK-842</a>
     */
    @Test
    public void testOrderableChildren() throws Exception {
        Tree a = root.getTree("/a");
        a.setOrderableChildren(true);

        testRoot.refresh();
        for (Tree t : testRoot.getTree("/a").getChildren()) {
            if (!"b".equals(t.getName())) {
                fail("Child " + t.getName() + " should not be accessible.");
            }
        }
    }

    @Test
    public void testHasProperty() throws Exception {
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);

        testRoot.refresh();
        Tree a = testRoot.getTree("/a");
        assertFalse(a.hasProperty("aProp"));
    }

    @Test
    public void testGetProperty() throws Exception {
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);

        testRoot.refresh();
        Tree a = testRoot.getTree("/a");
        assertNull(a.getProperty("aProp"));
    }

    @Test
    public void testGetPropertyStatus() throws Exception {
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_NODES);

        testRoot.refresh();
        Tree a = testRoot.getTree("/a");
        assertFalse(a.exists());

        PropertyState p = a.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        assertNotNull(p);
        assertEquals(Tree.Status.UNCHANGED, a.getPropertyStatus(JcrConstants.JCR_PRIMARYTYPE));

    }

    @Test
    public void testGetPropertyCount() throws Exception {
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);

        testRoot.refresh();
        Tree a = testRoot.getTree("/a");
        assertEquals(0, a.getPropertyCount());
    }

    @Test
    public void testGetProperties() throws Exception {
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);

        testRoot.refresh();
        Tree a = testRoot.getTree("/a");
        Iterable<? extends PropertyState> props = a.getProperties();
        assertFalse(props.iterator().hasNext());
    }
}
