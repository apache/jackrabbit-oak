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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;

public class NestedCugHookTest extends AbstractCugTest {

    protected static void assertNestedCugs(@Nonnull Root root, @Nonnull RootProvider rootProvider,
            @Nonnull String cugHoldingPath, boolean hasCugPolicy, @Nonnull String... expectedNestedPaths) {
        Root immutableRoot = rootProvider.createReadOnlyRoot(root);

        Tree tree = immutableRoot.getTree(cugHoldingPath);
        if (hasCugPolicy) {
            assertFalse(tree.hasProperty(HIDDEN_NESTED_CUGS));
            tree = tree.getChild(REP_CUG_POLICY);
        }

        assertTrue(tree.exists());

        if (tree.isRoot()) {
            if (expectedNestedPaths.length == 0) {
                assertFalse(tree.hasProperty(HIDDEN_TOP_CUG_CNT));
            } else {
                assertTrue(tree.hasProperty(HIDDEN_TOP_CUG_CNT));
                assertEquals(Long.valueOf(expectedNestedPaths.length), tree.getProperty(HIDDEN_TOP_CUG_CNT).getValue(Type.LONG));
            }
        } else {
            assertFalse(tree.hasProperty(HIDDEN_TOP_CUG_CNT));
        }

        if (expectedNestedPaths.length == 0) {
            assertFalse(tree.hasProperty(HIDDEN_NESTED_CUGS));
        } else {
            assertTrue(tree.hasProperty(HIDDEN_NESTED_CUGS));

            PropertyState nestedCugs = tree.getProperty(HIDDEN_NESTED_CUGS);
            assertNotNull(nestedCugs);

            Set<String> nestedPaths = ImmutableSet.copyOf(nestedCugs.getValue(Type.PATHS));
            assertEquals(ImmutableSet.copyOf(expectedNestedPaths), nestedPaths);
        }
    }

    protected boolean removeCug(@Nonnull String path, boolean doCommit) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        for (AccessControlPolicy policy : acMgr.getPolicies(path)) {
            if (policy instanceof CugPolicy) {
                acMgr.removePolicy(path, policy);
                if (doCommit) {
                    root.commit();
                }
                return true;
            }
        }
        return false;
    }

    @Test
    public void testToString() {
        assertEquals("NestedCugHook", new NestedCugHook().toString());
    }

    @Test
    public void testAddCug() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        root.commit();

        assertNestedCugs(root, getRootProvider(), "/", false, "/content");
        assertNestedCugs(root, getRootProvider(), "/content", true);
    }

    @Test
    public void testAddNestedCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/a", "/content/aa/bb", "/content2");
        assertNestedCugs(root, getRootProvider(), "/content/a", true, "/content/a/b/c");

        // add CUG at /content after having created CUGs in the subtree
        createCug("/content", EveryonePrincipal.getInstance());
        root.commit();
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content", "/content2");
        assertNestedCugs(root, getRootProvider(), "/content", true, "/content/a", "/content/aa/bb");
        assertNestedCugs(root, getRootProvider(), "/content/a", true, "/content/a/b/c");
    }

    @Test
    public void testAddNodeWithCug() throws Exception {
        createCug(SUPPORTED_PATH2, EveryonePrincipal.getInstance());

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH2), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH2, true, path);
    }

    @Test
    public void testAddNodeWithCugManually() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(root, path, getTestGroupPrincipal().getName());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true, path);
    }

    @Test
    public void testAddAtUnsupportedPath() throws Exception {
        String unsupportedPath = UNSUPPORTED_PATH + "/child";

        createCug(root, unsupportedPath, EveryonePrincipal.NAME);
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, unsupportedPath);
    }

    @Test
    public void testAddAtRoot() throws Exception {
        createCug(root, ROOT_PATH, EveryonePrincipal.NAME);
        root.commit();

        assertTrue(root.getTree(ROOT_PATH).hasChild(REP_CUG_POLICY));

        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content", "/content2");
    }

    @Test
    public void testRemoveCug() throws Exception {
        // create cugs at /content
        createCug("/content", getTestGroupPrincipal());
        root.commit();

        // remove CUG at /content
        assertTrue(removeCug("/content", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveNestedCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        // remove CUG at /content/a/b/c
        assertTrue(removeCug("/content/a/b/c", true));

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/a", "/content/aa/bb", "/content2");
        assertNestedCugs(root, getRootProvider(), "/content/a", true);
    }

    @Test
    public void testRemoveIntermediateCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        // remove CUG at /content/a
        assertTrue(removeCug("/content/a", true));

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/aa/bb", "/content2", "/content/a/b/c");
        assertFalse(root.getTree("/content/a").hasChild(REP_CUG_POLICY));
    }

    @Test
    public void testRemoveMultipleCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        assertTrue(removeCug("/content/a", false));
        assertTrue(removeCug("/content/aa/bb", false));
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2", "/content/a/b/c");
    }

    @Test
    public void testRemoveMultipleCug2() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        assertTrue(removeCug("/content/a", false));
        assertTrue(removeCug("/content/a/b/c", false));
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/aa/bb", "/content2");
    }

    @Test
    public void testRemoveContentNode() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        root.getTree("/content").remove();
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2");
    }

    @Test
    public void testRemoveContentANode() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        root.getTree("/content/a").remove();
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2", "/content/aa/bb");
    }

    @Test
    public void testRemoveRootCug() throws Exception {
        // add cug at /
        createCug(root, ROOT_PATH, EveryonePrincipal.NAME);
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertTrue(root.getTree(PathUtils.concat(ROOT_PATH, REP_CUG_POLICY)).remove());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content", "/content2");

        assertTrue(removeCug("/content", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2");

        assertTrue(removeCug("/content2", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveAndReadd() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true, path);

        removeCug(path, false);
        createCug(path, EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true, path);
    }

    @Test
    public void testMoveToUnsupportedPath() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        String destPath = PathUtils.concat(UNSUPPORTED_PATH, "moved");
        root.move(path, destPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3, destPath);
    }

    @Test
    public void testMoveToSupportedPath() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        String destPath = PathUtils.concat(SUPPORTED_PATH, "moved");
        root.move(path, destPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3, destPath);
    }

    @Test
    public void testMoveToNested() throws Exception {
        createCug(root, SUPPORTED_PATH2, EveryonePrincipal.NAME);
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        String destPath = PathUtils.concat(SUPPORTED_PATH2, "moved");
        root.move(path, destPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3, SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH2, true, destPath);
    }
}