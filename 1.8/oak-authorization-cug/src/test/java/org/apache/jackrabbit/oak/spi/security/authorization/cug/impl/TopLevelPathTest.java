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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TopLevelPathTest extends AbstractCugTest {

    private static final List<String> PATHS = ImmutableList.of(ROOT_PATH, SUPPORTED_PATH, SUPPORTED_PATH + "/subtree", SUPPORTED_PATH3, UNSUPPORTED_PATH, INVALID_PATH);

    @Test
    public void testHasAnyNoCug() {
        TopLevelPaths tlp = new TopLevelPaths(getRootProvider().createReadOnlyRoot(root));
        assertFalse(tlp.hasAny());
        assertFalse(tlp.hasAny());
    }

    @Test
    public void testHasAnyWithCug() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        createCug(tree.getPath(), EveryonePrincipal.getInstance());
        root.commit();

        TopLevelPaths tlp = new TopLevelPaths(getRootProvider().createReadOnlyRoot(root));
        assertTrue(tlp.hasAny());
        assertTrue(tlp.hasAny());
    }

    @Test
    public void testContainsNoCug()  throws Exception {
        TopLevelPaths tlp = new TopLevelPaths(getRootProvider().createReadOnlyRoot(root));
        for (String p : PATHS) {
            assertFalse(tlp.contains(p));
        }
    }

    @Test
    public void testContainsWithCug()  throws Exception {
        String cugPath = TreeUtil
                .addChild(root.getTree(SUPPORTED_PATH3), "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED).getPath();
        createCug(cugPath, EveryonePrincipal.getInstance());
        root.commit();

        TopLevelPaths tlp = new TopLevelPaths(getRootProvider().createReadOnlyRoot(root));

        assertTrue(tlp.contains(ROOT_PATH));
        assertTrue(tlp.contains(SUPPORTED_PATH3));
        assertTrue(tlp.contains(cugPath));

        assertFalse(tlp.contains(cugPath + "/subtree"));
        assertFalse(tlp.contains(SUPPORTED_PATH));
        assertFalse(tlp.contains(UNSUPPORTED_PATH));
    }

    @Test
    public void testContainsWithCugAtRoot() throws Exception {
        createCug(root, ROOT_PATH, EveryonePrincipal.NAME);
        root.commit();

        TopLevelPaths tlp = new TopLevelPaths(getRootProvider().createReadOnlyRoot(root));

        assertTrue(tlp.contains(ROOT_PATH));

        assertFalse(tlp.contains(SUPPORTED_PATH));
        assertFalse(tlp.contains(SUPPORTED_PATH2));
        assertFalse(tlp.contains(SUPPORTED_PATH3));
        assertFalse(tlp.contains(UNSUPPORTED_PATH));
    }

    @Test
    public void testContainsMany() throws Exception {
        Tree n = root.getTree(SUPPORTED_PATH3);
        for (int i = 0; i <= TopLevelPaths.MAX_CNT; i++) {
            Tree c = TreeUtil.addChild(n, "c" + i, NT_OAK_UNSTRUCTURED);
            createCug(c.getPath(), EveryonePrincipal.getInstance());
        }
        root.commit();

        TopLevelPaths tlp = new TopLevelPaths(getRootProvider().createReadOnlyRoot(root));
        assertTrue(tlp.contains(ROOT_PATH));
        assertTrue(tlp.contains(SUPPORTED_PATH));
        assertTrue(tlp.contains(SUPPORTED_PATH2));
        assertTrue(tlp.contains(SUPPORTED_PATH3));
        assertTrue(tlp.contains(UNSUPPORTED_PATH));
        assertTrue(tlp.contains(Text.getRelativeParent(SUPPORTED_PATH3, 1)));
        assertTrue(tlp.contains(SUPPORTED_PATH3 + "/c0"));
    }

    @Test
    public void testMayContainWithCug() throws Exception {
        String cugPath = TreeUtil
                .addChild(root.getTree(SUPPORTED_PATH3), "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED).getPath();
        createCug(cugPath, EveryonePrincipal.getInstance());
        root.commit();

        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(root);
        TopLevelPaths tlp = new TopLevelPaths(readOnlyRoot);
        for (String p : PATHS) {
            assertEquals(p, Text.isDescendantOrEqual(p, cugPath), tlp.contains(p));
        }

        assertTrue(tlp.contains(Text.getAbsoluteParent(SUPPORTED_PATH3, 0)));
        assertTrue(tlp.contains(cugPath));

        CugPermissionProvider cugPermProvider = createCugPermissionProvider(
                        ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2, SUPPORTED_PATH3),
                        getTestUser().getPrincipal(), EveryonePrincipal.getInstance());

        Tree t = readOnlyRoot.getTree(ROOT_PATH);
        TreePermission rootTp = cugPermProvider.getTreePermission(t, TreePermission.EMPTY);
        assertTrue(rootTp instanceof EmptyCugTreePermission);

        TreePermission tp = rootTp;
        for (String segm : PathUtils.elements(cugPath)) {
            t = t.getChild(segm);
            tp = cugPermProvider.getTreePermission(t, tp);
            assertCugPermission(tp, Text.isDescendantOrEqual(SUPPORTED_PATH3, t.getPath()));
        }

        for (String p : new String[] {SUPPORTED_PATH, SUPPORTED_PATH2, UNSUPPORTED_PATH }) {
            tp = getTreePermission(readOnlyRoot, p, cugPermProvider);
            assertSame(p, TreePermission.NO_RECOURSE, tp);
        }
    }

    @Test
    public void testMayContainWithCug2() throws Exception {
        String cugPath = SUPPORTED_PATH + "/subtree";
        createCug(cugPath, EveryonePrincipal.getInstance());
        root.commit();

        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(root);
        TopLevelPaths tlp = new TopLevelPaths(readOnlyRoot);

        assertTrue(tlp.contains(PathUtils.ROOT_PATH));
        assertTrue(tlp.contains(SUPPORTED_PATH));
        assertTrue(tlp.contains(cugPath));
        assertFalse(tlp.contains(SUPPORTED_PATH3));

        CugPermissionProvider cugPermProvider = createCugPermissionProvider(
                ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2, SUPPORTED_PATH3),
                getTestUser().getPrincipal(), EveryonePrincipal.getInstance());

        Tree t = readOnlyRoot.getTree(PathUtils.ROOT_PATH);
        TreePermission rootTp = cugPermProvider.getTreePermission(t, TreePermission.EMPTY);
        assertTrue(rootTp instanceof EmptyCugTreePermission);

        TreePermission tp = rootTp;
        for (String segm : PathUtils.elements(cugPath)) {
            t = t.getChild(segm);
            tp = cugPermProvider.getTreePermission(t, tp);
            assertCugPermission(tp, Text.isDescendantOrEqual(SUPPORTED_PATH, t.getPath()));
        }

        tp = getTreePermission(readOnlyRoot, Text.getAbsoluteParent(SUPPORTED_PATH3, 0), cugPermProvider);
        assertSame(TreePermission.NO_RECOURSE, tp);

        for (String p : new String[] {SUPPORTED_PATH2, UNSUPPORTED_PATH }) {
            tp = getTreePermission(readOnlyRoot, p, cugPermProvider);
            assertSame(p, TreePermission.NO_RECOURSE, tp);
        }
    }
}
