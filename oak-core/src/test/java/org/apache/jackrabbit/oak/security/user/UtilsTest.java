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
package org.apache.jackrabbit.oak.security.user;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.AccessDeniedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UtilsTest extends AbstractSecurityTest {

    private Tree tree;

    @Override
    public void before() throws Exception {
        super.before();

        tree = root.getTree(PathUtils.ROOT_PATH);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private void assertEqualPath(@NotNull Tree expected, @NotNull Tree result) {
        assertEquals(expected.getPath(), result.getPath());
    }

    @Test
    public void testGetOrAddTreeCurrentElement() throws Exception {
        Tree result = Utils.getOrAddTree(tree, ".", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertSame(tree, result);
    }

    @Test
    public void testGetOrAddTreeParentElement() throws Exception {
        Tree child = Utils.getOrAddTree(tree, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree parent = Utils.getOrAddTree(child, "..", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEqualPath(tree, parent);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetOrAddTreeParentElementFromRoot() throws Exception {
        Utils.getOrAddTree(tree, "..", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }

    @Test
    public void testGetOrAddTreeSingleElement() throws Exception {
        Tree child = Utils.getOrAddTree(tree, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEqualPath(root.getTree("/child"), child);
    }

    @Test
    public void testGetOrAddTree() throws Exception {
        Map<String, String> map = ImmutableMap.of(
                "a/b/c", "/a/b/c",
                "a/../b/c", "/b/c",
                "a/b/c/../..", "/a",
                "a/././././b/c", "/a/b/c"
        );
        for (String relPath : map.keySet()) {
            Tree t = Utils.getOrAddTree(tree, relPath, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            assertEqualPath(root.getTree(map.get(relPath)), t);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testGetOrAddTreeReachesParentOfRoot() throws Exception {
        Utils.getOrAddTree(tree, "a/../../b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetOrAddTreeTargetNotAccessible() throws Exception {
        Tree nonExisting = mock(Tree.class);
        when(nonExisting.exists()).thenReturn(false);

        Tree t = mock(Tree.class);
        when(t.exists()).thenReturn(true);
        when(t.getParent()).thenReturn(t);
        when(t.getChild("a")).thenReturn(t);
        when(t.getChild("b")).thenReturn(nonExisting);
        when(t.addChild("b")).thenReturn(nonExisting);

        Utils.getOrAddTree(t, "a/a/b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }
}
