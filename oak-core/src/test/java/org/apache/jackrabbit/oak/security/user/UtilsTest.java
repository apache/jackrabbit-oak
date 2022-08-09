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
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Tree t = Utils.getOrAddTree(tree, entry.getKey(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            assertEqualPath(root.getTree(entry.getValue()), t);
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
    
    @Test
    public void testIsEveryoneUser() throws Exception {
        AuthorizableImpl user = when(mock(AuthorizableImpl.class).getPrincipal()).thenReturn(EveryonePrincipal.getInstance()).getMock();
        when(user.isGroup()).thenReturn(false);
        assertFalse(Utils.isEveryone(user));
    }

    @Test
    public void testIsEveryoneGroup() throws Exception {
        Group gr = getUserManager(root).createGroup(EveryonePrincipal.getInstance());
        assertTrue(Utils.isEveryone(gr));
    }
    
    @Test
    public void testIsEveryoneOtherAuthorizable() throws Exception {
        Authorizable a = when(mock(Authorizable.class).getPrincipal()).thenReturn(EveryonePrincipal.getInstance()).getMock();
        when(a.isGroup()).thenReturn(false);
        assertFalse(Utils.isEveryone(a));

        when(a.isGroup()).thenReturn(true);
        assertTrue(Utils.isEveryone(a));
    }

    @Test
    public void testIsEveryoneGetPrincipalFails() throws Exception {
        Authorizable a = when(mock(Authorizable.class).getPrincipal()).thenThrow(new RepositoryException()).getMock();
        when(a.isGroup()).thenReturn(true);
        assertFalse(Utils.isEveryone(a));
    }
    
    @Test
    public void testGetTreeFromTreeAware() throws Exception {
        Tree t = mock(Tree.class);
        Root r = mock(Root.class);
        
        Authorizable a = mock(Authorizable.class, withSettings().extraInterfaces(TreeAware.class));
        when(((TreeAware) a).getTree()).thenReturn(t);
        
        assertSame(t, Utils.getTree(a, r));
        
        verifyNoInteractions(r);
        verify((TreeAware) a).getTree();
        verifyNoMoreInteractions(a);
    }

    @Test
    public void testGetTree() throws Exception {
        Tree t = mock(Tree.class);
        Root r = when(mock(Root.class).getTree("/user/path")).thenReturn(t).getMock();
        
        Authorizable a = mock(Authorizable.class);
        when(a.getPath()).thenReturn("/user/path");
        
        assertSame(t, Utils.getTree(a, r));
        
        verify(r).getTree(anyString());
        verify(a).getPath();
        verifyNoMoreInteractions(a, r);
    }
}
