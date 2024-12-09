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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.ReadOnly;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.security.Principal;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_ACCESS_CONTROLLED_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class PermissionUtilTest {

    @Test
    public void testParentPathOrNull() {
        for (String path : new String[] {"", PathUtils.ROOT_PATH, "a"}) {
            assertNull(PermissionUtil.getParentPathOrNull(path));
        }

        assertEquals(PathUtils.ROOT_PATH, PermissionUtil.getParentPathOrNull("/single"));

        for (String path : new String[] {"/a/path", "/a/longer/path"}) {
            assertEquals(PathUtils.getParentPath(path), PermissionUtil.getParentPathOrNull(path));
        }
    }

    @Test
    public void testCheckACLPath() {
        Tree tree = mock(Tree.class);
        String path = "/path";

        when(tree.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(null);
        assertFalse(PermissionUtil.checkACLPath(tree, path));

        PropertyState nonMatching = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, "/another");
        when(tree.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(nonMatching);
        assertFalse(PermissionUtil.checkACLPath(tree, path));

        PropertyState matching = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, "/path");
        when(tree.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(matching);
        assertTrue(PermissionUtil.checkACLPath(tree, path));
    }

    @Test
    public void testCheckACLPath2() {
        NodeBuilder nb = mock(NodeBuilder.class);
        String path = "/path";

        when(nb.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(null);
        assertFalse(PermissionUtil.checkACLPath(nb, path));

        PropertyState nonMatching = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, "/another");
        when(nb.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(nonMatching);
        assertFalse(PermissionUtil.checkACLPath(nb, path));

        PropertyState matching = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, "/path");
        when(nb.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(matching);
        assertTrue(PermissionUtil.checkACLPath(nb, path));
    }

    @Test
    public void testIsAdminOrSystem() {
        ConfigurationParameters params = ConfigurationParameters.of(PARAM_ADMINISTRATIVE_PRINCIPALS, Set.of("administrative"));

        Set<Principal> principals = Set.of(new PrincipalImpl("name"), new PrincipalImpl("administrative"));
        assertTrue(PermissionUtil.isAdminOrSystem(principals, params));
        assertFalse(PermissionUtil.isAdminOrSystem(principals, ConfigurationParameters.EMPTY));
    }

    @Test
    public void testIsAdminOrSystemForAdminPrincipal() {
        assertTrue(PermissionUtil.isAdminOrSystem(Set.of(mock(AdminPrincipal.class)), ConfigurationParameters.EMPTY));
        assertTrue(PermissionUtil.isAdminOrSystem(Set.of(new PrincipalImpl("name"), mock(AdminPrincipal.class)), ConfigurationParameters.EMPTY));
    }

    @Test
    public void testIsAdminOrSystemForSystemPrincial() {
        assertTrue(PermissionUtil.isAdminOrSystem(Set.of(SystemPrincipal.INSTANCE), ConfigurationParameters.EMPTY));
        assertTrue(PermissionUtil.isAdminOrSystem(Set.of(new PrincipalImpl("name"), SystemPrincipal.INSTANCE), ConfigurationParameters.EMPTY));
    }

    @Test
    public void testGetPath() {
        Tree t = when(mock(Tree.class).getPath()).thenReturn("/path").getMock();

        assertNull(PermissionUtil.getPath(null, null));
        assertEquals("/path", PermissionUtil.getPath(t, null));
        assertEquals("/path", PermissionUtil.getPath(null, t));

        Tree afterT = when(mock(Tree.class).getPath()).thenReturn("/afterPath").getMock();
        assertEquals("/path", PermissionUtil.getPath(t, afterT));
    }

    @Test
    public void testGetReadOnlyTreeOrNullFromNull() {
        Root r = mock(Root.class);

        assertNull(PermissionUtil.getReadOnlyTreeOrNull(null, r));
        verify(r, never()).getTree(anyString());
    }

    @Test
    public void testGetReadOnlyTreeOrNull() {
        Tree readOnlyTree = mock(Tree.class, withSettings().extraInterfaces(ReadOnly.class));
        Root r = mock(Root.class);

        assertSame(readOnlyTree, PermissionUtil.getReadOnlyTreeOrNull(readOnlyTree, r));
        verify(r, never()).getTree(anyString());
    }

    @Test
    public void testGetReadOnlyTreeOrNullFromTree() {
        Tree readOnlyTree = mock(Tree.class, withSettings().extraInterfaces(ReadOnly.class));

        Root r = when(mock(Root.class).getTree("/path")).thenReturn(readOnlyTree).getMock();
        Tree t = when(mock(Tree.class).getPath()).thenReturn("/path").getMock();

        assertSame(readOnlyTree, PermissionUtil.getReadOnlyTreeOrNull(t, r));
        verify(r, times(1)).getTree("/path");
    }

    @Test
    public void testGetReadOnlyTree() {
        Tree readOnlyTree = mock(Tree.class, withSettings().extraInterfaces(ReadOnly.class));
        Root r = mock(Root.class);

        assertSame(readOnlyTree, PermissionUtil.getReadOnlyTree(readOnlyTree, r));
        verify(r, never()).getTree(anyString());
    }

    @Test
    public void testGetReadOnlyTreeFromTree() {
        Tree readOnlyTree = mock(Tree.class, withSettings().extraInterfaces(ReadOnly.class));

        Root r = when(mock(Root.class).getTree("/path")).thenReturn(readOnlyTree).getMock();
        Tree t = when(mock(Tree.class).getPath()).thenReturn("/path").getMock();

        assertSame(readOnlyTree, PermissionUtil.getReadOnlyTree(t, r));
        verify(r, times(1)).getTree("/path");
    }
}