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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextImplTest implements Constants {

    private static final Context CTX = ContextImpl.INSTANCE;

    private static Tree mockTree(@NotNull String name, @NotNull String ntName, boolean exists, @NotNull String... propertyNames) {
        return MockUtility.mockTree(name, ntName, exists, propertyNames);
    }

    @Test
    public void testDefinesTree() {
        assertTrue(CTX.definesTree(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true)));
        assertTrue(CTX.definesTree(mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, true)));
        assertTrue(CTX.definesTree(mockTree("anyEntry", NT_REP_PRINCIPAL_ENTRY, true)));
    }

    @Test
    public void testNotDefinesTree() {
        assertFalse(CTX.definesTree(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, false)));
        assertFalse(CTX.definesTree(mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, false)));
        assertFalse(CTX.definesTree(mockTree("anyEntry", NT_REP_PRINCIPAL_ENTRY, false)));
        assertFalse(CTX.definesTree(mockTree("anyName", "anyNt", false)));
        assertFalse(CTX.definesTree(mockTree("anyName", "anyNt", true)));
    }

    @Test
    public void testDefinesProperty() {
        PropertyState anyProperty = mock(PropertyState.class);

        assertTrue(CTX.definesProperty(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true), anyProperty));
        assertTrue(CTX.definesProperty(mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, true), anyProperty));
        assertTrue(CTX.definesProperty(mockTree("anyEntry", NT_REP_PRINCIPAL_ENTRY, true), anyProperty));
    }

    @Test
    public void testDefinesCtxRoot() {
        assertTrue(CTX.definesContextRoot(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true)));
    }

    @Test
    public void testNotDefinesCtxRoot() {
        assertFalse(CTX.definesContextRoot(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, false)));
        assertFalse(CTX.definesContextRoot(mockTree(AccessControlConstants.REP_POLICY, NT_REP_PRINCIPAL_POLICY, true)));
        assertFalse(CTX.definesContextRoot(mockTree(REP_PRINCIPAL_POLICY, AccessControlConstants.NT_REP_POLICY, true)));
        assertFalse(CTX.definesContextRoot(mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, true)));
    }

    @Test
    public void testDefinesNodeLocation() {
        assertTrue(CTX.definesLocation(TreeLocation.create(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true))));
        assertTrue(CTX.definesLocation(TreeLocation.create(mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, true))));
        assertTrue(CTX.definesLocation(TreeLocation.create(mockTree("anyEntry", NT_REP_PRINCIPAL_ENTRY, true))));
    }

    @Test
    public void testDefinesPropertyLocation() {
        String[] propNames = new String[] {REP_PRINCIPAL_NAME, "prop1", "prop2"};
        Tree policyTree = MockUtility.mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, PathUtils.ROOT_PATH + REP_PRINCIPAL_POLICY, propNames);

        Tree rootTree = MockUtility.mockTree(PathUtils.ROOT_NAME, NodeTypeConstants.NT_REP_ROOT, PathUtils.ROOT_PATH, propNames);
        when(rootTree.getChild(REP_PRINCIPAL_POLICY)).thenReturn(policyTree);

        Root r = when(mock(Root.class).getTree(PathUtils.ROOT_PATH)).thenReturn(rootTree).getMock();

        for (String propName : propNames) {
            assertTrue(CTX.definesLocation(TreeLocation.create(r, PathUtils.concat(policyTree.getPath(), propName))));
            assertFalse(CTX.definesLocation(TreeLocation.create(r, PathUtils.concat(rootTree.getPath(), propName))));
        }
    }

    @Test
    public void testDefinesNonExistingLocation() {
        assertTrue(CTX.definesLocation(TreeLocation.create(mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, false))));
        assertTrue(CTX.definesLocation(TreeLocation.create(mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, false))));

        Tree t = mockTree(REP_RESTRICTIONS, NT_REP_RESTRICTIONS, false, "anyResidualProperty");
        assertTrue(CTX.definesLocation(TreeLocation.create(t).getChild("anyResidualProperty")));

        t = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, false, "anyResidualAceName");
        assertTrue(CTX.definesLocation(TreeLocation.create(t).getChild("anyResidualAceName")));

        String[] propNames = new String[] {REP_EFFECTIVE_PATH, REP_PRINCIPAL_NAME, REP_PRIVILEGES};
        t = mockTree("anyEntry", "anyNt", false, propNames);
        for (String propName : propNames) {
            assertTrue(CTX.definesLocation(TreeLocation.create(t).getChild(propName)));
        }

        String[] nodeNames = new String[] {REP_PRINCIPAL_POLICY, REP_RESTRICTIONS};
        for (String nodeName : nodeNames) {
            assertTrue(CTX.definesLocation(TreeLocation.create(mockTree(nodeName, "anyNt", false))));
        }
    }

    @Test
    public void testNotDefinesLocation() {
        Tree t = mockTree("anyEntry", "anyNt", false, "anyResidualProperty");
        assertFalse(CTX.definesLocation(TreeLocation.create(t).getChild("anyResidualProperty")));

        t = mockTree("anyEntry", "anyNt", true, "anyResidualProperty");
        assertFalse(CTX.definesLocation(TreeLocation.create(t).getChild("anyResidualProperty")));
    }

    @Test
    public void testDefinesInternal() {
        assertFalse(CTX.definesInternal(mock(Tree.class)));
        assertFalse(CTX.definesInternal(when(mock(Tree.class).getName()).thenReturn(PermissionConstants.REP_PERMISSION_STORE).getMock()));
    }
}