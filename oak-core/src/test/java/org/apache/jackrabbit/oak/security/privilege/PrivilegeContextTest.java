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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class PrivilegeContextTest {

    private final Context ctx = PrivilegeContext.getInstance();

    private static Tree mockTree(@Nonnull String name, @Nonnull String ntName) {
        Tree t = Mockito.mock(Tree.class);
        when(t.getName()).thenReturn(name);
        when(t.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, ntName, Type.NAME));
        return t;
    }


    @Test
    public void testDefinesProperty() {
        for (String propName : PrivilegeConstants.PRIVILEGE_PROPERTY_NAMES) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            for (String ntName : PrivilegeConstants.PRIVILEGE_NODETYPE_NAMES) {
                assertTrue(ctx.definesProperty(mockTree("anyName", ntName), property));

            }
        }
    }

    @Test
    public void testNameNotDefinesProperty() {
        for (String propName : new String[] {"anyName", JcrConstants.JCR_PRIMARYTYPE}) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            for (String ntName : PrivilegeConstants.PRIVILEGE_NODETYPE_NAMES) {
                assertFalse(ctx.definesProperty(mockTree("anyName", ntName), property));

            }
        }
    }


    @Test
    public void testParentNotDefinesProperty() {
        for (String propName : PrivilegeConstants.PRIVILEGE_PROPERTY_NAMES) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            for (String ntName : new String[] {JcrConstants.NT_BASE, JcrConstants.NT_UNSTRUCTURED}) {
                assertFalse(ctx.definesProperty(mockTree("anyName", ntName), property));

            }
        }
    }

    @Test
    public void testDefinesContextRoot() {
        assertTrue(ctx.definesContextRoot(mockTree(PrivilegeConstants.REP_PRIVILEGES, PrivilegeConstants.NT_REP_PRIVILEGES)));
    }

    @Test
    public void testNotDefinesContextRoot() {
        assertFalse(ctx.definesContextRoot(mockTree(PrivilegeConstants.REP_PRIVILEGES, PrivilegeConstants.NT_REP_PRIVILEGE)));
        assertFalse(ctx.definesContextRoot(mockTree(PrivilegeConstants.REP_PRIVILEGES, JcrConstants.NT_UNSTRUCTURED)));
        assertFalse(ctx.definesContextRoot(mockTree(PrivilegeConstants.REP_PRIVILEGES, NodeTypeConstants.NT_REP_NAMED_CHILD_NODE_DEFINITIONS)));
        assertFalse(ctx.definesContextRoot(mockTree("anyName", PrivilegeConstants.NT_REP_PRIVILEGES)));
    }

    @Test
    public void testDefinesTree() {
        for (String ntName : PrivilegeConstants.PRIVILEGE_NODETYPE_NAMES) {
            assertTrue(ctx.definesTree(mockTree("anyName", ntName)));
        }
    }

    @Test
    public void testEmptyNotDefinesTree() {
        assertFalse(ctx.definesTree(TreeFactory.createReadOnlyTree(EmptyNodeState.EMPTY_NODE)));
    }

    @Test
    public void testNotDefinesTree() {
        for (String ntName : new String[] {JcrConstants.NT_UNSTRUCTURED, JcrConstants.NT_BASE, NodeTypeConstants.NT_REP_SYSTEM, NodeTypeConstants.NT_REP_ROOT}) {
            assertFalse(ctx.definesTree(mockTree(PrivilegeConstants.REP_PRIVILEGES, ntName)));
        }
    }

    @Test
    public void testDefinesLocation() {
        List<String> paths = ImmutableList.of(
                PrivilegeConstants.PRIVILEGES_PATH,
                PrivilegeConstants.PRIVILEGES_PATH + "/child",
                PrivilegeConstants.PRIVILEGES_PATH + "/another/child"
        );

        for (String path : paths) {
            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getPath()).thenReturn(path);

            assertTrue(path, ctx.definesLocation(location));
        }
    }

    @Test
    public void testNotDefinesLocation() {
        List<String> paths = ImmutableList.of(
                PathUtils.ROOT_PATH,
                PrivilegeConstants.PRIVILEGES_PATH + "sibling",
                "/some/other/path",
                ""
        );

        for (String path : paths) {
            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getPath()).thenReturn(path);

            assertFalse(path, ctx.definesLocation(location));
        }
    }

    @Test
    public void testDefinesInternal() {
        assertFalse(ctx.definesInternal(Mockito.mock(Tree.class)));
    }
}