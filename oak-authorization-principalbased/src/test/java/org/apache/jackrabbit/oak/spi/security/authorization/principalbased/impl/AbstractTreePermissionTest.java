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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AbstractTreePermissionTest {

    private Tree tree;
    private PrincipalBasedPermissionProvider pp;

    @Before
    public void before() {
        tree = mock(Tree.class);
        pp = mock(PrincipalBasedPermissionProvider.class);

    }

    private AbstractTreePermission createAbstractTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull PrincipalBasedPermissionProvider pp) {
        return new AbstractTreePermission(tree, type) {
            @Override
            PrincipalBasedPermissionProvider getPermissionProvider() {
                return pp;
            }
        };
    }

    @Test
    public void testGetTree() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.DEFAULT, pp);
        assertEquals(tree, atp.getTree());
    }

    @Test
    public void testGetType() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.INTERNAL, pp);
        assertSame(TreeType.INTERNAL, atp.getType());
    }

    @Test
    public void testGetChildPermission() {
        NodeState childState = mock(NodeState.class);
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.HIDDEN, pp);
        atp.getChildPermission("childName", childState);

        verify(pp, times(1)).getTreePermission("childName", childState, atp);
    }

    @Test
    public void testCanRead() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.DEFAULT, pp);
        atp.canRead();

        verify(pp, times(1)).isGranted(tree, null, Permissions.READ_NODE);
    }

    @Test
    public void testCanReadAcType() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.ACCESS_CONTROL, pp);
        atp.canRead();

        verify(pp, times(1)).isGranted(tree, null, Permissions.READ_ACCESS_CONTROL);
    }

    @Test
    public void testCanReadWithProperty() {
        PropertyState ps = mock(PropertyState.class);

        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.VERSION, pp);
        atp.canRead(ps);

        verify(pp, times(1)).isGranted(tree, ps, Permissions.READ_PROPERTY);
    }

    @Test
    public void testCanReadWithPropertyAcType() {
        PropertyState ps = mock(PropertyState.class);

        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.ACCESS_CONTROL, pp);
        atp.canRead(ps);

        verify(pp, times(1)).isGranted(tree, ps, Permissions.READ_ACCESS_CONTROL);
    }

    @Test
    public void testCanReadAll() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.DEFAULT, pp);
        assertFalse(atp.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.DEFAULT, pp);
        assertFalse(atp.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.ACCESS_CONTROL, pp);
        atp.isGranted(Permissions.ALL);

        verify(pp, times(1)).isGranted(tree, null, Permissions.ALL);
    }

    @Test
    public void testIsGrantedWithProperty() {
        PropertyState ps = mock(PropertyState.class);

        AbstractTreePermission atp = createAbstractTreePermission(tree, TreeType.VERSION, pp);
        atp.isGranted(Permissions.SET_PROPERTY|Permissions.VERSION_MANAGEMENT, ps);

        verify(pp, times(1)).isGranted(tree, ps, Permissions.SET_PROPERTY|Permissions.VERSION_MANAGEMENT);
        verify(pp, never()).isGranted(tree, null, Permissions.SET_PROPERTY|Permissions.VERSION_MANAGEMENT);
    }

}