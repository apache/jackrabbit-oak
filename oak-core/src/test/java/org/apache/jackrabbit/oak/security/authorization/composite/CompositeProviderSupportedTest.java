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
package org.apache.jackrabbit.oak.security.authorization.composite;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;
import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.OR;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.READ_NODE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeProviderSupportedTest extends AbstractSecurityTest {

    private long[] supportedPermissions = new long[] {READ_NODE, READ_NODE, Permissions.NO_PERMISSION, READ_ACCESS_CONTROL};
    private AggregatedPermissionProvider[] pps = new AggregatedPermissionProvider[] {
            mock(AggregatedPermissionProvider.class),
            mock(AggregatedPermissionProvider.class),
            mock(AggregatedPermissionProvider.class),
            mock(AggregatedPermissionProvider.class)
    };

    private CompositePermissionProvider cppAND;
    private CompositePermissionProvider cppOR;

    private long expectedPermissions = READ_NODE|READ_ACCESS_CONTROL;
    private PrivilegeBits expectedBits;

    private PrivilegeBitsProvider pbp;

    @Override
    public void before() throws Exception {
        super.before();

        pbp = new PrivilegeBitsProvider(root);
        for (int i = 0; i < pps.length; i++) {
            when(pps[i].supportedPermissions(any(Tree.class), any(PropertyState.class), anyLong())).thenReturn(supportedPermissions[i]);
            when(pps[i].supportedPermissions(any(Tree.class), isNull(), anyLong())).thenReturn(supportedPermissions[i]);
            when(pps[i].supportedPermissions((Tree) isNull(), isNull(), anyLong())).thenReturn(supportedPermissions[i]);
            when(pps[i].supportedPermissions(any(TreePermission.class), any(PropertyState.class), anyLong())).thenReturn(supportedPermissions[i]);
            when(pps[i].supportedPermissions(any(TreePermission.class), isNull(), anyLong())).thenReturn(supportedPermissions[i]);
            when(pps[i].supportedPermissions(any(TreeLocation.class), anyLong())).thenReturn(supportedPermissions[i]);

            PropertyState ps = PropertyStates.createProperty("any", supportedPermissions[i], Type.LONG);
            PrivilegeBits bts = (supportedPermissions[i] == Permissions.NO_PERMISSION) ? PrivilegeBits.EMPTY : PrivilegeBits.getInstance(ps);
            when(pps[i].supportedPrivileges(any(Tree.class), any(PrivilegeBits.class))).thenReturn(bts);
            when(pps[i].supportedPrivileges(isNull(), any(PrivilegeBits.class))).thenReturn(bts);
        }

        cppAND = createProvider(AND, pps);
        cppOR = createProvider(OR, pps);

        expectedBits = PrivilegeBits.getInstance(PropertyStates.createProperty("any", expectedPermissions, Type.LONG)).unmodifiable();
    }

    private CompositePermissionProvider createProvider(@NotNull CompositeAuthorizationConfiguration.CompositionType compositionType, @NotNull AggregatedPermissionProvider... aggregated) {
        return CompositePermissionProvider.create(root, ImmutableList.copyOf(aggregated), Context.DEFAULT, compositionType, getRootProvider(), getTreeProvider());
    }

    @Test
    public void testSupportedPermissionsFromTree() {
        Tree tree = mock(Tree.class);
        PropertyState ps = mock(PropertyState.class);

        for (CompositePermissionProvider ccp : new CompositePermissionProvider[] {cppAND, cppOR}) {
            assertEquals(expectedPermissions, ccp.supportedPermissions((Tree) null, null, Permissions.ALL));
            assertEquals(expectedPermissions, ccp.supportedPermissions(tree, null, Permissions.ALL));
            assertEquals(expectedPermissions, ccp.supportedPermissions(tree, ps, Permissions.ALL));
        }
    }

    @Test
    public void testSupportedPermissionsFromTreePermission() {
        TreePermission tp = mock(TreePermission.class);
        PropertyState ps = mock(PropertyState.class);

        for (CompositePermissionProvider ccp : new CompositePermissionProvider[] {cppAND, cppOR}) {
            assertEquals(expectedPermissions, ccp.supportedPermissions(tp, null, Permissions.ALL));
            assertEquals(expectedPermissions, ccp.supportedPermissions(tp, ps, Permissions.ALL));
        }
    }

    @Test
    public void testSupportedPermissionsFromLocation() {
        TreeLocation location = TreeLocation.create(root, PathUtils.concat(PathUtils.ROOT_PATH, "any"));

        for (CompositePermissionProvider ccp : new CompositePermissionProvider[] {cppAND, cppOR}) {
            assertEquals(expectedPermissions, ccp.supportedPermissions(location, Permissions.ALL));
            assertEquals(expectedPermissions, ccp.supportedPermissions(location, Permissions.ALL));
        }
    }

    @Test
    public void testSupportedPrivilegeBits() {
        PrivilegeBits all = pbp.getBits(PrivilegeConstants.JCR_ALL);
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        for (CompositePermissionProvider ccp : new CompositePermissionProvider[] {cppAND, cppOR}) {
            assertEquals(expectedBits, ccp.supportedPrivileges(null, all).unmodifiable());
            assertEquals(expectedBits, ccp.supportedPrivileges(tree, all).unmodifiable());
        }
    }
}