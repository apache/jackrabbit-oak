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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeTreePermissionOrTest extends CompositeTreePermissionTest {

    @NotNull
    @Override
    CompositeAuthorizationConfiguration.CompositionType getCompositionType() {
        return CompositeAuthorizationConfiguration.CompositionType.OR;
    }

    @Test
    public void testIsGrantedUncoveredPermissions() {
        TreePermission mockTp = when(mock(TreePermission.class).isGranted(anyLong())).thenReturn(true).getMock();

        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.WRITE)).thenReturn(Permissions.SET_PROPERTY);

        TreePermission rootTp = createRootTreePermission(mockPP, mockPP);

        assertFalse(rootTp.isGranted(Permissions.WRITE));

        verify(mockTp, times(2)).isGranted(Permissions.ADD_PROPERTY);
        verify(mockTp, times(2)).isGranted(Permissions.MODIFY_PROPERTY);
        verify(mockTp, times(2)).isGranted(Permissions.REMOVE_PROPERTY);
        verify(mockTp, never()).isGranted(Permissions.SET_PROPERTY);
        verify(mockTp, never()).isGranted(Permissions.WRITE);
        verify(mockPP, times(2)).supportedPermissions(mockTp, null, Permissions.WRITE);
    }
}