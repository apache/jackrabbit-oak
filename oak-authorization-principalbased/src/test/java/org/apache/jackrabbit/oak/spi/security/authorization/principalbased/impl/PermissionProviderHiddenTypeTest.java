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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.mockReadOnlyTree;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PermissionProviderHiddenTypeTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedPermissionProvider permissionProvider;

    @Before
    public void before() throws Exception {
        super.before();

        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
    }


    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @Test
    public void testGetPrivileges() {
        assertTrue(permissionProvider.getPrivileges(mockReadOnlyTree(TreeType.HIDDEN)).isEmpty());
    }

    @Test
    public void testHasPrivileges() {
        assertFalse(permissionProvider.hasPrivileges(mockReadOnlyTree(TreeType.HIDDEN), PrivilegeConstants.REP_READ_NODES));
    }

    @Test
    public void testIsGranted() {
        assertTrue(permissionProvider.isGranted(mockReadOnlyTree(TreeType.HIDDEN), null, Permissions.ALL));
        assertTrue(permissionProvider.isGranted(mockReadOnlyTree(TreeType.HIDDEN), mock(PropertyState.class), Permissions.ALL));
    }
    @Test
    public void testGetTreePermission() throws Exception {
        assertSame(TreePermission.ALL, permissionProvider.getTreePermission(mockReadOnlyTree(TreeType.HIDDEN), TreeType.HIDDEN, mock(AbstractTreePermission.class)));
    }

    @Test
    public void testGetChildTreePermission() {
        String indexPath = "/" + IndexConstants.INDEX_DEFINITIONS_NAME + "/acPrincipalName/" + IndexConstants.INDEX_CONTENT_NODE_NAME;
        Tree readOnly = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = (AbstractTreePermission) permissionProvider.getTreePermission(readOnly, TreePermission.EMPTY);
        NodeState ns = getTreeProvider().asNodeState(readOnly);
        for (String elem : PathUtils.elements(indexPath)) {
            ns = ns.getChildNode(elem);
            tp = permissionProvider.getTreePermission(elem, ns, (AbstractTreePermission) tp);
        }
        assertSame(TreePermission.ALL, tp);
    }
}