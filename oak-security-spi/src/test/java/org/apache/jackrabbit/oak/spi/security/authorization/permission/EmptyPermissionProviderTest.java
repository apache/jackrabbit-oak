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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class EmptyPermissionProviderTest {

    private final PermissionProvider emptyProvider = EmptyPermissionProvider.getInstance();

    private final Tree tree = Mockito.mock(Tree.class);

    @Test
    public void testGetPrivileges() {
        assertTrue(emptyProvider.getPrivileges(tree).isEmpty());
    }

    @Test
    public void testHasPrivileges() {
        assertFalse(emptyProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
    }

    @Test
    public void testGetRepositoryPermission() {
        assertSame(RepositoryPermission.EMPTY, emptyProvider.getRepositoryPermission());
    }

    @Test
    public void testGetTreePermission() {
        assertSame(TreePermission.EMPTY, emptyProvider.getTreePermission(tree, TreePermission.EMPTY));
    }

    @Test
    public void testIsGranted() {
        assertFalse(emptyProvider.isGranted(tree, null, Permissions.READ));
        assertFalse(emptyProvider.isGranted(tree, PropertyStates.createProperty("prop", "value"), Permissions.READ));
    }

    @Test
    public void testIsGrantedActions() {
        assertFalse(emptyProvider.isGranted("/", Session.ACTION_READ));
    }
}