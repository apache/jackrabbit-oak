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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.NT_REP_PERMISSIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.NT_REP_PERMISSION_STORE;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_ACCESS_CONTROLLED_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_PERMISSION_STORE;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_PRIVILEGE_BITS;
import static org.junit.Assert.assertEquals;

public class PermissionConstantsTest {

    @Test
    public void testNtNames() {
        assertEquals(ImmutableSet.of(NT_REP_PERMISSIONS, NT_REP_PERMISSION_STORE), PermissionConstants.PERMISSION_NODETYPE_NAMES);
    }

    @Test
    public void testNodeNames() {
        assertEquals(ImmutableSet.of(REP_PERMISSION_STORE), PermissionConstants.PERMISSION_NODE_NAMES);
    }

    @Test
    public void testPropertyNames() {
        assertEquals(ImmutableSet.of(REP_ACCESS_CONTROLLED_PATH, REP_PRIVILEGE_BITS), PermissionConstants.PERMISSION_PROPERTY_NAMES);
    }

    @Test
    public void testJr2Permissions() {
        assertEquals(Permissions.USER_MANAGEMENT|Permissions.REMOVE_NODE, Permissions.getPermissions(PermissionConstants.VALUE_PERMISSIONS_JR2));
    }

    @Test
    public void testDefaultReadPaths() {
        assertEquals(ImmutableSet.of(
                NamespaceConstants.NAMESPACES_PATH,
                NodeTypeConstants.NODE_TYPES_PATH,
                PrivilegeConstants.PRIVILEGES_PATH), PermissionConstants.DEFAULT_READ_PATHS);
    }
}