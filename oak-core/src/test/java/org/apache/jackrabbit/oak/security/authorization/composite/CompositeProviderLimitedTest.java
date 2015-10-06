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

import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the combination of
 *
 * - default permission provider
 * - custom provider that grants JCR_NAMESPACE_MANAGEMENT on repository level
 *   and REP_READ_NODES only
 *
 * both for the set of principals associated with the test user and with the admin session.
 * The expected outcome is that
 * - test user can only read nodes where this is also granted by the default provider
 *   but has no other access granted
 * - admin user can only read nodes and register namespaces
 */
public class CompositeProviderLimitedTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cppTestUser;
    private CompositePermissionProvider cppAdminUser;

    @Override
    public void before() throws Exception {
        super.before();

        cppTestUser = createPermissionProvider(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        cppAdminUser = createPermissionProvider(root.getContentSession().getAuthInfo().getPrincipals());
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new TestPermissionProvider(root, true);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        // TODO
    }


    @Test
    public void testHasPrivileges() throws Exception {
        // TODO
    }


    @Test
    public void testIsGranted() throws Exception {
        // TODO
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        // TODO
    }

    @Test
    public void testRepositoryPermissionIsGranted() throws Exception {
        RepositoryPermission rp = cppTestUser.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testRepositoryPermissionIsGrantedAdminUser() throws Exception {
        RepositoryPermission rp = cppAdminUser.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.ALL));
    }

    @Test
    public void testGetTreePermission() throws Exception {
        // TODO
    }
}