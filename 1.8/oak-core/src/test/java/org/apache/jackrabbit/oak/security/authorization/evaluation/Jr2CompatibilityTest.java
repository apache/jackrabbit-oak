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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test compatibility with Jackrabbit 2.x using the
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants#PARAM_PERMISSIONS_JR2} configuration parameter.
 */
public class Jr2CompatibilityTest extends AbstractOakCoreTest {

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        setupPermission("/", getTestUser().getPrincipal(), true, PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            AccessControlManager acMgr = getAccessControlManager(root);
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            if (acl != null) {
                boolean modified = false;
                for (AccessControlEntry entry : acl.getAccessControlEntries()) {
                    if (entry.getPrincipal().equals(getTestUser().getPrincipal())) {
                        acl.removeAccessControlEntry(entry);
                        modified = true;
                    }
                }
                if (modified) {
                    acMgr.setPolicy("/", acl);
                    root.commit();
                }
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters acConfig = ConfigurationParameters.of(
                PermissionConstants.PARAM_PERMISSIONS_JR2, PermissionConstants.VALUE_PERMISSIONS_JR2);

        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, acConfig);
    }

    @Test
    public void testUserManagementPermissionWithJr2Flag() throws Exception {
        Root testRoot = getTestRoot();
        testRoot.refresh();

        UserManager testUserMgr = getUserConfiguration().getUserManager(testRoot, NamePathMapper.DEFAULT);
        try {
            User u = testUserMgr.createUser("a", "b");
            testRoot.commit();

            u.changePassword("c");
            testRoot.commit();

            u.remove();
            testRoot.commit();
        } finally {
            root.refresh();
            Authorizable user = getUserManager(root).getAuthorizable("a");
            if (user != null) {
                user.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testRemoveNodeWithJr2Flag() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        /* deny REMOVE_NODE privilege at subtree. */
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.JCR_REMOVE_NODE);

        Root testRoot = getTestRoot();
        AccessControlManager acMgr = getAccessControlManager(testRoot);
        assertTrue(acMgr.hasPrivileges("/a", privilegesFromNames(PrivilegeConstants.REP_WRITE)));
        assertFalse(acMgr.hasPrivileges("/a/b", privilegesFromNames(PrivilegeConstants.JCR_REMOVE_NODE)));

        // removing the tree must fail
        try {
            testRoot.getTree("/a").remove();
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
        }
    }

    @Test
    public void testRemoveNodeWithJr2Flag2() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        /* deny REP_REMOVE_PROPERTIES privilege at subtree. */
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.REP_REMOVE_PROPERTIES);

        Root testRoot = getTestRoot();
        AccessControlManager acMgr = getAccessControlManager(testRoot);
        assertTrue(acMgr.hasPrivileges("/a", privilegesFromNames(PrivilegeConstants.REP_WRITE)));
        assertFalse(acMgr.hasPrivileges("/a/b", privilegesFromNames(PrivilegeConstants.REP_REMOVE_PROPERTIES)));

        // removing the tree must fail
        try {
            testRoot.getTree("/a").remove();
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
        }
    }
}