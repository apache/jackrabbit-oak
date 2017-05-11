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
package org.apache.jackrabbit.oak.security.user.action;

import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the {@code AccessControlAction} with complete security setup
 */
public class AccessControlActionTest extends AbstractSecurityTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userConfig = ConfigurationParameters.of(
                AccessControlAction.GROUP_PRIVILEGE_NAMES, new String[] {PrivilegeConstants.JCR_READ},
                AccessControlAction.USER_PRIVILEGE_NAMES, new String[] {PrivilegeConstants.JCR_ALL}
        );
        ConfigurationParameters authorizationConfig = ConfigurationParameters.of(
                PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, new String[] {"administrativePrincipalName"}
        );
        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig, AuthorizationConfiguration.NAME, authorizationConfig);
    }

    @Test
    public void testAccessControlActionForUser() throws Exception {
        UserManager userMgr = getUserManager(root);
        User u = null;
        try {
            String uid = "actionTestUser";
            u = userMgr.createUser(uid, uid);
            root.commit();

            assertAcAction(u, PrivilegeConstants.JCR_ALL);
        } finally {
            root.refresh();
            if (u != null) {
                u.remove();
            }
            root.commit();
        }
    }

    @Test
    public void testAccessControlAction() throws Exception {
        UserManager userMgr = getUserManager(root);
        Group gr = null;
        try {
            gr = userMgr.createGroup("actionTestGroup");
            root.commit();

            assertAcAction(gr, PrivilegeConstants.JCR_READ);
        } finally {
            root.refresh();
            if (gr != null) {
                gr.remove();
            }
            root.commit();
        }
    }

    @Test
    public void testAdministrativePrincipals() throws Exception {
        UserManager userMgr = getUserManager(root);
        Group gr = null;
        try {
            gr = userMgr.createGroup("administrativePrincipalName");
            root.commit();

            AccessControlManager acMgr = getAccessControlManager(root);
            AccessControlPolicy[] policies = acMgr.getPolicies(gr.getPath());
            assertEquals(0, policies.length);
        } finally {
            root.refresh();
            if (gr != null) {
                gr.remove();
            }
            root.commit();
        }
    }

    private void assertAcAction(Authorizable a, String expectedPrivName) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlPolicy[] policies = acMgr.getPolicies(a.getPath());
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof AccessControlList);
        AccessControlList acl = (AccessControlList) policies[0];
        assertEquals(1, acl.getAccessControlEntries().length);
        assertArrayEquals(new Privilege[]{getPrivilegeManager(root).getPrivilege(expectedPrivName)}, acl.getAccessControlEntries()[0].getPrivileges());
    }
}