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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ImpersonationImplEmptyTest extends AbstractSecurityTest {

    final GroupPrincipal groupPrincipal = new GroupPrincipal() {
        @Override
        public boolean isMember(@NotNull Principal member) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Enumeration<? extends Principal> members() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return "name";
        }
    };

    UserImpl user;
    ImpersonationImpl impersonation;

    @Override
    public void before() throws Exception {
        super.before();

        String uid = "u" + UUID.randomUUID();
        user = (UserImpl) getUserManager(root).createUser(uid, uid);
        root.commit();

        impersonation = new ImpersonationImpl(user);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            user.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private Principal getAdminPrincipal() throws Exception {
        String id = getConfig(UserConfiguration.class).getParameters().getConfigValue(UserConstants.PARAM_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID);
        User adminUser = getUserManager(root).getAuthorizable(id, User.class);
        assertNotNull(adminUser);

        return adminUser.getPrincipal();
    }

    @NotNull
    static Subject createSubject(@NotNull Principal... principals) {
        return new Subject(true, Set.of(principals), Set.of(), Set.of());
    }

    @Test
    public void testGetImpersonators() throws Exception {
        assertFalse(impersonation.getImpersonators().hasNext());
    }

    @Test
    public void testGrantNonExistingPrincipal() throws Exception {
        assertFalse(impersonation.grantImpersonation(new PrincipalImpl("principal" + UUID.randomUUID())));
    }

    @Test
    public void testGrantNonExistingTreeBasedPrincipal() throws Exception {
        TreeBasedPrincipal tbPrincipal = new TreeBasedPrincipal("name", "/nonExisting", getNamePathMapper());
        assertFalse(impersonation.grantImpersonation(tbPrincipal));
    }

    @Test
    public void testGrantNonAuthorizableTreeBasedPrincipal() throws Exception {
        TreeBasedPrincipal tbPrincipal = new TreeBasedPrincipal("name", PathUtils.ROOT_PATH, getNamePathMapper());
        assertFalse(impersonation.grantImpersonation(tbPrincipal));
    }

    @Test
    public void testGrantAdminPrincipal() throws Exception {
        assertFalse(impersonation.grantImpersonation((AdminPrincipal) () -> "name"));
    }

    @Test
    public void testGrantAdminPrincipal2() throws Exception {
        assertFalse(impersonation.grantImpersonation(getAdminPrincipal()));
    }

    @Test
    public void testGrantAdminPrincipal3() throws Exception {
        assertFalse(impersonation.grantImpersonation(new PrincipalImpl(getAdminPrincipal().getName())));
    }

    @Test
    public void testGrantSystemPrincipal() throws Exception {
        assertFalse(impersonation.grantImpersonation(SystemPrincipal.INSTANCE));
    }

    @Test
    public void testGrantGroupPrincipal() throws Exception {
        Group group = getUserManager(root).createGroup("testGroup");
        try {
            assertFalse(impersonation.grantImpersonation(group.getPrincipal()));
        } finally {
            group.remove();
        }
    }

    @Test
    public void testGrantNonExistingGroupPrincipal() throws Exception {
        assertFalse(impersonation.grantImpersonation(groupPrincipal));
    }

    @Test
    public void testGrantExistingUserPrincipal() throws Exception {
        assertTrue(impersonation.grantImpersonation(getTestUser().getPrincipal()));
    }

    @Test
    public void testGrantAgain() throws Exception {
        final Principal principal = getTestUser().getPrincipal();
        impersonation.grantImpersonation(principal);

        assertFalse(impersonation.grantImpersonation(principal));
        assertFalse(impersonation.grantImpersonation(new PrincipalImpl(principal.getName())));
        assertFalse(impersonation.grantImpersonation(() -> principal.getName()));
    }

    @Test
    public void testGrantSelf() throws Exception {
        assertFalse(impersonation.grantImpersonation(user.getPrincipal()));
    }

    @Test
    public void testRevokeNotGranted() throws Exception {
        assertFalse(impersonation.revokeImpersonation(getTestUser().getPrincipal()));
    }

    @Test
    public void testAllowsNull() {
        assertFalse(impersonation.allows(null));
    }

    @Test
    public void testAllowsEmpty() {
        assertFalse(impersonation.allows(new Subject()));
    }

    @Test
    public void testAllowsGroup() {
        assertFalse(impersonation.allows(createSubject(groupPrincipal)));
    }

    @Test
    public void testAllowsAdminPrincipal() throws Exception {
        assertTrue(impersonation.allows(createSubject(getAdminPrincipal())));
    }

    @Test
    public void testAllowsAdminPrincipal2() {
        assertTrue(impersonation.allows(createSubject((AdminPrincipal) () -> "principalName")));
    }

    @Test
    public void testAllowsAdminPrincipal3() throws Exception {
        assertTrue(impersonation.allows(createSubject(new PrincipalImpl(getAdminPrincipal().getName()))));
    }

    @Test
    public void testAllowsSystemPrincipal() {
        assertFalse(impersonation.allows(createSubject(SystemPrincipal.INSTANCE)));
    }

    @Test
    public void testAllowsNonExistingPrincipal() {
        assertFalse(impersonation.allows(createSubject(new PrincipalImpl("nonExisting"))));
    }
}