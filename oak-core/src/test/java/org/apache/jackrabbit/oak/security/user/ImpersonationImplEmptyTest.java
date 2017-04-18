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

import java.security.Principal;
import java.util.Enumeration;
import java.util.UUID;

import javax.security.auth.Subject;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ImpersonationImplEmptyTest extends AbstractSecurityTest {

    final java.security.acl.Group groupPrincipal = new java.security.acl.Group() {
        @Override
        public boolean addMember(Principal user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeMember(Principal user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMember(Principal member) {
            throw new UnsupportedOperationException();
        }

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

    @Test
    public void testGetImpersonators() throws Exception {
        assertFalse(impersonation.getImpersonators().hasNext());
    }

    @Test
    public void testGrantNonExistingPrincipal() throws Exception {
        assertFalse(impersonation.grantImpersonation(new PrincipalImpl("principal" + UUID.randomUUID())));
    }

    @Test
    public void testGrantAdminPrincipal() throws Exception {
        assertFalse(impersonation.grantImpersonation(new AdminPrincipal() {
            @Override
            public String getName() {
                return "name";
            }
        }));
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
        assertFalse(impersonation.grantImpersonation(new Principal() {

            @Override
            public String getName() {
                return principal.getName();
            }
        }));
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
    public void testAllowsNull() throws Exception {
        assertFalse(impersonation.allows(null));
    }

    @Test
    public void testAllowsEmpty() throws Exception {
        assertFalse(impersonation.allows(new Subject()));
    }

    @Test
    public void testAllowsGroup() throws Exception {
        assertFalse(impersonation.allows(new Subject(true, ImmutableSet.of(groupPrincipal), ImmutableSet.of(), ImmutableSet.of())));
    }

    @Test
    public void testAllowsAdminPrincipal() throws Exception {
        Subject subject = new Subject(true, ImmutableSet.of(getAdminPrincipal()), ImmutableSet.of(), ImmutableSet.of());
        assertTrue(impersonation.allows(subject));
    }

    @Test
    public void testAllowsAdminPrincipal2() throws Exception {
        Subject subject = new Subject(true, ImmutableSet.of(new AdminPrincipal() {
            @Override
            public String getName() {
                return "principalName";
            }
        }), ImmutableSet.of(), ImmutableSet.of());
        assertTrue(impersonation.allows(subject));
    }

    @Test
    public void testAllowsSystemPrincipal() throws Exception {
        Subject subject = new Subject(true, ImmutableSet.of(SystemPrincipal.INSTANCE), ImmutableSet.of(), ImmutableSet.of());
        assertFalse(impersonation.allows(subject));
    }
}