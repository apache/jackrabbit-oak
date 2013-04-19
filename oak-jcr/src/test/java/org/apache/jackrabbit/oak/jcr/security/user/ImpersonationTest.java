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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import java.util.Collections;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the {@link Impersonation} implementation.
 */
public class ImpersonationTest extends AbstractUserTest {

    private User user2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        user2 = userMgr.createUser("user2", "pw");
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            user2.remove();
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testImpersonation() throws RepositoryException, NotExecutableException {
        Principal user2Principal = user2.getPrincipal();
        Subject subject = new Subject(true, Collections.singleton(user2Principal), Collections.<Object>emptySet(), Collections.<Object>emptySet());

        Impersonation impers = user.getImpersonation();
        assertFalse(impers.allows(subject));

        assertTrue(impers.grantImpersonation(user2Principal));
        assertFalse(impers.grantImpersonation(user2Principal));
        superuser.save();

        assertTrue(impers.allows(subject));

        assertTrue(impers.revokeImpersonation(user2Principal));
        assertFalse(impers.revokeImpersonation(user2Principal));
        superuser.save();

        assertFalse(impers.allows(subject));
    }

    @Test
    public void testAdminAsImpersonator() throws RepositoryException, NotExecutableException {
        String adminId = superuser.getUserID();
        Authorizable admin = userMgr.getAuthorizable(adminId);
        if (admin == null || admin.isGroup() || !((User) admin).isAdmin()) {
            throw new NotExecutableException(adminId + " is not administators ID");
        }

        Principal adminPrincipal = admin.getPrincipal();

        // admin cannot be add/remove to set of impersonators of 'u' but is
        // always allowed to impersonate that user.
        Impersonation impersonation = user.getImpersonation();

        assertFalse(impersonation.grantImpersonation(adminPrincipal));
        assertFalse(impersonation.revokeImpersonation(adminPrincipal));
        assertTrue(impersonation.allows(buildSubject(adminPrincipal)));

        // same if the impersonation object of the admin itself is used.
        Impersonation adminImpersonation = ((User) admin).getImpersonation();

        assertFalse(adminImpersonation.grantImpersonation(adminPrincipal));
        assertFalse(adminImpersonation.revokeImpersonation(adminPrincipal));
        assertTrue(impersonation.allows(buildSubject(adminPrincipal)));
    }

    public void testAdminPrincipalAsImpersonator() throws RepositoryException, NotExecutableException {

        Principal adminPrincipal = new AdminPrincipal() {
            @Override
            public String getName() {
                return "some-admin-name";
            }
        };

        // admin cannot be add/remove to set of impersonators of 'u' but is
        // always allowed to impersonate that user.
        Impersonation impersonation = user.getImpersonation();

        assertFalse(impersonation.grantImpersonation(adminPrincipal));
        assertFalse(impersonation.revokeImpersonation(adminPrincipal));
        assertTrue(impersonation.allows(buildSubject(adminPrincipal)));
    }
}