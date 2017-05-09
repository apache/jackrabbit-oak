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
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.security.principal.AbstractPrincipalProviderTest;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class UserPrincipalProviderTest extends AbstractPrincipalProviderTest {

    @Override
    protected PrincipalProvider createPrincipalProvider() {
        return new UserPrincipalProvider(root, getUserConfiguration(), namePathMapper);
    }

    @Test
    public void testTreeBasedUserPrincipal() throws Exception {
        User user = getTestUser();
        Principal principal = principalProvider.getPrincipal(user.getPrincipal().getName());
        assertTrue(principal instanceof TreeBasedPrincipal);
    }

    @Test
    public void testTreeBasedSystemUserPrincipal() throws Exception {
        User systemUser = getUserManager(root).createSystemUser("systemUser" + UUID.randomUUID(), null);
        root.commit();

        try {
            Principal principal = principalProvider.getPrincipal(systemUser.getPrincipal().getName());
            assertTrue(principal instanceof SystemUserPrincipalImpl);
        } finally {
            systemUser.remove();
            root.commit();
        }
    }

    @Test
    public void testTreeBasedGroupPrincipal() throws Exception {
        Group group = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        root.commit();

        try {
            Principal principal = principalProvider.getPrincipal(group.getPrincipal().getName());
            assertTrue(principal instanceof AbstractGroupPrincipal);
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void testGetPrincipalsForUser() throws Exception {
        Group group = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        group.addMember(getTestUser());
        root.commit();

        try {
            Set<? extends Principal> principals = principalProvider.getPrincipals(getTestUser().getID());
            for (Principal p : principals) {
                String name = p.getName();
                if (name.equals(getTestUser().getPrincipal().getName())) {
                    assertTrue(p instanceof TreeBasedPrincipal);
                } else if (!EveryonePrincipal.NAME.equals(name)) {
                    assertTrue(p instanceof AbstractGroupPrincipal);
                }
            }
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void testGetPrincipalsForSystemUser() throws Exception {
        User systemUser = getUserManager(root).createSystemUser("systemUser" + UUID.randomUUID(), null);
        Group group = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        group.addMember(systemUser);
        root.commit();

        try {
            Set<? extends Principal> principals = principalProvider.getPrincipals(systemUser.getID());
            for (Principal p : principals) {
                String name = p.getName();
                if (name.equals(systemUser.getPrincipal().getName())) {
                    assertTrue(p instanceof SystemUserPrincipalImpl);
                } else if (!EveryonePrincipal.NAME.equals(name)) {
                    assertTrue(p instanceof AbstractGroupPrincipal);
                }
            }
        } finally {
            systemUser.remove();
            group.remove();
            root.commit();
        }
    }

    @Test
    public void testGetPrincipalsForAdminUser() throws Exception {
        Authorizable adminUser = getUserManager(root).getAuthorizable(adminSession.getAuthInfo().getUserID());
        if (adminUser != null && adminUser.getPrincipal() instanceof AdminPrincipal) {
            Set<? extends Principal> principals = principalProvider.getPrincipals(adminUser.getID());
            for (Principal p : principals) {
                String name = p.getName();
                if (name.equals(adminUser.getPrincipal().getName())) {
                    assertTrue(p instanceof AdminPrincipalImpl);
                } else if (!EveryonePrincipal.NAME.equals(name)) {
                    assertTrue(p instanceof AbstractGroupPrincipal);
                }
            }
        }
    }

    @Test
    public void testEveryoneMembers() throws Exception {
        Principal everyone = principalProvider.getPrincipal(EveryonePrincipal.NAME);
        assertTrue(everyone instanceof EveryonePrincipal);

        Group everyoneGroup = null;
        try {
            UserManager userMgr = getUserManager(root);
            everyoneGroup = userMgr.createGroup(EveryonePrincipal.NAME);
            root.commit();

            Principal ep = principalProvider.getPrincipal(EveryonePrincipal.NAME);

            assertTrue(ep instanceof java.security.acl.Group);
            ((java.security.acl.Group) ep).members();
            ((java.security.acl.Group) ep).isMember(getTestUser().getPrincipal());

        } finally {
            if (everyoneGroup != null) {
                everyoneGroup.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testGroupMembers() throws Exception {
        Group group = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        group.addMember(getTestUser());
        root.commit();

        try {
            Principal principal = principalProvider.getPrincipal(group.getPrincipal().getName());

            assertTrue(principal instanceof java.security.acl.Group);

            boolean found = false;
            Enumeration<? extends Principal> members = ((java.security.acl.Group) principal).members();
            while (members.hasMoreElements() && !found) {
                found = members.nextElement().equals(getTestUser().getPrincipal());
            }
            assertTrue(found);
        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void testGroupIsMember() throws Exception {
        Group group = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        group.addMember(getTestUser());
        root.commit();

        try {
            Principal principal = principalProvider.getPrincipal(group.getPrincipal().getName());

            assertTrue(principal instanceof java.security.acl.Group);
            ((java.security.acl.Group) principal).isMember(getTestUser().getPrincipal());
        } finally {
            group.remove();
            root.commit();
        }
    }
}
