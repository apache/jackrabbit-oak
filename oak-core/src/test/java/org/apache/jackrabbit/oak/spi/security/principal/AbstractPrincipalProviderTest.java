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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPrincipalProviderTest extends AbstractSecurityTest {

    protected PrincipalProvider principalProvider;

    @Override
    public void before() throws Exception {
        super.before();

        principalProvider = createPrincipalProvider();
    }

    protected abstract PrincipalProvider createPrincipalProvider();

    @Test
    public void testGetPrincipals() throws Exception {
        String adminId = adminSession.getAuthInfo().getUserID();
        Set<? extends Principal> principals = principalProvider.getPrincipals(adminId);

        assertNotNull(principals);
        assertFalse(principals.isEmpty());
        assertTrue(principals.contains(EveryonePrincipal.getInstance()));

        for (Principal principal : principals) {
            assertNotNull(principalProvider.getPrincipal(principal.getName()));
        }
    }

    @Test
    public void testUserPrincipal() throws Exception {
        User user = getTestUser();

        Principal principal = principalProvider.getPrincipal(user.getPrincipal().getName());

        assertNotNull(principal);
    }

    @Test
    public void testAdminPrincipal() throws Exception {
        String userId = adminSession.getAuthInfo().getUserID();
        Authorizable admin = getUserManager(root).getAuthorizable(userId);
        if (admin != null && admin.getPrincipal() instanceof AdminPrincipal) {

            Principal principal = principalProvider.getPrincipal(admin.getPrincipal().getName());
            assertTrue(principal instanceof AdminPrincipal);

            Set<? extends Principal> principals = principalProvider.getPrincipals(userId);
            boolean containsAdminPrincipal = false;
            for (Principal p : principals) {
                if (p instanceof AdminPrincipal) {
                    containsAdminPrincipal = true;
                    break;
                }
            }
            assertTrue(containsAdminPrincipal);
        }
    }

    @Test
    public void testSystemUserPrincipal() throws Exception {
        User user = getUserManager(root).createSystemUser("testSystemUser" + UUID.randomUUID(), null);
        root.commit();

        try {
            Principal principal = principalProvider.getPrincipal(user.getPrincipal().getName());

            assertNotNull(principal);
            assertTrue(principal instanceof SystemUserPrincipal);

        } finally {
            user.remove();
            root.commit();
        }
    }

    @Test
    public void testGroupPrincipal() throws Exception {
        Group group = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        root.commit();

        try {
            Principal principal = principalProvider.getPrincipal(group.getPrincipal().getName());

            assertNotNull(principal);
            assertTrue(principal instanceof java.security.acl.Group);

        } finally {
            group.remove();
            root.commit();
        }
    }

    @Test
    public void testEveryone() throws Exception {
        Principal everyone = principalProvider.getPrincipal(EveryonePrincipal.NAME);
        assertTrue(everyone instanceof EveryonePrincipal);

        Group everyoneGroup = null;
        try {
            UserManager userMgr = getUserManager(root);
            everyoneGroup = userMgr.createGroup(EveryonePrincipal.NAME);
            root.commit();

            Principal ep = principalProvider.getPrincipal(EveryonePrincipal.NAME);
            assertFalse(ep instanceof EveryonePrincipal);
        } finally {
            if (everyoneGroup != null) {
                everyoneGroup.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testFindUserPrincipal() throws Exception {
        User testUser = null;
        try {
            UserManager userMgr = getUserManager(root);
            testUser = userMgr.createUser("TestUser", "pw");
            root.commit();

            String principalName = testUser.getPrincipal().getName();
            assertNotNull(principalProvider.getPrincipal(principalName));

            List<String> nameHints = new ArrayList<String>();
            nameHints.add("TestUser");
            nameHints.add("Test");
            nameHints.add("User");
            nameHints.add("stUs");

            assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_NOT_GROUP, true);
            assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_ALL, true);
            assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_GROUP, false);
        } finally {
            if (testUser != null) {
                testUser.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testFindGroupPrincipal() throws Exception {
        Group testGroup = null;
        try {
            UserManager userMgr = getUserManager(root);
            testGroup = userMgr.createGroup("TestGroup");
            root.commit();

            String principalName = testGroup.getPrincipal().getName();
            assertNotNull(principalProvider.getPrincipal(principalName));

            List<String> nameHints = new ArrayList<String>();
            nameHints.add("TestGroup");
            nameHints.add("Test");
            nameHints.add("Group");
            nameHints.add("stGr");

            assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_GROUP, true);
            assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_ALL, true);
            assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_NOT_GROUP, false);
        } finally {
            if (testGroup != null) {
                testGroup.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testFindEveryone() {
        assertNotNull(principalProvider.getPrincipal(EveryonePrincipal.NAME));

        Map<Integer, Boolean> tests = new HashMap<Integer, Boolean>();
        tests.put(PrincipalManager.SEARCH_TYPE_ALL, Boolean.TRUE);
        tests.put(PrincipalManager.SEARCH_TYPE_GROUP, Boolean.TRUE);
        tests.put(PrincipalManager.SEARCH_TYPE_NOT_GROUP, Boolean.FALSE);

        for (Integer searchType : tests.keySet()) {
            boolean found = false;
            Iterator<? extends Principal> it = principalProvider.findPrincipals(EveryonePrincipal.NAME, searchType);
            while (it.hasNext()) {
                Principal p = it.next();
                if (p.getName().equals(EveryonePrincipal.NAME)) {
                    found = true;
                }
            }
            Boolean expected = tests.get(searchType);
            assertEquals(expected, found);
        }
    }

    @Test
    public void testFindEveryoneHint() {
        assertNotNull(principalProvider.getPrincipal(EveryonePrincipal.NAME));

        List<String> nameHints = new ArrayList<String>();
        nameHints.add("everyone");
        nameHints.add("every");
        nameHints.add("one");
        nameHints.add("very");

        assertResult(principalProvider, nameHints, EveryonePrincipal.NAME, PrincipalManager.SEARCH_TYPE_ALL, true);
        assertResult(principalProvider, nameHints, EveryonePrincipal.NAME, PrincipalManager.SEARCH_TYPE_GROUP, true);
        assertResult(principalProvider, nameHints, EveryonePrincipal.NAME, PrincipalManager.SEARCH_TYPE_NOT_GROUP, false);
    }

    @Test
    public void testFindWithoutHint() throws Exception {
        User testUser = null;
        Group testGroup = null;
        try {
            UserManager userMgr = getUserManager(root);
            testUser = userMgr.createUser("TestUser", "pw");
            testGroup = userMgr.createGroup("TestGroup");

            root.commit();

            Set<String> resultNames = new HashSet<String>();
            Iterator<? extends Principal> principals = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
            while (principals.hasNext()) {
                resultNames.add(principals.next().getName());
            }

            assertTrue(resultNames.contains(EveryonePrincipal.NAME));
            assertTrue(resultNames.contains("TestUser"));
            assertTrue(resultNames.contains("TestGroup"));

        } finally {
            if (testUser != null) {
                testUser.remove();
            }
            if (testGroup != null) {
                testGroup.remove();
            }
            root.commit();
        }
    }

    private static void assertResult(PrincipalProvider principalProvider,
                                     List<String> nameHints, String expectedName,
                                     int searchType, boolean toBeFound) {
        for (String nameHint : nameHints) {
            Iterator<? extends Principal> result = principalProvider.findPrincipals(nameHint, searchType);
            boolean found = false;
            while (result.hasNext()) {
                Principal p = result.next();
                if (p.getName().equals(expectedName)) {
                    found = true;
                }
            }
            if (toBeFound) {
                assertTrue("Expected principal to be found by name hint " + expectedName, found);
            } else {
                assertFalse("Expected principal NOT to be found by name hint " + expectedName, found);
            }
        }
    }
}
