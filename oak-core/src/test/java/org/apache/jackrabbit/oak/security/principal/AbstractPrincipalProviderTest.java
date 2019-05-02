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
package org.apache.jackrabbit.oak.security.principal;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPrincipalProviderTest extends AbstractSecurityTest {

    protected PrincipalProvider principalProvider;

    private Principal userPrincipal;
    private Principal nonExisting;

    protected  String groupId;
    protected Group testGroup;

    protected String groupId2;
    protected Group testGroup2;

    protected String groupId3;
    protected Group testGroup3;

    @Override
    public void before() throws Exception {
        // because of full text search test #testFindRange
        getQueryEngineSettings().setFailTraversal(false);
        getQueryEngineSettings().setFullTextComparisonWithoutIndex(true);
        super.before();

        userPrincipal = getTestUser().getPrincipal();
        nonExisting = new PrincipalImpl("nonExisting");

        groupId = "testGroup" + UUID.randomUUID();
        testGroup = getUserManager(root).createGroup(groupId);
        testGroup.addMember(getTestUser());

        groupId2 = "testGroup" + UUID.randomUUID() + "2";
        testGroup2 = getUserManager(root).createGroup(groupId2);
        testGroup.addMember(testGroup2);

        groupId3 = "testGroup" + UUID.randomUUID() + "3";
        testGroup3 = getUserManager(root).createGroup(groupId3);

        root.commit();

        principalProvider = createPrincipalProvider();
        assertNull(principalProvider.getPrincipal(nonExisting.getName()));
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            String[] rm = new String[] { groupId, groupId2, groupId3 };
            for (String id : rm) {
                Group gr = getUserManager(root).getAuthorizable(id, Group.class);
                if (gr != null) {
                    gr.remove();
                    root.commit();
                }
            }
        } finally {
            super.after();
        }
    }

    @NotNull
    protected abstract PrincipalProvider createPrincipalProvider();

    @Test
    public void testGetPrincipals() {
        String adminId = adminSession.getAuthInfo().getUserID();
        Set<? extends Principal> principals = principalProvider.getPrincipals(adminId);

        assertNotNull(principals);
        assertFalse(principals.isEmpty());
        assertTrue(principals.contains(EveryonePrincipal.getInstance()));
        assertEquals(adminSession.getAuthInfo().getPrincipals(), principals);

        for (Principal principal : principals) {
            assertNotNull(principalProvider.getPrincipal(principal.getName()));
        }
    }

    @Test
    public void testGetPrincipalsNonExistingId() throws Exception {
        assertNull(getUserManager(root).getAuthorizable("nonExisting"));
        assertTrue(principalProvider.getPrincipals("nonExisting").isEmpty());
    }

    @Test
    public void testGetPrincipalsForGroup() throws Exception {
        Authorizable group = null;
        try {
            group = getUserManager(root).createGroup("testGroup");
            root.commit();

            assertTrue(principalProvider.getPrincipals(group.getID()).isEmpty());
        } finally {
            if (group != null) {
                group.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testGetItemBasedPrincipal() throws Exception {
        assertTrue(userPrincipal instanceof ItemBasedPrincipal);
        String jcrPath = ((ItemBasedPrincipal) userPrincipal).getPath();
        assertEquals(userPrincipal, principalProvider.getItemBasedPrincipal(getNamePathMapper().getOakPath(jcrPath)));
    }

    @Test
    public void testGetItemBasedGroupPrincipal() throws Exception {
        String jcrPath = testGroup.getPath();
        assertEquals(testGroup.getPrincipal(), principalProvider.getItemBasedPrincipal(getNamePathMapper().getOakPath(jcrPath)));
    }

    @Test
    public void testGetItemBasedPrincipalRoundTrip() throws Exception {
        Principal principal = principalProvider.getPrincipal(testGroup2.getPrincipal().getName());
        assertTrue(principal instanceof ItemBasedPrincipal);

        String jcrPath = ((ItemBasedPrincipal) principal).getPath();
        assertEquals(principal, principalProvider.getItemBasedPrincipal(jcrPath));
    }

    @Test
    public void testGetItemBasedPrincipalPropertyPath() throws Exception {
        String propPath = PathUtils.concat(((ItemBasedPrincipal) userPrincipal).getPath(), UserConstants.REP_PRINCIPAL_NAME);
        assertNull(principalProvider.getItemBasedPrincipal(getNamePathMapper().getOakPath(propPath)));
    }

    @Test
    public void testGetItemBasedPrincipalNonExisting() throws Exception {
        assertNull(principalProvider.getItemBasedPrincipal(UserConstants.DEFAULT_GROUP_PATH));
    }

    @Test
    public void testUserPrincipal() throws Exception {
        Principal principal = principalProvider.getPrincipal(userPrincipal.getName());

        assertNotNull(principal);
    }

    @Test
    public void testNonExistingPrincipal() throws Exception {
        assertNull(principalProvider.getPrincipal(nonExisting.getName()));
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
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());

        assertNotNull(principal);
        assertTrue(principal instanceof GroupPrincipal);
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
    public void testGetGroupMembership() throws Exception {
        Set<Principal> grPrincipals  = principalProvider.getMembershipPrincipals(userPrincipal);
        assertEquals(2, grPrincipals.size());
        assertTrue(grPrincipals.contains(EveryonePrincipal.getInstance()));
        assertTrue(grPrincipals.contains(testGroup.getPrincipal()));
    }

    @Test
    public void tstGetGroupMembershipNonExisting() {
        Set<Principal> grPrincipals = principalProvider.getMembershipPrincipals(nonExisting);
        assertNotNull(grPrincipals);
        assertTrue(grPrincipals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipEveryonePrincipal() {
        Set<Principal> grPrincipals = principalProvider.getMembershipPrincipals(EveryonePrincipal.getInstance());
        assertNotNull(grPrincipals);
        assertTrue(grPrincipals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipGroupPrincipal() throws Exception {
        Set<Principal> grPrincipals = principalProvider.getMembershipPrincipals(testGroup.getPrincipal());
        assertNotNull(grPrincipals);
        assertEquals(1, grPrincipals.size());
        assertTrue(grPrincipals.contains(EveryonePrincipal.getInstance()));
    }

    @Test
    public void testGetGroupMembershipGroupPrincipal2() throws Exception {
        Set<Principal> grPrincipals = principalProvider.getMembershipPrincipals(testGroup2.getPrincipal());
        assertNotNull(grPrincipals);
        assertEquals(2, grPrincipals.size());
        assertTrue(grPrincipals.contains(testGroup.getPrincipal()));
        assertTrue(grPrincipals.contains(EveryonePrincipal.getInstance()));
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
        String principalName = testGroup.getPrincipal().getName();
        assertNotNull(principalProvider.getPrincipal(principalName));

        List<String> nameHints = new ArrayList<String>();
        nameHints.add("testGroup");
        nameHints.add("test");
        nameHints.add("Group");
        nameHints.add("stGr");

        assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_GROUP, true);
        assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_ALL, true);
        assertResult(principalProvider, nameHints, principalName, PrincipalManager.SEARCH_TYPE_NOT_GROUP, false);
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

            List<String> resultNames = getNames(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL));
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

    @Test
    public void testFindRange() throws Exception {
        List<String> expected = Arrays.asList(groupId, groupId2, groupId3);
        Collections.sort(expected);

        for (int offset = 0; offset < expected.size() + 1; offset++) {
            for (int limit = -1; limit < expected.size() + 2; limit++) {
                int to = expected.size();
                if (limit >= 0) {
                    to = Math.min(offset + limit, to);
                }
                List<String> sub = expected.subList(offset, to);
                Iterator<? extends Principal> i1 = principalProvider.findPrincipals("testGroup", false,
                        PrincipalManager.SEARCH_TYPE_ALL, offset, limit);
                assertEquals(sub, getNames(i1));
                Iterator<? extends Principal> i2 = principalProvider.findPrincipals("testGroup", true,
                        PrincipalManager.SEARCH_TYPE_ALL, offset, limit);
                assertEquals(sub, getNames(i2));
            }
        }
    }

    protected static List<String> getNames(Iterator<? extends Principal> i) {
        List<String> l = new ArrayList<>();
        while (i.hasNext()) {
            l.add(i.next().getName());
        }
        return l;
    }

    @Test
    public void testFindRangeNegativeOffset() {
        List<String> expected = Arrays.asList(groupId, groupId2, groupId3);
        Collections.sort(expected);

        for (int limit = -1; limit < expected.size() + 2; limit++) {
            Iterator<? extends Principal> i1 = principalProvider.findPrincipals("testGroup", false,
                    PrincipalManager.SEARCH_TYPE_ALL, -1, limit);
            Iterator<? extends Principal> i2 = principalProvider.findPrincipals("testGroup", true,
                    PrincipalManager.SEARCH_TYPE_ALL, 0, limit);
            assertTrue(Iterators.elementsEqual(i1, i2));
        }
    }
}
