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

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.security.principal.AbstractPrincipalProviderTest;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.lang.reflect.Method;
import java.security.Principal;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.api.security.principal.PrincipalManager.SEARCH_TYPE_GROUP;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_USER;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_PRINCIPAL_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserPrincipalProviderTest extends AbstractPrincipalProviderTest {

    @NotNull
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
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());
        assertTrue(principal instanceof AbstractGroupPrincipal);
    }

    @Test
    public void testTreeBasedGroupPrincipalReflectsMemberChanges() throws Exception {
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());
        assertTrue(principal instanceof AbstractGroupPrincipal);
        AbstractGroupPrincipal agp = (AbstractGroupPrincipal) principal;

        User u = getTestUser();
        assertTrue(agp.isMember(u));

        testGroup.removeMember(u);
        assertFalse(agp.isMember(u));
    }

    @Test
    public void testTreeBasedGroupPrincipalReflectsRemoval() throws Exception {
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());
        assertTrue(principal instanceof AbstractGroupPrincipal);
        AbstractGroupPrincipal agp = (AbstractGroupPrincipal) principal;

        testGroup.remove();
        assertFalse(agp.isMember(getTestUser()));
    }

    @Test
    public void testTreeBasedGroupPrincipalReflectsChangedType() throws Exception {
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());
        assertTrue(principal instanceof AbstractGroupPrincipal);
        AbstractGroupPrincipal agp = (AbstractGroupPrincipal) principal;

        Tree t = root.getTree(testGroup.getPath());
        t.setProperty(JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
        assertFalse(agp.isMember(getTestUser()));
    }

    @Test
    public void testGetPrincipalsForUser() throws Exception {
        Set<? extends Principal> principals = principalProvider.getPrincipals(getTestUser().getID());
        for (Principal p : principals) {
            String name = p.getName();
            if (name.equals(getTestUser().getPrincipal().getName())) {
                assertTrue(p instanceof TreeBasedPrincipal);
            } else if (!EveryonePrincipal.NAME.equals(name)) {
                assertTrue(p instanceof AbstractGroupPrincipal);
            }
        }
    }

    @Test
    public void testGetPrincipalsForSystemUser() throws Exception {
        User systemUser = null;
        try {
            systemUser = getUserManager(root).createSystemUser("systemUser" + UUID.randomUUID(), null);
            testGroup.addMember(systemUser);
            root.commit();
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
            if (systemUser != null) {
                systemUser.remove();
                root.commit();
            }
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

            assertTrue(ep instanceof GroupPrincipal);
            //((GroupPrincipal) ep).members();
            //assertTrue(((GroupPrincipal) ep).isMember(getTestUser().getPrincipal()));

        } finally {
            if (everyoneGroup != null) {
                everyoneGroup.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testGroupMembers() throws Exception {
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());
        assertTrue(principal instanceof GroupPrincipal);

        boolean found = false;
        Enumeration<? extends Principal> members = ((GroupPrincipal) principal).members();
        while (members.hasMoreElements() && !found) {
            found = members.nextElement().equals(getTestUser().getPrincipal());
        }
        assertTrue(found);
    }

    @Test
    public void testGroupIsMember() throws Exception {
        Principal principal = principalProvider.getPrincipal(testGroup.getPrincipal().getName());

        assertTrue(principal instanceof GroupPrincipal);
        assertTrue(((GroupPrincipal) principal).isMember(getTestUser().getPrincipal()));
    }

    @Test
    public void testMissingUserPrincipalName() throws Exception {
        User u = getTestUser();
        Tree t = root.getTree(u.getPath());
        t.removeProperty(REP_PRINCIPAL_NAME);

        assertTrue(principalProvider.getPrincipals(u.getID()).isEmpty());
    }

    @Test
    public void testMissingGroupPrincipalName() throws Exception {
        Principal p = testGroup.getPrincipal();
        Tree t = root.getTree(testGroup.getPath());
        t.removeProperty(REP_PRINCIPAL_NAME);

        assertFalse(principalProvider.getPrincipals(getTestUser().getID()).contains(p));
    }

    @Test
    public void testFindWithEmptyHint() throws Exception {
        List<String> resultNames = getNames(principalProvider.findPrincipals("", PrincipalManager.SEARCH_TYPE_GROUP));

        assertFalse(resultNames.contains(getTestUser().getPrincipal().getName()));

        assertTrue(resultNames.contains(EveryonePrincipal.NAME));
        assertTrue(resultNames.contains(testGroup.getPrincipal().getName()));
    }

    @Test
    public void testFindFullTextWithAndWithoutWildcard() {
        Iterator<? extends Principal> i1 = principalProvider.findPrincipals("testGroup", true,
                SEARCH_TYPE_GROUP, 0, -1);
        Iterator<? extends Principal> i2 = principalProvider.findPrincipals("testGroup*", true,
                SEARCH_TYPE_GROUP, 0, -1);
        assertTrue(Iterators.elementsEqual(i1, i2));
    }

    @Test
    public void testFindFiltersDuplicateEveryone() throws Exception {
        Group everyoneGroup = null;
        try {
            UserManager userMgr = getUserManager(root);
            everyoneGroup = userMgr.createGroup(EveryonePrincipal.NAME);
            root.commit();

            Iterator<? extends Principal> principals = principalProvider.findPrincipals(null, SEARCH_TYPE_GROUP);
            Iterator filtered = Iterators.filter(principals, principal -> EveryonePrincipal.NAME.equals(principal.getName()));
            assertEquals(1, Iterators.size(filtered));
        } finally {
            if (everyoneGroup != null) {
                everyoneGroup.remove();
                root.commit();
            }
        }

        List<String> expected = Arrays.asList(groupId, groupId2, groupId3);
        Collections.sort(expected);

        for (int limit = -1; limit < expected.size() + 2; limit++) {
            Iterator<? extends Principal> i1 = principalProvider.findPrincipals("testGroup", true,
                    SEARCH_TYPE_GROUP, 0, limit);
            Iterator<? extends Principal> i2 = principalProvider.findPrincipals("testGroup*", true,
                    SEARCH_TYPE_GROUP, 0, limit);
            assertTrue(Iterators.elementsEqual(i1, i2));
        }
    }

    @Test
    public void testFindPrincipalsQueryFails() throws ParseException {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), anyLong(), anyLong(), any(Map.class), any(Map.class))).thenThrow(new ParseException("err",0));

        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        UserPrincipalProvider upp = new UserPrincipalProvider(r, getUserConfiguration(), NamePathMapper.DEFAULT);
        Iterator<? extends Principal> it = upp.findPrincipals("a", false, PrincipalManager.SEARCH_TYPE_ALL, -1, -1);
        assertNotNull(it);
        assertFalse(it.hasNext());
    }

    @Test
    public void testFindPrincipalsQueryFailsNullHint() throws ParseException {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), anyLong(), anyLong(), any(Map.class), any(Map.class))).thenThrow(new ParseException("err",0));

        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        UserPrincipalProvider upp = new UserPrincipalProvider(r, getUserConfiguration(), NamePathMapper.DEFAULT);
        Iterator<? extends Principal> it = upp.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_ALL, -1, -1);
        assertNotNull(it);
        assertFalse(it.hasNext());
    }

    @Test
    public void testCreatePrincipalInvalidType() throws Exception {
        Method m = UserPrincipalProvider.class.getDeclaredMethod("createPrincipal", Tree.class);
        m.setAccessible(true);

        assertNull(m.invoke(principalProvider, (Tree)null));

        PropertyState ps = PropertyStates.createProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
        Tree tree = when(mock(Tree.class).getProperty(JCR_PRIMARYTYPE)).thenReturn(ps).getMock();
        assertNull(m.invoke(principalProvider, tree));
    }
}
