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
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ImpersonationImplTest extends ImpersonationImplEmptyTest {

    private User impersonator;

    @Override
    public void before() throws Exception {
        super.before();

        impersonator = getUserManager(root).createUser("impersonator" + UUID.randomUUID().toString(), null);
        impersonation.grantImpersonation(impersonator.getPrincipal());
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            impersonator.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetImpersonators() throws Exception {
        PrincipalIterator it = impersonation.getImpersonators();
        assertTrue(it.hasNext());
        assertTrue(Iterators.contains(it, impersonator.getPrincipal()));
    }

    @Test
    public void testGetImpersonatorsImpersonatorRemoved() throws Exception {
        Principal p = impersonator.getPrincipal();
        impersonator.remove();

        PrincipalIterator it = impersonation.getImpersonators();
        assertTrue(it.hasNext());
        assertTrue(Iterators.contains(it, p));
    }

    @Test
    public void testContentRepresentation() throws Exception {
        Tree tree = root.getTree(user.getPath());

        PropertyState property = tree.getProperty(UserConstants.REP_IMPERSONATORS);
        assertNotNull(property);
        assertEquals(ImmutableList.of(impersonator.getPrincipal().getName()), property.getValue(Type.STRINGS));
    }

    @Test
    public void testAllows() throws Exception {
        assertTrue(impersonation.allows(createSubject(impersonator.getPrincipal())));
    }

    @Test
    public void testAllowsIncludingGroup() throws Exception {
        Group gr = getUserManager(root).createGroup("gId");
        assertTrue(impersonation.allows(createSubject(impersonator.getPrincipal(), gr.getPrincipal())));
    }

    @Test
    public void testAllowsExistingGroup() throws Exception {
        Group gr = getUserManager(root).createGroup("gId");
        try {
            root.commit();
            assertFalse(impersonation.allows(createSubject(new PrincipalImpl(gr.getPrincipal().getName()))));
        } finally {
            gr.remove();
            root.commit();
        }
    }

    @Test
    public void testAllowsIncludingNonExistingGroup() throws Exception {
        assertTrue(impersonation.allows(createSubject(impersonator.getPrincipal(), groupPrincipal)));
    }

    @Test
    public void testAllowsImpersonatorRemoved() throws Exception {
        Subject s = createSubject(impersonator.getPrincipal());
        impersonator.remove();
        assertTrue(impersonation.allows(s));
    }

    @Test
    public void testAllowsNonExistingUser() {
        assertFalse(impersonation.allows(createSubject(new PrincipalImpl("nonExisting"))));
    }

    @Test
    public void testAllowsUserLookupFails() throws Exception {
        Principal nonExisting = new PrincipalImpl("nonExisting");
        UserManagerImpl uMgr = spy((UserManagerImpl) getUserManager(root));
        when(uMgr.getAuthorizable(nonExisting)).thenThrow(new RepositoryException());

        ImpersonationImpl imp = new ImpersonationImpl(new UserImpl(user.getID(), user.getTree(), uMgr));
        assertFalse(imp.allows(createSubject(nonExisting)));
    }

    @Test
    public void testGrantImpersonationUserLookupFails() throws Exception {
        Principal nonExisting = new TreeBasedPrincipal("nonExisting", user.getTree(), getNamePathMapper());
        UserManagerImpl uMgr = spy((UserManagerImpl) getUserManager(root));
        when(uMgr.getAuthorizable(nonExisting)).thenThrow(new RepositoryException());

        ImpersonationImpl imp = new ImpersonationImpl(new UserImpl(user.getID(), user.getTree(), uMgr));
        assertFalse(imp.grantImpersonation(nonExisting));
    }

    @Test
    public void testRevoke() throws Exception {
        assertTrue(impersonation.revokeImpersonation(impersonator.getPrincipal()));
    }

    @Test
    public void testContentRepresentationAfterModification() throws Exception {
        Principal principal2 = getTestUser().getPrincipal();
        impersonation.grantImpersonation(principal2);

        Tree tree = root.getTree(user.getPath());

        PropertyState property = tree.getProperty(UserConstants.REP_IMPERSONATORS);
        assertNotNull(property);

        Set<String> expected = Set.of(impersonator.getPrincipal().getName(), principal2.getName());
        assertEquals(expected, CollectionUtils.toSet(property.getValue(Type.STRINGS)));

        impersonation.revokeImpersonation(impersonator.getPrincipal());

        property = tree.getProperty(UserConstants.REP_IMPERSONATORS);
        assertNotNull(property);

        expected = Set.of(principal2.getName());
        assertEquals(expected, CollectionUtils.toSet(property.getValue(Type.STRINGS)));

        impersonation.revokeImpersonation(principal2);
        assertNull(tree.getProperty(UserConstants.REP_IMPERSONATORS));
    }

    @Test
    public void testImpersonationAllowByImpersonationGroupMember() throws Exception {
        String impersonatorGroupName = "impersonators-group";
        String impersonatorMember = "member-of-impersonator-group";
        impersonation = new ImpersonationImpl(ImpersonationTestUtil.getUserWithMockedConfigs(impersonatorGroupName, user));
        Subject impersonatorSubject = createSubject(impersonator.getPrincipal(), new PrincipalImpl(impersonatorGroupName));
        Subject impersonatorGroupMemberSubject = createSubject(impersonator.getPrincipal(), new PrincipalImpl(impersonatorMember), new GroupPrincipal() {
            @Override
            public boolean isMember(@NotNull Principal member) {
                return member.getName().equals(impersonatorMember);
            }

            @Override
            public @NotNull Enumeration<? extends Principal> members() {
                return null;
            }

            @Override
            public String getName() {
                return impersonatorGroupName;
            }
        });
        Subject nonImpersonatorSubject = createSubject(new PrincipalImpl("simple-user"));

        assertTrue(impersonation.allows(impersonatorSubject));
        assertTrue(impersonation.allows(impersonatorGroupMemberSubject));
        assertFalse(impersonation.allows(nonImpersonatorSubject));
    }
}