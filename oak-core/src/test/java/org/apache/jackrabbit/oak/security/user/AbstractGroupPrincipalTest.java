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
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class AbstractGroupPrincipalTest extends AbstractSecurityTest {

    private Group testGroup;

    private AbstractGroupPrincipal agp;
    private AbstractGroupPrincipal everyoneAgp;
    private AbstractGroupPrincipal throwing;


    @Override
    public void before() throws Exception {
        super.before();

        testGroup = getUserManager(root).createGroup("AbstractGroupPrincipalTest");
        root.commit();

        agp = new AGP();
        everyoneAgp = new AGP();
        ((AGP) everyoneAgp).isEveryone = true;

        throwing = new ThrowingAGP();
    }

    @Override
    public void after() throws Exception {
        try {
            if (testGroup != null) {
                testGroup.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testIsMemberOf() throws Exception {
        final Principal p = getTestUser().getPrincipal();
        assertTrue(agp.isMember(p));
        assertTrue(agp.isMember(new PrincipalImpl(p.getName())));
        assertTrue(agp.isMember(new Principal() {
            @Override
            public String getName() {
                return p.getName();
            }
        }));

    }

    @Test
    public void testIsMemberMissingAuthorizable() throws RepositoryException {
        List<Principal> principals = ImmutableList.of(
                new PrincipalImpl("name"),
                new Principal() {
                    @Override
                    public String getName() {
                        return "name";
                    }
                }
        );

        for (Principal p : principals) {
            assertFalse(agp.isMember(p));
        }
    }

    @Test
    public void testIsMemberOfEveryone() throws Exception {
        final Principal p = getTestUser().getPrincipal();
        assertTrue(everyoneAgp.isMember(p));
        assertTrue(everyoneAgp.isMember(new PrincipalImpl(p.getName())));
        assertTrue(everyoneAgp.isMember(new Principal() {
            @Override
            public String getName() {
                return p.getName();
            }
        }));

    }

    @Test
    public void testIsMemberOfEveryoneMissingAuthorizable() throws RepositoryException {
        List<Principal> principals = ImmutableList.of(
                new PrincipalImpl("name"),
                new Principal() {
                    @Override
                    public String getName() {
                        return "name";
                    }
                }
        );

        for (Principal p : principals) {
            assertTrue(everyoneAgp.isMember(p));
        }
    }

    @Test
    public void testIsMemberOfInternalError() throws Exception {
        final Principal p = getTestUser().getPrincipal();
        assertFalse(throwing.isMember(p));
    }

    @Test(expected = IllegalStateException.class)
    public void testMembersInternalError() throws Exception {
        throwing.members();
    }

    @Test
    public void testEveryoneIsMemberOfEveryone() throws RepositoryException {
        AbstractGroupPrincipal member = Mockito.mock(AbstractGroupPrincipal.class);
        when(member.getName()).thenReturn(EveryonePrincipal.NAME);

        assertFalse(everyoneAgp.isMember(member));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddMember() {
        agp.addMember(new PrincipalImpl("name"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveMember() {
        agp.removeMember(new PrincipalImpl("name"));
    }

    private class AGP extends AbstractGroupPrincipal {

        private Authorizable member;
        private boolean isEveryone;

        AGP() throws Exception {
            super(testGroup.getPrincipal().getName(), root.getTree(testGroup.getPath()), getNamePathMapper());
            member = getTestUser();
        }

        @Override
        UserManager getUserManager() {
            return AbstractGroupPrincipalTest.this.getUserManager(root);
        }

        @Override
        boolean isEveryone() throws RepositoryException {
            return isEveryone;
        }

        @Override
        boolean isMember(@Nonnull Authorizable authorizable) throws RepositoryException {
            return member.getID().equals(authorizable.getID());
        }

        @Nonnull
        @Override
        Iterator<Authorizable> getMembers() throws RepositoryException {
            return ImmutableList.of(member).iterator();
        }
    }

    private class ThrowingAGP extends AGP {

        ThrowingAGP() throws Exception {
            super();
        }

        @Override
        UserManager getUserManager() {
            UserManager userManager = Mockito.mock(UserManager.class);
            Mockito.doThrow(RepositoryException.class);
            return userManager;
        }

        @Override
        boolean isEveryone() throws RepositoryException {
            throw new RepositoryException();
        }

        @Override
        boolean isMember(@Nonnull Authorizable authorizable) throws RepositoryException {
            throw new RepositoryException();
        }

        @Nonnull
        @Override
        Iterator<Authorizable> getMembers() throws RepositoryException {
            throw new RepositoryException();
        }
    }
}