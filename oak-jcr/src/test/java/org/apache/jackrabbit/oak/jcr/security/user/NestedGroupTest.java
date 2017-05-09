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
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the {@link Group} implementation specific for nested group membership.
 */
public class NestedGroupTest extends AbstractUserTest {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private Group createGroup(Principal p) throws RepositoryException {
        Group gr = userMgr.createGroup(p);
        superuser.save();
        return gr;
    }

    private void removeGroup(Group gr) throws RepositoryException {
        gr.remove();
        superuser.save();
    }

    private boolean addMember(Group gr, Authorizable member) throws RepositoryException {
        boolean added = gr.addMember(member);
        superuser.save();
        return added;
    }

    private boolean removeMember(Group gr, Authorizable member) throws RepositoryException {
        boolean removed = gr.removeMember(member);
        superuser.save();
        return removed;
    }

    @Test
    public void testAddGroupAsMember() throws NotExecutableException, RepositoryException {
        Group gr1 = null;
        Group gr2 = null;

        try {
            gr1 = createGroup(getTestPrincipal());
            gr2 = createGroup(getTestPrincipal());

            assertFalse(gr1.isMember(gr2));

            assertTrue(addMember(gr1, gr2));
            assertTrue(gr1.isMember(gr2));

        } finally {
            if (gr1 != null) {
                removeMember(gr1, gr2);
                removeGroup(gr1);
            }
            if (gr2 != null) {
                removeGroup(gr2);
            }
        }
    }

    @Test
    public void testAddCircularMembers() throws NotExecutableException, RepositoryException {
        Group gr1 = null;
        Group gr2 = null;

        try {
            gr1 = createGroup(getTestPrincipal());
            gr2 = createGroup(getTestPrincipal());

            assertTrue(addMember(gr1, gr2));
            try {
                assertFalse(addMember(gr2, gr1));
            } catch (ConstraintViolationException e) {
                // cycle detected upon save => success
                assertCyclicMembershipError(e);
            }

        } finally {
            if (gr1 != null && gr1.isMember(gr2)) {
                removeMember(gr1, gr2);
            }
            if (gr2 != null && gr2.isMember(gr1)) {
                removeMember(gr2, gr1);
            }
            if (gr1 != null) removeGroup(gr1);
            if (gr2 != null) removeGroup(gr2);
        }
    }

    @Test
    public void testCyclicMembers2() throws RepositoryException, NotExecutableException {
        Group gr1 = null;
        Group gr2 = null;
        Group gr3 = null;
        try {
            gr1 = createGroup(getTestPrincipal());
            gr2 = createGroup(getTestPrincipal());
            gr3 = createGroup(getTestPrincipal());

            assertTrue(addMember(gr1, gr2));
            assertTrue(addMember(gr2, gr3));
            try {
                assertFalse(addMember(gr3, gr1));
            } catch (ConstraintViolationException e) {
                // cycle detected upon save => success
                assertCyclicMembershipError(e);
            }

        } finally {
            if (gr1 != null) {
                removeMember(gr1, gr2);
            }
            if (gr2 != null) {
                removeMember(gr2, gr3);
                removeGroup(gr2);
            }
            if (gr3 != null) {
                removeMember(gr3, gr1);
                removeGroup(gr3);
            }
            if (gr1 != null) removeGroup(gr1);

        }
    }

    private static void assertCyclicMembershipError(Exception e) {
        Throwable th = e.getCause();
        if (th != null) {
            assertTrue(th instanceof CommitFailedException);
            CommitFailedException ce = (CommitFailedException) th;
            assertEquals(CommitFailedException.CONSTRAINT, ce.getType());
            assertEquals(31, ce.getCode());
        }
    }

    @Test
    public void testInheritedMembership() throws NotExecutableException, RepositoryException {
        Group gr1 = null;
        Group gr2 = null;
        Group gr3 = null;

        if (!(superuser instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }

        try {
            gr1 = createGroup(getTestPrincipal());
            gr2 = createGroup(getTestPrincipal());
            gr3 = createGroup(getTestPrincipal());

            assertTrue(addMember(gr1, gr2));
            assertTrue(addMember(gr2, gr3));

            // NOTE: don't test with Group.isMember for not required to detect
            // inherited membership -> rather with PrincipalManager.
            boolean isMember = false;
            PrincipalManager pmgr = ((JackrabbitSession) superuser).getPrincipalManager();
            for (PrincipalIterator it = pmgr.getGroupMembership(gr3.getPrincipal());
                 it.hasNext() && !isMember;) {
                isMember = it.nextPrincipal().equals(gr1.getPrincipal());
            }
            assertTrue(isMember);

        } finally {
            if (gr1 != null && gr1.isMember(gr2)) {
                removeMember(gr1, gr2);
            }
            if (gr2 != null && gr2.isMember(gr3)) {
                removeMember(gr2, gr3);
            }
            if (gr1 != null) removeGroup(gr1);
            if (gr2 != null) removeGroup(gr2);
            if (gr3 != null) removeGroup(gr3);
        }
    }
}