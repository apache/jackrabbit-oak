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
import java.util.UUID;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GroupImplTest extends AbstractSecurityTest {

    private final String groupId = "gr" + UUID.randomUUID();

    private UserManagerImpl uMgr;
    private GroupImpl group;

    @Override
    public void before() throws Exception {
        super.before();

        uMgr = new UserManagerImpl(root, getNamePathMapper(), getSecurityProvider());
        Group g = uMgr.createGroup(groupId);

        group = new GroupImpl(groupId, root.getTree(g.getPath()), uMgr);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckValidTree() throws Exception {
        new GroupImpl(getTestUser().getID(), root.getTree(getTestUser().getPath()), uMgr);
    }

    @Test
    public void testAddMemberInvalidAuthorizable() throws Exception {
        assertFalse(group.addMember(Mockito.mock(Authorizable.class)));
    }

    @Test
    public void testAddMemberEveryone() throws Exception {
        Group everyoneGroup = uMgr.createGroup(EveryonePrincipal.getInstance());
        assertFalse(group.addMember(everyoneGroup));
    }

    @Test
    public void testAddMemberItself() throws Exception {
        assertFalse(group.addMember(group));
    }

    @Test
    public void testRemoveMemberInvalidAuthorizable() throws Exception {
        assertFalse(group.removeMember(Mockito.mock(Authorizable.class)));
    }

    @Test
    public void testRemoveNotMember() throws Exception {
        assertFalse(group.removeMember(getTestUser()));
    }

    @Test
    public void testIsMemberInvalidAuthorizable() throws Exception {
        assertFalse(group.isMember(Mockito.mock(Authorizable.class)));
    }

    @Test
    public void testGroupPrincipal() throws Exception {
        Principal groupPrincipal = group.getPrincipal();
        assertTrue(groupPrincipal instanceof AbstractGroupPrincipal);

        AbstractGroupPrincipal agp = (AbstractGroupPrincipal) groupPrincipal;
        assertSame(uMgr, agp.getUserManager());
        assertEquals(group.isEveryone(), agp.isEveryone());
    }

    @Test
    public void testGroupPrincipalIsMember() throws Exception {
        group.addMember(getTestUser());

        AbstractGroupPrincipal groupPrincipal = (AbstractGroupPrincipal) group.getPrincipal();
        assertTrue(groupPrincipal.isMember(getTestUser()));
    }

    @Test
    public void testGroupPrincipalMembers() throws Exception {
        group.addMember(getTestUser());

        AbstractGroupPrincipal groupPrincipal = (AbstractGroupPrincipal) group.getPrincipal();
        Iterator<Authorizable> members = groupPrincipal.getMembers();
        assertTrue(Iterators.elementsEqual(group.getMembers(), members));
    }
}