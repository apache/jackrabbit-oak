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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import javax.jcr.nodetype.ConstraintViolationException;
import java.security.Principal;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GroupImplTest extends AbstractUserTest {

    private final String groupId = "gr" + UUID.randomUUID();

    private UserManagerImpl uMgr;
    private GroupImpl group;

    @Override
    public void before() throws Exception {
        super.before();

        uMgr = createUserManagerImpl(root);
        Group g = uMgr.createGroup(groupId);

        group = new GroupImpl(groupId, root.getTree(g.getPath()), uMgr);
    }

    @Override
    public void after() throws Exception {
        try {
            clearInvocations(monitor);
            root.refresh();
        } finally {
            super.after();
        }
    }

    private void verifyMonitor(long failedCnt, boolean isRemove) {
        verify(monitor).doneUpdateMembers(anyLong(), eq(1L), eq(failedCnt), eq(isRemove));
    }

    @Test
    public void testIsGroup() {
        assertTrue(group.isGroup());
    }

    @Test
    public void testRemove() throws Exception {
        group.remove();
        assertNull(uMgr.getAuthorizable(groupId, Group.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckValidTree() throws Exception {
        new GroupImpl(getTestUser().getID(), root.getTree(getTestUser().getPath()), uMgr);
    }

    @Test
    public void testAddMemberInvalidAuthorizable() throws Exception {
        assertFalse(group.addMember(mock(Authorizable.class)));
        verifyMonitor(1, false);
    }

    @Test
    public void testAddMemberEveryone() throws Exception {
        Group everyoneGroup = uMgr.createGroup(EveryonePrincipal.getInstance());
        assertFalse(group.addMember(everyoneGroup));
        verifyMonitor(1, false);
    }

    @Test
    public void testAddMemberItself() throws Exception {
        assertFalse(group.addMember(group));
        verifyMonitor(1, false);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testAddMemberWithCycle() throws Exception {
        Group g2 = uMgr.createGroup("group2");
        g2.addMember(group);
        group.addMember(g2);
    }

    @Test
    public void testRemoveMemberInvalidAuthorizable() throws Exception {
        assertFalse(group.removeMember(mock(Authorizable.class)));
        verifyMonitor(1, true);
    }

    @Test
    public void testRemoveNotMember() throws Exception {
        assertFalse(group.removeMember(getTestUser()));
        verifyMonitor(1, true);
    }

    @Test
    public void testIsMemberInvalidAuthorizable() throws Exception {
        assertFalse(group.isMember(mock(Authorizable.class)));
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

    @Test
    public void testImpactOfOak8054AddingMembers() throws Exception {
        Tree groupTree = root.getTree(group.getPath());
        groupTree.setProperty(REP_MEMBERS, ImmutableList.of(new UserProvider(root, ConfigurationParameters.EMPTY).getContentID(getTestUser().getID())), Type.STRINGS);
        root.commit();

        group.addMember(uMgr.createUser("userid", null));
        root.commit();

        groupTree = root.getTree(group.getPath());
        PropertyState membersProp = groupTree.getProperty(REP_MEMBERS);
        assertEquals(Type.WEAKREFERENCES, membersProp.getType());
        assertEquals(2, membersProp.count());
    }

    @Test
    public void testImpactOfOak8054RemovingMembers() throws Exception {
        User user = uMgr.createUser("userid", null);
        UserProvider up = new UserProvider(root, ConfigurationParameters.EMPTY);
        Tree groupTree = root.getTree(group.getPath());
        groupTree.setProperty(REP_MEMBERS, ImmutableList.of(up.getContentID(getTestUser().getID()), up.getContentID(user.getID())), Type.STRINGS);
        root.commit();

        group.removeMembers(user.getID());
        root.commit();

        groupTree = root.getTree(group.getPath());
        PropertyState membersProp = groupTree.getProperty(REP_MEMBERS);
        assertEquals(Type.WEAKREFERENCES, membersProp.getType());
        assertEquals(1, membersProp.count());
    }
}