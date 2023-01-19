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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DuplicateMembershipTest extends AbstractSecurityTest {

    private DynamicMembershipProvider dmp;

    private Group group;
    private Authorizable member;
    
    @Before 
    public void before() throws Exception {
        super.before();
        UserManagerImpl userManager = (UserManagerImpl) spy(getUserManager(root));
        member = userManager.getAuthorizable(getTestUser().getID());
        
        group = userManager.createGroup("dynamicTestGroup");
        group.addMember(member);
        root.commit();

        dmp = mockDynamicMembershipProvider();
        when(userManager.getDynamicMembershipProvider()).thenReturn(dmp);
    }

    @Override
    public void after() throws Exception {
        try {
            group.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private @NotNull DynamicMembershipProvider mockDynamicMembershipProvider() throws RepositoryException {
        DynamicMembershipProvider dmp = mock(DynamicMembershipProvider.class);
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(true);
        // mock iterators with duplicate entries
        when(dmp.getMembers(any(Group.class), anyBoolean())).thenReturn(Iterators.forArray(member, member));
        when(dmp.getMembership(any(Authorizable.class), anyBoolean())).thenReturn(Iterators.forArray(group, group));
        return dmp;
    }
    
    @Test
    public void testGetMembers() throws Exception {
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(false);
        assertEquals(1, Iterators.size(group.getMembers()));
    }

    @Test
    public void testGetDeclaredMembers() throws Exception {
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(false);
        assertEquals(1, Iterators.size(group.getDeclaredMembers()));
    }

    @Test
    public void testGetMembersCoversAll() throws Exception {
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(true);
        assertEquals(1, Iterators.size(group.getMembers()));
    }

    @Test
    public void testGetDeclaredMembersCoversAll() throws Exception {
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(true);
        assertEquals(1, Iterators.size(group.getDeclaredMembers()));
    }
    
    @Test
    public void testGetMembership() throws Exception {
        assertEquals(1, Iterators.size(member.memberOf()));
    }

    @Test
    public void testGetDeclaredMembership() throws Exception {
        assertEquals(1, Iterators.size(member.declaredMemberOf()));
    }

    @Test
    public void testGetMembershipLocalMembershipRemoved() throws Exception {
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(false);

        group.removeMember(member);
        root.commit();

        assertEquals(1, Iterators.size(member.memberOf()));
    }

    @Test
    public void testGetDeclaredMembershipLocalMembershipRemoved() throws Exception {
        when(dmp.coversAllMembers(any(Group.class))).thenReturn(false);

        group.removeMember(member);
        root.commit();

        assertEquals(1, Iterators.size(member.declaredMemberOf()));
    }
}