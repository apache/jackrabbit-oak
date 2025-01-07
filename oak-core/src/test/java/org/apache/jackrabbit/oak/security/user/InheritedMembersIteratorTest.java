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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class InheritedMembersIteratorTest extends AbstractSecurityTest {
    
    private Group base;
    private Group nonDynamicGroup;
    private Group dynamicGroup;
    private User dynamicUser;
    
    private final Set<String> ids = new HashSet<>();

    @Before
    public void before() throws Exception {
        super.before();

        UserManager um = getUserManager(root);
        base = um.createGroup("base");
        nonDynamicGroup = um.createGroup("testGroup");
        
        // add 2 members to the base group
        base.addMember(nonDynamicGroup);
        base.addMember(getTestUser());

        // add 1 nested group member to 'nonDynamicGroup'
        dynamicGroup = um.createGroup("dynamicTestGroup");
        nonDynamicGroup.addMember(dynamicGroup);

        // create another user
        dynamicUser = um.createUser("dynamicTestUser", null);
        root.commit();

        // remember all IDs for cleanup
        ids.addAll(Set.of("base", "testGroup", "dynamicTestGroup", "dynamicTestUser"));
    }

    @Override
    public void after() throws Exception {
        try {
            UserManager um = getUserManager(root);
            for (String id : ids) {
                Authorizable a = um.getAuthorizable(id);
                if (a != null) {
                    a.remove();
                }
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testNoDynamicMembers() throws Exception {
        DynamicMembershipProvider dmp = mock(DynamicMembershipProvider.class);
        when(dmp.getMembers(any(Group.class), anyBoolean())).thenReturn(Collections.emptyIterator());

        // no dynamic members in result
        Set<String> expectedMemberIds = Set.of(getTestUser().getID(), "testGroup", "dynamicTestGroup");
        assertEquals(expectedMemberIds, getMembersIds(new InheritedMembersIterator(base.getMembers(), dmp)));
        
        verify(dmp, times(2)).getMembers(any(Group.class), eq(false));
        verifyNoMoreInteractions(dmp);
    }

    @Test
    public void testDynamicMembers() throws Exception {
        DynamicMembershipProvider dmp = mock(DynamicMembershipProvider.class);
        when(dmp.getMembers(dynamicGroup, false)).thenReturn(Iterators.forArray(dynamicUser, getTestUser()));
        when(dmp.getMembers(nonDynamicGroup, false)).thenReturn(Collections.emptyIterator());

        // dynamic members get resolved this time
        Set<String> expectedMemberIds = Set.of(getTestUser().getID(), "testGroup", "dynamicTestGroup", "dynamicTestUser");
        assertEquals(expectedMemberIds, getMembersIds(new InheritedMembersIterator(base.getMembers(), dmp)));

        verify(dmp, times(2)).getMembers(any(Group.class), eq(false));
        verifyNoMoreInteractions(dmp);
    }

    @Test
    public void testDynamicMembersFails() throws Exception {
        DynamicMembershipProvider dmp = mock(DynamicMembershipProvider.class);
        when(dmp.getMembers(any(Group.class), anyBoolean())).thenThrow(new RepositoryException());

        Set<String> expectedMemberIds = Set.of(getTestUser().getID(), "testGroup", "dynamicTestGroup");
        assertEquals(expectedMemberIds, getMembersIds(new InheritedMembersIterator(base.getMembers(), dmp)));

        verify(dmp, times(2)).getMembers(any(Group.class), eq(false));
        verifyNoMoreInteractions(dmp);
    }
    
    private static @NotNull Set<String> getMembersIds(@NotNull InheritedMembersIterator it) {
        return CollectionUtils.toSet(Iterators.transform(it, authorizable -> {
            try {
                return authorizable.getID();
            } catch (RepositoryException repositoryException) {
                return null;
            }
        }));
    }
}