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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.lang.reflect.Method;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrincipalMembershipReaderTest extends AbstractSecurityTest {

    String groupId;
    String groupId2;

    @Override
    public void before() throws Exception {
        super.before();
        
        groupId = "testGroup" + UUID.randomUUID();
        Group testGroup = getUserManager(root).createGroup(groupId);
        testGroup.addMember(getTestUser());

        groupId2 = "testGroup" + UUID.randomUUID() + "2";
        Group testGroup2 = getUserManager(root).createGroup(groupId2);
        testGroup.addMember(testGroup2);

        root.commit();
    }

    public void after() throws Exception {
        try {
            root.refresh();
            String[] rm = new String[] { groupId, groupId2};
            for (String id : rm) {
                Authorizable a = getUserManager(root).getAuthorizable(id);
                if (a != null) {
                    a.remove();
                    root.commit();
                }
            }
        } finally {
            super.after();
        }
    }

    @NotNull PrincipalMembershipReader.GroupPrincipalFactory createFactory(@NotNull Root root) throws Exception {
        UserPrincipalProvider userPrincipalProvider = (UserPrincipalProvider) createPrincipalProvider(root);
        Method m = UserPrincipalProvider.class.getDeclaredMethod("createGroupPrincipalFactory");
        m.setAccessible(true);
        return (PrincipalMembershipReader.GroupPrincipalFactory) m.invoke(userPrincipalProvider);
    }
    
    @NotNull PrincipalProvider createPrincipalProvider(@NotNull Root root) {
        return new UserPrincipalProvider(root, getUserConfiguration(), namePathMapper);
    }
    
    @NotNull MembershipProvider createMembershipProvider(@NotNull Root root) {
        return new MembershipProvider(root, getUserConfiguration().getParameters());
    }
    
    @NotNull PrincipalMembershipReader createPrincipalMembershipReader(@NotNull Root root) throws Exception {
        return new PrincipalMembershipReader(createMembershipProvider(root), createFactory(root));
    }
    
    @NotNull Tree getTree(@NotNull String id, @NotNull Root root) throws Exception {
        return root.getTree(getUserManager(root).getAuthorizable(id).getPath());
    }
    
    @Test
    public void testReadMembershipForUser() throws Exception {
        Set<Principal> result = new HashSet<>();
        createPrincipalMembershipReader(root).readMembership(getTree(getTestUser().getID(), root), result);
        assertEquals(1, result.size());
    }
    
    @Test
    public void testReadMembershipForGroup() throws Exception {
        Set<Principal> result = new HashSet<>();
        createPrincipalMembershipReader(root).readMembership(getTree(groupId2, root), result);
        assertEquals(1, result.size());
    }
    
    @Test
    public void testResolvingGroupsReturnsUnexpectedAuthorizableType() throws Exception {
        PropertyState ps = PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        Tree mockTree = when(mock(Tree.class).getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(ps).getMock();
        MembershipProvider mp = when(mock(MembershipProvider.class).getMembership(any(Tree.class), anyBoolean())).thenReturn(Iterators.singletonIterator(mockTree)).getMock();
        
        PrincipalMembershipReader reader = new PrincipalMembershipReader(mp, createFactory(root));
        Set<Principal> result = new HashSet<>();
        reader.readMembership(root.getTree(getTestUser().getPath()), result);
        assertTrue(result.isEmpty());
    }
}
