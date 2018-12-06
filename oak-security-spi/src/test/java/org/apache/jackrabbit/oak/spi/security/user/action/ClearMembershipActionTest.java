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
package org.apache.jackrabbit.oak.spi.security.user.action;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.jcr.Value;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ClearMembershipActionTest {

    private final ClearMembershipAction action = new ClearMembershipAction();

    private final Root root = Mockito.mock(Root.class);
    private final SecurityProvider securityProvider = Mockito.mock(SecurityProvider.class);

    private final UserManager userManager = Mockito.mock(UserManager.class);
    private final UserConfiguration userConfiguration = Mockito.mock(UserConfiguration.class);

    private final User user = Mockito.mock(User.class);
    private final TestGroup gr = new TestGroup();

    @Before
    public void before() {
        action.init(securityProvider, ConfigurationParameters.EMPTY);
    }

    @Test
    public void testOnRemoveUserNoMembership() throws Exception {
        when(user.declaredMemberOf()).thenReturn(Collections.emptyIterator());
        action.onRemove(user, root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnRemoveGroupNoMembership() throws Exception {
        action.onRemove(gr, root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnRemoveUserWithMembership() throws Exception {
        when(user.declaredMemberOf()).thenReturn(Iterators.singletonIterator(gr));

        action.onRemove(user, root, NamePathMapper.DEFAULT);
        assertTrue(gr.removed.contains(user));
    }

    @Test
    public void testOnRemoveGroupWithMembership() throws Exception {
        Group memberGroup = Mockito.mock(Group.class);
        when(memberGroup.declaredMemberOf()).thenReturn(Iterators.singletonIterator(gr));

        action.onRemove(memberGroup, root, NamePathMapper.DEFAULT);
        assertTrue(gr.removed.contains(memberGroup));
    }


    private static final class TestGroup implements Group {

        Set<Authorizable> removed = new HashSet<>();

        @Override
        public Iterator<Authorizable> getDeclaredMembers() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Authorizable> getMembers() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDeclaredMember(Authorizable authorizable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMember(Authorizable authorizable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addMember(Authorizable authorizable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> addMembers(@NotNull String... strings) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeMember(Authorizable authorizable) {
            return removed.add(authorizable);
        }

        @Override
        public Set<String> removeMembers(@NotNull String... strings) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isGroup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal getPrincipal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Group> declaredMemberOf() {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Group> memberOf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<String> getPropertyNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<String> getPropertyNames(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasProperty(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setProperty(String s, Value value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setProperty(String s, Value[] values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Value[] getProperty(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeProperty(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPath() {
            throw new UnsupportedOperationException();
        }
    }
}