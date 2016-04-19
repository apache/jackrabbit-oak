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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupActionTest extends AbstractSecurityTest {

    private static final String TEST_GROUP_ID = "testGroup";
    private static final String TEST_USER_PREFIX = "testUser";

    TestGroupAction groupAction = new TestGroupAction();
    Group testGroup;
    private User testUser01;
    private User testUser02;

    @Before
    public void before() throws Exception {
        super.before();

        testGroup = getUserManager(root).createGroup(TEST_GROUP_ID);
        root.commit();
    }

    @After
    public void after() throws Exception {
        if (testGroup != null) {
            testGroup.remove();
            root.commit();
        }

        if (testUser01 != null) {
            testUser01.remove();
            root.commit();
        }

        if (testUser02 != null) {
            testUser02.remove();
            root.commit();
        }

        root = null;
        super.after();
    }

    @Test
    public void testMemberAdded() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");

        testGroup.addMember(testUser01);
        assertTrue(groupAction.onMemberAddedCalled);
        assertEquals(testGroup, groupAction.group);
        assertEquals(testUser01, groupAction.member);
    }

    @Test
    public void testMemberRemoved() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");
        testGroup.addMember(testUser01);
        root.commit();

        testGroup.removeMember(testUser01);
        assertTrue(groupAction.onMemberRemovedCalled);
        assertEquals(testGroup, groupAction.group);
        assertEquals(testUser01, groupAction.member);
    }

    @Test
    public void testMembersAdded() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");
        testUser02 = getUserManager(root).createUser(TEST_USER_PREFIX + "02", "");
        testGroup.addMember(testUser02);

        List<String> memberIds = ImmutableList.of(testUser01.getID());
        List<String> failedIds = ImmutableList.of(testUser02.getID(), testGroup.getID());
        Iterable<String> ids = Iterables.concat(memberIds, failedIds);

        testGroup.addMembers(Iterables.toArray(ids, String.class));
        assertTrue(groupAction.onMembersAddedCalled);
        assertEquals(testGroup, groupAction.group);
        assertTrue(Iterables.elementsEqual(memberIds, groupAction.memberIds));
        assertTrue(Iterables.elementsEqual(failedIds, groupAction.failedIds));
    }

    @Test
    public void testMembersAddedNonExisting() throws Exception {
        List<String> nonExisting = ImmutableList.of("blinder", "passagier");

        testGroup.addMembers(nonExisting.toArray(new String[nonExisting.size()]));
        assertFalse(groupAction.memberIds.iterator().hasNext());
        assertTrue(Iterables.elementsEqual(nonExisting, groupAction.failedIds));
    }

    @Test
    public void testMembersRemoved() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");
        testUser02 = getUserManager(root).createUser(TEST_USER_PREFIX + "02", "");
        testGroup.addMember(testUser01);

        List<String> memberIds = ImmutableList.of(testUser01.getID());
        List<String> failedIds = ImmutableList.of(testUser02.getID(), testGroup.getID());
        Iterable<String> ids = Iterables.concat(memberIds, failedIds);

        testGroup.removeMembers(Iterables.toArray(ids, String.class));
        assertTrue(groupAction.onMembersRemovedCalled);
        assertEquals(testGroup, groupAction.group);
        assertTrue(Iterables.elementsEqual(memberIds, groupAction.memberIds));
        assertTrue(Iterables.elementsEqual(failedIds, groupAction.failedIds));
    }

    @Test
    public void testMembersRemovedNonExisting() throws Exception {
        List<String> nonExisting = ImmutableList.of("blinder", "passagier");

        testGroup.removeMembers(nonExisting.toArray(new String[nonExisting.size()]));
        assertFalse(groupAction.memberIds.iterator().hasNext());
        assertTrue(Iterables.elementsEqual(nonExisting, groupAction.failedIds));
    }

    @Override
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = new TestSecurityProvider();
        }
        return securityProvider;
    }

    class TestGroupAction extends AbstractGroupAction {

        boolean onMemberAddedCalled = false;
        boolean onMembersAddedCalled = false;
        boolean onMemberRemovedCalled = false;
        boolean onMembersRemovedCalled = false;

        Group group;
        Iterable<String> memberIds;
        Iterable<String> failedIds;
        Authorizable member;

        @Override
        public void onMemberAdded(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            this.group = group;
            this.member = member;
            onMemberAddedCalled = true;
        }

        @Override
        public void onMembersAdded(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            this.group = group;
            this.memberIds = memberIds;
            this.failedIds = failedIds;
            onMembersAddedCalled = true;
        }

        @Override
        public void onMemberRemoved(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            this.group = group;
            this.member = member;
            onMemberRemovedCalled = true;
        }

        @Override
        public void onMembersRemoved(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            this.group = group;
            this.memberIds = memberIds;
            this.failedIds = failedIds;
            onMembersRemovedCalled = true;
        }
    }

    private class TestSecurityProvider extends SecurityProviderImpl {

        private final AuthorizableActionProvider actionProvider;

        private TestSecurityProvider() {
            actionProvider = new AuthorizableActionProvider() {
                @Nonnull
                @Override
                public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
                    return ImmutableList.of(groupAction);
                }
            };
        }

        @Nonnull
        public <T> T getConfiguration(@Nonnull Class<T> configClass) {
            if (UserConfiguration.class == configClass) {
                return (T) new UserConfigurationImpl(this) {
                    @Nonnull
                    @Override
                    public ConfigurationParameters getParameters() {
                        return ConfigurationParameters.of(super.getParameters(),
                                ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider),
                                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior())
                        );
                    }

                    @Nullable
                    @Override
                    public PrincipalProvider getUserPrincipalProvider(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
                        return null;
                    }
                };
            } else {
                return super.getConfiguration(configClass);
            }
        }
    }

    String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }
}
