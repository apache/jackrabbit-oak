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
package org.apache.jackrabbit.oak.security.user.action;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.GroupAction;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GroupActionTest extends AbstractSecurityTest {

    private static final String TEST_GROUP_ID = "testGroup";
    private static final String TEST_USER_PREFIX = "testUser";

    final GroupAction groupAction = mock(GroupAction.class);
    private final AuthorizableActionProvider actionProvider = securityProvider -> ImmutableList.of(groupAction);

    private User testUser01;
    private User testUser02;

    Group testGroup;

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

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userParams = ConfigurationParameters.of(
                UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider,
                ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior()
        );
        return ConfigurationParameters.of(UserConfiguration.NAME, userParams);
    }

    String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    @Test
    public void testMemberAdded() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");

        testGroup.addMember(testUser01);
        verify(groupAction, times(1)).onMemberAdded(testGroup, testUser01, root, getNamePathMapper());
    }

    @Test
    public void testMemberRemoved() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");
        testGroup.addMember(testUser01);
        root.commit();

        testGroup.removeMember(testUser01);
        verify(groupAction, times(1)).onMemberRemoved(testGroup, testUser01, root, getNamePathMapper());
    }

    @Test
    public void testMembersAdded() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");
        testUser02 = getUserManager(root).createUser(TEST_USER_PREFIX + "02", "");
        testGroup.addMember(testUser02);

        Set<String> memberIds = Set.of(testUser01.getID());
        Set<String> failedIds = Set.of(testUser02.getID(), testGroup.getID());
        Iterable<String> ids = Iterables.concat(memberIds, failedIds);

        testGroup.addMembers(Iterables.toArray(ids, String.class));

        verify(groupAction, times(1)).onMembersAdded(testGroup, memberIds, failedIds, root, getNamePathMapper());
    }

    @Test
    public void testMembersAddedNonExisting() throws Exception {
        Set<String> nonExisting = Set.of("blinder", "passagier");

        testGroup.addMembers(nonExisting.toArray(new String[0]));
        verify(groupAction, times(1)).onMembersAdded(testGroup, Collections.emptySet(), nonExisting, root, getNamePathMapper());
    }

    @Test
    public void testMembersRemoved() throws Exception {
        testUser01 = getUserManager(root).createUser(TEST_USER_PREFIX + "01", "");
        testUser02 = getUserManager(root).createUser(TEST_USER_PREFIX + "02", "");
        testGroup.addMember(testUser01);

        Set<String> memberIds = Set.of(testUser01.getID());
        Set<String> failedIds = Set.of(testUser02.getID(), testGroup.getID());
        Iterable<String> ids = Iterables.concat(memberIds, failedIds);

        testGroup.removeMembers(Iterables.toArray(ids, String.class));
        verify(groupAction, times(1)).onMembersRemoved(testGroup, memberIds, failedIds, root, getNamePathMapper());
    }

    @Test
    public void testMembersRemovedNonExisting() throws Exception {
        Set<String> nonExisting = Set.of("blinder", "passagier");

        testGroup.removeMembers(nonExisting.toArray(new String[0]));
        verify(groupAction, times(1)).onMembersRemoved(testGroup, Collections.emptySet(), nonExisting, root, getNamePathMapper());
    }
}
