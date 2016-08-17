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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractGroupAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Testing {@link ImportBehavior#IGNORE} for group import
 */
public class GroupImportWithActionsTest extends AbstractImportTest {

    private final TestGroupAction groupAction = new TestGroupAction();
    private final TestActionProvider actionProvider = new TestActionProvider();

    @Override
    public void before() throws Exception {
        super.before();
        actionProvider.addAction(groupAction);
    }

    @Test
    public void testImportMembersIgnore() throws Exception {

        User user1 = getUserManager().createUser("user1", "");
        String uuid1 = getImportSession().getNode(user1.getPath()).getUUID();
        User user2 = getUserManager().createUser("user2", "");
        String uuid2 = getImportSession().getNode(user2.getPath()).getUUID();
        String nonExistingUUID = UUID.randomUUID().toString();
        String failedUUID = uuid1;

        String xml =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                        "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                        "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "      <sv:value>rep:AuthorizableFolder</sv:value>" +
                        "   </sv:property>" +
                        "   <sv:node sv:name=\"g1\">" +
                        "      <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                        "      <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                        "      <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" + "   <sv:property sv:name=\"rep:members\" sv:multiple=\"true\" sv:type=\"WeakReference\">" +
                        "         <sv:value>" + uuid1 + "</sv:value>" +
                        "         <sv:value>" + uuid2 + "</sv:value>" +
                        "         <sv:value>" + nonExistingUUID + "</sv:value>" +
                        "         <sv:value>" + failedUUID + "</sv:value>" +
                        "      </sv:property>" +
                        "   </sv:node>" +
                        "</sv:node>";

        doImport(getTargetPath(), xml);

        Group g1 = (Group) getUserManager().getAuthorizable("g1");
        assertEquals(g1.getID(), groupAction.group.getID());

        assertFalse(groupAction.onMemberAddedCalled);
        assertFalse(groupAction.onMembersAddedContentIdCalled);

        assertTrue(groupAction.onMembersAddedCalled);
        assertEquals(ImmutableSet.of(user1.getID(), user2.getID()), groupAction.memberIds);
    }

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    @Override
    protected String getTargetPath() {
        return GROUPPATH;
    }

    @Override
    protected ConfigurationParameters getConfigurationParameters() {
        Map<String, Object> userParams = new HashMap<String, Object>();
        userParams.put(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior());
        userParams.put(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider);
        return ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams));
    }

    private class TestGroupAction extends AbstractGroupAction {

        private boolean onMemberAddedCalled = false;
        private boolean onMembersAddedCalled = false;
        private boolean onMembersAddedContentIdCalled = false;

        Group group;
        Set<String> memberIds = Sets.newHashSet();

        @Override
        public void onMembersAdded(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            this.group = group;
            this.memberIds.addAll(ImmutableSet.copyOf(memberIds));
            onMembersAddedCalled = true;
        }

        @Override
        public void onMemberAdded(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            memberIds.add(member.getID());
            onMemberAddedCalled = true;
        }

        @Override
        public void onMembersAddedContentId(Group group, Iterable<String> memberContentIds, Iterable<String> failedIds, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            onMembersAddedContentIdCalled = true;
        }
    }

    private final class TestActionProvider implements AuthorizableActionProvider {

        private final List<AuthorizableAction> actions = Lists.newArrayList();

        private void addAction(AuthorizableAction action) {
            actions.add(action);
        }

        @Nonnull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
            return actions;
        }
    }
}
