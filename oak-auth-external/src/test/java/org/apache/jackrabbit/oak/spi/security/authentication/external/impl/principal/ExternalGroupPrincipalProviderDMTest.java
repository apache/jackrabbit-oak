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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.security.Principal;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.jackrabbit.api.security.user.UserManager.SEARCH_TYPE_GROUP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_SECOND_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Additional tests for {@code ExternalGroupPrincipalProvider} covering methods defined by 
 * {@link org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider} interface.
 */
@RunWith(Parameterized.class)
public class ExternalGroupPrincipalProviderDMTest extends AbstractPrincipalTest {

    @Parameterized.Parameters(name = "name={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { true, DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT+1, "Dynamic Groups Enabled, Membership-Nesting-Depth=1" },
                new Object[] { true, DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT+2, "Dynamic Groups Enabled, Membership-Nesting-Depth=2" },
                new Object[] { false, DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT, "Dynamic Groups NOT Enabled" });
    }
    
    private final boolean dynamicGroupsEnabled;
    private final long membershipNestingDepth;
    
    private Group testGroup;
    
    public ExternalGroupPrincipalProviderDMTest(boolean dynamicGroupsEnabled, int membershipNestingDepth, @NotNull String name) {
        this.dynamicGroupsEnabled = dynamicGroupsEnabled;
        this.membershipNestingDepth = membershipNestingDepth;
    }

    @Override
    public void before() throws Exception {
        super.before();

        testGroup = createTestGroup();
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            testGroup.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config =  super.createSyncConfig();
        config.group().setDynamicGroups(dynamicGroupsEnabled);
        config.user().setMembershipNestingDepth(membershipNestingDepth);
        return config;
    }

    @Override
    @NotNull Set<String> getIdpNamesWithDynamicGroups() {
        if (dynamicGroupsEnabled) {
            return Collections.singleton(idp.getName());
        } else {
            return super.getIdpNamesWithDynamicGroups();
        }
    }
    
    @Test
    public void testCoversAllMembersLocalGroup() {
        assertFalse(principalProvider.coversAllMembers(testGroup));
    }

    @Test
    public void testCoversAllMembersDifferentIDP() throws Exception {
        String extId = new ExternalIdentityRef(testGroup.getID(), "someIdp").getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));
        assertFalse(principalProvider.coversAllMembers(testGroup));
    }

    @Test
    public void testCoversAllMembers() throws Exception {
        String extId = new ExternalIdentityRef(testGroup.getID(), idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));
        assertEquals(dynamicGroupsEnabled, principalProvider.coversAllMembers(testGroup));
    }
    
    @Test
    public void testCannotAccessRepExternalId() throws Exception {
        Group gr = when(mock(Group.class).getProperty(ExternalIdentityConstants.REP_EXTERNAL_ID)).thenThrow(new RepositoryException("failure")).getMock();
        assertFalse(principalProvider.coversAllMembers(gr));
    }
    
    @Test
    public void testCoversAllMembersGroupWithMemberProperty() throws Exception {
        testGroup.addMember(getTestUser());

        String extId = new ExternalIdentityRef(testGroup.getID(), idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));
        
        assertFalse(principalProvider.coversAllMembers(testGroup));
        
        // remove members again => must be fully dynamic
        assertTrue(testGroup.removeMember(getTestUser()));
        assertEquals(dynamicGroupsEnabled, principalProvider.coversAllMembers(testGroup));
    }

    @Test
    public void testCoversAllMembersGroupWithMembersChild() throws Exception {
        Tree groupTree = DynamicGroupUtil.getTree(testGroup, root);
        String extId = new ExternalIdentityRef(testGroup.getID(), idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));

        Map<String, String> nameType = Map.of(
                UserConstants.REP_MEMBERS, UserConstants.NT_REP_MEMBERS,
                UserConstants.REP_MEMBERS_LIST, UserConstants.NT_REP_MEMBER_REFERENCES_LIST);
        
        for (Map.Entry<String,String> entry : nameType.entrySet()) {
            Tree child = TreeUtil.addChild(groupTree, entry.getKey(), entry.getValue());
            assertFalse(principalProvider.coversAllMembers(testGroup));
            child.remove();
        }
        assertEquals(dynamicGroupsEnabled, principalProvider.coversAllMembers(testGroup));
    }
    
    @Test
    public void testGetMembersLocalGroup() throws Exception {
        assertFalse(principalProvider.getMembers(testGroup, false).hasNext());
        assertFalse(principalProvider.getMembers(testGroup, true).hasNext());
        
        testGroup.addMember(getTestUser());
        root.commit();
        assertFalse(principalProvider.getMembers(testGroup, false).hasNext());
    }

    @Test
    public void testGetMembersNoResult() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));

        assertFalse(principalProvider.getMembers(testGroup, false).hasNext());
        assertFalse(principalProvider.getMembers(testGroup, true).hasNext());
    }

    @Test
    public void testGetMembers() throws Exception {
        Group gr = getUserManager(root).getAuthorizable("a", Group.class);
        if (gr != null) {
            // dynamic groups are enabled
            Iterator<Authorizable> membersDecl = principalProvider.getMembers(gr, false);
            Iterator<Authorizable> membersInh = principalProvider.getMembers(gr, true);
            assertTrue(membersDecl.hasNext());
            assertTrue(membersInh.hasNext());
            assertTrue(Iterators.elementsEqual(membersDecl, membersInh));
        }
    }

    @Test
    public void testGetMembersWithParseException() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));

        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("fail", 0));

        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        ExternalGroupPrincipalProvider pp = createPrincipalProvider(r, getUserConfiguration());

        assertFalse(pp.getMembers(testGroup, true).hasNext());
        assertFalse(pp.getMembers(testGroup, false).hasNext());
    }

    @Test
    public void testIsMemberLocalGroup() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);
        
        assertFalse(principalProvider.isMember(testGroup, user, true));
        assertFalse(principalProvider.isMember(testGroup, user, false));
        assertFalse(principalProvider.isMember(testGroup, testGroup, false));
    }

    @Test
    public void testIsMemberLocalUser() throws Exception {
        User user = getTestUser();
        assertNotNull(user);

        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));
        testGroup.addMember(user);
        
        assertFalse(principalProvider.isMember(testGroup, user, true));
        assertFalse(principalProvider.isMember(testGroup, user, false));
    }

    @Test
    public void testIsMemberGroup() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));

        assertFalse(principalProvider.isMember(testGroup, testGroup, true));
        assertFalse(principalProvider.isMember(testGroup, testGroup, false));
    }

    @Test
    public void testIsMemberNotMember() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);
        
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        testGroup.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, getValueFactory(root).createValue(extId));

        assertFalse(principalProvider.isMember(testGroup, user, true));
        assertFalse(principalProvider.isMember(testGroup, user, false));
    }

    @Test
    public void testIsMember() throws Exception {
        UserManager uMgr = getUserManager(root);
        User user = uMgr.getAuthorizable(USER_ID, User.class);
        assertNotNull(user);

        Group gr = uMgr.getAuthorizable("a", Group.class);
        if (gr != null) {
            // dynamic groups are enabled
            assertTrue(principalProvider.isMember(gr, user, true));
            assertTrue(principalProvider.isMember(gr, user, false));
        } else {
            assertFalse(principalProvider.isMember(testGroup, user, true));
            assertFalse(principalProvider.isMember(testGroup, user, false));
        }
    }

    @Test
    public void testIsMemberMissingRepExternalPrincipalNames() throws Exception {
        UserManager uMgr = getUserManager(root);
        User user = uMgr.getAuthorizable(USER_ID, User.class);
        assertNotNull(user);
        
        // remove the rep:externalPrincipalNames property (if existing)
        user.removeProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        
        // with rep:externalPrincipalNames removed principal provider must behave the same with and without
        // dynamic-groups enabled.
        assertFalse(principalProvider.isMember(testGroup, user, true));
        assertFalse(principalProvider.isMember(testGroup, user, false));
    }
    
    @Test
    public void testGetMembershipLocalGroup() throws Exception {
        assertFalse(principalProvider.getMembership(testGroup, true).hasNext());
        assertFalse(principalProvider.getMembership(testGroup, false).hasNext());
    }

    @Test
    public void testGetMembershipLocalUser() throws Exception {
        User user = getTestUser();
        assertFalse(principalProvider.getMembership(user, true).hasNext());
        assertFalse(principalProvider.getMembership(user, false).hasNext());
    }
    
    @Test
    public void testGetMembershipDeclared() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);
        
        Iterator<Group> groups = principalProvider.getMembership(user, false);
        if (dynamicGroupsEnabled) {
            assertEquals(getExpectedNumberOfGroups(), Iterators.size(groups));
        } else {
            assertFalse(groups.hasNext());
        }
    }

    @Test
    public void testGetMembershipInherited() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);

        Iterator<Group> groups = principalProvider.getMembership(user, true);
        if (dynamicGroupsEnabled) {
            assertEquals(getExpectedNumberOfGroups(), Iterators.size(groups));
        } else {
            assertFalse(groups.hasNext());
        }
    }

    @Test
    public void testGetMembershipMissingRepExternalPrincipalNames() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);

        user.removeProperty(REP_EXTERNAL_PRINCIPAL_NAMES);

        Iterator<Group> groups = principalProvider.getMembership(user, true);
        assertFalse(groups.hasNext());
    }
    
    @Test
    public void testGetMembershipIdpMismatch() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);

        // alter the rep:externalId property of the synced user to point to a different IDP
        ExternalIdentityRef ref = DefaultSyncContext.getIdentityRef(user);
        ExternalIdentityRef newRef = new ExternalIdentityRef(ref.getId(), "different");
        user.setProperty(REP_EXTERNAL_PRINCIPAL_NAMES, getValueFactory().createValue(newRef.getString()));

        Iterator<Group> groups = principalProvider.getMembership(user, true);
        assertFalse(groups.hasNext());
    }
    
    private long getExpectedNumberOfGroups() throws Exception {
        return getExpectedSyncedGroupIds(syncConfig.user().getMembershipNestingDepth(), idp, idp.getUser(USER_ID)).size();
    }

    @Test
    public void testGetMembershipEmptyPrincipalNames() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        Value extIdValue = getValueFactory(root).createValue(extId);

        User user = mock(User.class);
        when(user.getProperty(ExternalIdentityConstants.REP_EXTERNAL_ID)).thenReturn(new Value[] {extIdValue});
        when(user.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES)).thenReturn(new Value[0]);

        // empty value array
        Iterator<Group> groups = principalProvider.getMembership(user, false);
        assertFalse(groups.hasNext());
    }

    @Test
    public void testGetMembershipNullPrincipalNames() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        Value extIdValue = getValueFactory(root).createValue(extId);

        User user = mock(User.class);
        when(user.getProperty(ExternalIdentityConstants.REP_EXTERNAL_ID)).thenReturn(new Value[] {extIdValue});
        when(user.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES)).thenReturn(null);

        // rep:externalPrincipalNames is null
        Iterator<Group> groups = principalProvider.getMembership(user, false);
        assertFalse(groups.hasNext());
    }

    @Test
    public void testGetMembershipGroupNonExisting() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        Value extIdValue = getValueFactory(root).createValue(extId);
        Value[] extPrincNames = new Value[] {getValueFactory(root).createValue("nonexistingGroup")};

        User user = mock(User.class);
        when(user.getProperty(ExternalIdentityConstants.REP_EXTERNAL_ID)).thenReturn(new Value[] {extIdValue});
        when(user.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES)).thenReturn(extPrincNames);

        Iterator<Group> groups = principalProvider.getMembership(user, true);
        assertFalse(groups.hasNext());
    }

    @Test
    public void testGetMembershipResolvesToUser() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        Value extIdValue = getValueFactory(root).createValue(extId);
        Value[] extPrincNames = new Value[] {getValueFactory(root).createValue(ID_SECOND_USER)};

        User user = mock(User.class);
        when(user.getProperty(ExternalIdentityConstants.REP_EXTERNAL_ID)).thenReturn(new Value[] {extIdValue});
        when(user.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES)).thenReturn(extPrincNames);

        Iterator<Group> groups = principalProvider.getMembership(user, false);
        assertFalse(groups.hasNext());
    }

    @Test
    public void testGetMembershipLookupFails() throws Exception {
        String extId = new ExternalIdentityRef(USER_ID, idp.getName()).getString();
        Value extIdValue = getValueFactory(root).createValue(extId);
        Value[] extPrincNames = new Value[] {getValueFactory(root).createValue("a")};

        User user = mock(User.class);
        when(user.getProperty(ExternalIdentityConstants.REP_EXTERNAL_ID)).thenReturn(new Value[] {extIdValue});
        when(user.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES)).thenReturn(extPrincNames);

        UserManager um = spy(getUserManager(root));
        doThrow(new RepositoryException()).when(um).getAuthorizable(any(Principal.class));
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();
        
        ExternalGroupPrincipalProvider provider = createPrincipalProvider(root, uc);
        Iterator<Group> groups = provider.getMembership(user, false);
        assertFalse(groups.hasNext());
    }

    @Test
    public void testGetMembershipBuildsItemBasedPrincipal() throws Exception {
        User user = getUserManager(root).getAuthorizable(USER_ID, User.class);
        assertNotNull(user);
        
        Set<Principal> groupPrincipals = principalProvider.getMembershipPrincipals(user.getPrincipal());
        groupPrincipals.forEach(principal -> {
            assertTrue(principal instanceof GroupPrincipal);
            assertEquals(dynamicGroupsEnabled, principal instanceof ItemBasedPrincipal);
            if (dynamicGroupsEnabled) {
                try {
                    String path = ((ItemBasedPrincipal) principal).getPath();
                    Authorizable gr = getUserManager(root).getAuthorizable(principal);
                    assertNotNull(gr);
                    assertTrue(gr.isGroup());
                    assertEquals(gr.getPath(), path);
                } catch (RepositoryException e) {
                    fail("Failed to retrieve path for group " + principal.getName());
                }
            }
        });
    }
    
    @Test
    public void testItemBasedPrincipalGetPathFails() throws Exception {
        UserManager um = getUserManager(root);
        User user = um.getAuthorizable(USER_ID, User.class);
        assertNotNull(user);

        Set<Principal> groupPrincipals = principalProvider.getMembershipPrincipals(user.getPrincipal());
        Optional<Principal> gp = groupPrincipals.stream().filter(this::isExternalGroupPrincipal).findFirst();
        if (gp.isPresent()) {
            Principal principal = gp.get();
            assertEquals(dynamicGroupsEnabled, principal instanceof ItemBasedPrincipal);
            if (dynamicGroupsEnabled) {
                // remove the group
                Authorizable a = um.getAuthorizable(principal);
                assertNotNull(a);
                a.remove();
                root.commit();
                
                // verify that looking up the path of the principal does not succeed
                try {
                    String path = ((ItemBasedPrincipal) principal).getPath();
                    fail("Group lookup by principal returns null  -> path retrieval must fail. Found "+path);
                } catch (RepositoryException e) {
                    // success
                }
            }
        }
    }
    
    //------------------------------------------------------------------------------------------------------------------
    // principal provider methods that have short-cut for setup where all sync-configurations have dynamic groups enabled
    //------------------------------------------------------------------------------------------------------------------
    @Test
    public void testGetPrincipal() throws Exception {
        // even if group 'a' has been synchronized the external-group-p-provider must not try to find it
        // as it has been synced as regular group
        assertNull(principalProvider.getPrincipal(idp.getGroup("a").getPrincipalName()));
    }

    @Test
    public void testFindAllPrincipals() {
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP).hasNext());
    }
    
    @Test
    public void testFindPrincipals() throws ExternalIdentityException {
        String principalName = idp.getGroup("a").getPrincipalName();
        assertFalse(principalProvider.findPrincipals(principalName, SEARCH_TYPE_GROUP).hasNext());
        assertFalse(principalProvider.findPrincipals(principalName, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, Long.MAX_VALUE).hasNext());
    }
}
