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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.ValueFactory;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_LASTMODIFIEDBY;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_GROUP;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicGroupValidatorTest extends AbstractPrincipalTest {
    
    private Root r;
    private UserManager userManager;
    private User testUser;
    private Group localGroup;
    private Group dynamicGroup;

    @Override
    public void before() throws Exception {
        super.before();
        User tu = getTestUser();
        
        r = getSystemRoot();
        userManager = getUserManager(r);
        testUser = userManager.getAuthorizable(tu.getID(), User.class);
        localGroup = createTestGroup(r);
        dynamicGroup = userManager.getAuthorizable("aaa", Group.class);
        assertNotNull(dynamicGroup);
        
        registerSyncHandler(syncConfigAsMap(), idp.getName());
    }

    @Override
    public void after() throws Exception {
        try {                
            r.refresh();
            if (localGroup != null) {
                localGroup.remove();
                r.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config =  super.createSyncConfig();
        config.group().setDynamicGroups(true);
        config.user().setMembershipNestingDepth(2);
        return config;
    }

    @Override
    @NotNull Set<String> getIdpNamesWithDynamicGroups() {
        return Collections.singleton(idp.getName());
    }
    
    @Test
    public void testAddMemberDynamicGroup() throws Exception {
        dynamicGroup.addMember(userManager.getAuthorizable(USER_ID));
        try {
            r.commit();
            fail("CommitFailedException 77 expected.");
        } catch (CommitFailedException e) {
            assertEquals(77, e.getCode());
        }
    }

    @Test
    public void testAddMemberLocalGroup() throws Exception {
        localGroup.addMember(testUser);
        r.commit();

        assertTrue(localGroup.isDeclaredMember(testUser));
    }

    @Test
    public void testAddMembersProperty() throws Exception {
        Tree groupTree = r.getTree(dynamicGroup.getPath());
        assertFalse(groupTree.hasProperty(REP_MEMBERS));
        
        String uuid = r.getTree(userManager.getAuthorizable(USER_ID).getPath()).getProperty(JCR_UUID).getValue(Type.STRING);
        groupTree.setProperty(REP_MEMBERS, ImmutableList.of(uuid), Type.WEAKREFERENCES);
        try {
            r.commit();
            fail("CommitFailedException 77 expected.");
        } catch (CommitFailedException e) {
            assertEquals(77, e.getCode());
        }
    }

    @Test
    public void testAddMembersListTree() throws Exception {
        Tree groupTree = r.getTree(dynamicGroup.getPath());
        assertFalse(groupTree.hasChild(UserConstants.REP_MEMBERS_LIST));
        TreeUtil.addChild(groupTree, UserConstants.REP_MEMBERS_LIST, UserConstants.NT_REP_MEMBER_REFERENCES_LIST);
        try {
            r.commit();
            fail("CommitFailedException 77 expected.");
        } catch (CommitFailedException e) {
            assertEquals(77, e.getCode());
        }
    }

    @Test
    public void testAddMembersTree() throws Exception {
        Tree groupTree = r.getTree(dynamicGroup.getPath());
        assertFalse(groupTree.hasChild(REP_MEMBERS));
        TreeUtil.addChild(groupTree, REP_MEMBERS, UserConstants.NT_REP_MEMBERS);
        try {
            r.commit();
            fail("CommitFailedException 77 expected.");
        } catch (CommitFailedException e) {
            assertEquals(77, e.getCode());
        }
    }

    @Test
    public void testAddMembersTreeWithoutPrimaryType() throws Exception {
        NodeState ns = mock(NodeState.class);
        when(ns.getChildNode(anyString())).thenReturn(ns);
        
        String groupRoot = UserUtil.getAuthorizableRootPath(getUserConfiguration().getParameters(), AuthorizableType.GROUP);
        DynamicGroupValidatorProvider provider = new DynamicGroupValidatorProvider(getRootProvider(), getTreeProvider(), getSecurityProvider(), getIdpNamesWithDynamicGroups());
        Validator v = provider.getRootValidator(ns, ns, CommitInfo.EMPTY);
        // traverse the subtree-validator
        for (String name : PathUtils.elements(groupRoot)) {
            v = v.childNodeAdded(name, ns);
        }
        
        // add a dynamic group node
        PropertyState ps = PropertyStates.createProperty(REP_EXTERNAL_ID, new ExternalIdentityRef("gr", idp.getName()).getString(), Type.STRING);
        when(ns.getProperty(REP_EXTERNAL_ID)).thenReturn(ps);
        PropertyState primaryPs = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_REP_GROUP, Type.NAME);
        when(ns.getProperty(JCR_PRIMARYTYPE)).thenReturn(primaryPs);
        v = v.childNodeAdded("group", ns);

        // add rep:membersList child node without primary type inside the dynamic group -> must not fail
        NodeState ns2 = mock(NodeState.class);
        assertNotNull(v.childNodeAdded(REP_MEMBERS_LIST, ns2));
    }
    
    @Test
    public void testAddMembersToPreviouslySyncedGroup() throws Exception {
        User second = userManager.getAuthorizable(TestIdentityProvider.ID_SECOND_USER, User.class);
        assertNotNull(second);
        assertFalse(second.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        Group secondGroup = userManager.getAuthorizable("secondGroup", Group.class);
        assertNotNull(secondGroup);
        Tree groupTree = r.getTree(secondGroup.getPath());
        assertTrue(groupTree.hasProperty(REP_MEMBERS));
        
        // adding members to a group that has been synced-before must not succeed as the DynamicSyncContext will
        // eventually migrate them to dynamic membership
        secondGroup.addMember(userManager.getAuthorizable(USER_ID));
        try {
            r.commit();
            fail("CommitFailedException 77 expected.");
        } catch (CommitFailedException e) {
            assertEquals(77, e.getCode());
        }
    }
    
    @Test
    public void testAddProperties() throws Exception {
        Tree groupTree = r.getTree(dynamicGroup.getPath());
        ValueFactory vf = getValueFactory(r);
        
        dynamicGroup.setProperty("rel/path/test", vf.createValue("value"));
        r.commit();
        assertTrue(groupTree.hasChild("rel"));

        groupTree.setProperty("test", "value");
        r.commit();
        assertTrue(dynamicGroup.hasProperty("test"));
    }

    @Test
    public void testModifyProperties() throws Exception {
        ValueFactory vf = getValueFactory(r);

        dynamicGroup.setProperty("rel/path/test", vf.createValue("value"));
        r.commit();
        assertTrue(dynamicGroup.hasProperty("rel/path/test"));

        dynamicGroup.setProperty("rel/path/test", vf.createValue("value2"));
        r.commit();
        assertTrue(dynamicGroup.hasProperty("rel/path/test"));
    }

    @Test
    public void testModifyPropertiesLocalGroup() throws Exception {
        ValueFactory vf = getValueFactory(r);

        localGroup.setProperty("test", vf.createValue("value"));
        r.commit();
        assertTrue(localGroup.hasProperty("test"));

        localGroup.setProperty("test", vf.createValue("value2"));
        r.commit();
        assertTrue(localGroup.hasProperty("test"));
    }

    @Test
    public void testModifyMembersPropertyLocalGroup() throws Exception {
        localGroup.addMember(testUser);
        r.commit();

        Tree groupTree = r.getTree(localGroup.getPath());
        Tree userTree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        List<String> members = CollectionUtils.toList(groupTree.getProperty(REP_MEMBERS).getValue(Type.STRINGS));
        members.add(userTree.getProperty(JCR_UUID).getValue(Type.STRING));
        groupTree.setProperty(REP_MEMBERS, members, Type.WEAKREFERENCES);
        r.commit();

        assertEquals(2, Iterators.size(localGroup.getMembers()));
    }


    @Test
    public void testModifyFolderProperties() throws Exception {
        Tree folderTree = r.getTree(localGroup.getPath()).getParent();
        TreeUtil.addMixin(folderTree, NodeTypeConstants.MIX_LASTMODIFIED, r.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        r.commit();
        
        folderTree.setProperty(JCR_LASTMODIFIEDBY, "otherId");
        r.commit();

        assertEquals("otherId", folderTree.getProperty(JCR_LASTMODIFIEDBY).getValue(Type.STRING));
        assertEquals("otherId", folderTree.getProperty(JCR_LASTMODIFIEDBY).getValue(Type.STRING));
    }
    
    @Test
    public void testModifyMembersPropertyRemove() throws Exception {
        localGroup.addMember(testUser);
        localGroup.addMember(userManager.getAuthorizable(USER_ID));
        // mark the local group as dynamic (setting the extid property)
        String extid = new ExternalIdentityRef(localGroup.getID(), idp.getName()).getString();
        localGroup.setProperty(REP_EXTERNAL_ID, getValueFactory(r).createValue(extid));
        r.commit();
        
        Tree groupTree = r.getTree(localGroup.getPath());
        List<String> members = CollectionUtils.toList(groupTree.getProperty(REP_MEMBERS).getValue(Type.STRINGS));
        members.remove(1);
        groupTree.setProperty(REP_MEMBERS, members, Type.WEAKREFERENCES);
        r.commit();

        assertEquals(1, Iterators.size(localGroup.getMembers()));
    }

    @Test
    public void testModifyMembersPropertyAdd() throws Exception {
        localGroup.addMember(testUser);
        // mark the local group as dynamic (setting the extid property)
        String extid = new ExternalIdentityRef(localGroup.getID(), idp.getName()).getString();
        localGroup.setProperty(REP_EXTERNAL_ID, getValueFactory(r).createValue(extid));
        r.commit();

        Tree groupTree = r.getTree(localGroup.getPath());
        Tree userTree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        List<String> members = CollectionUtils.toList(groupTree.getProperty(REP_MEMBERS).getValue(Type.STRINGS));
        members.add(userTree.getProperty(JCR_UUID).getValue(Type.STRING));
        groupTree.setProperty(REP_MEMBERS, members, Type.WEAKREFERENCES);
        try {
            r.commit();
            fail("CommitFailedException 77 expected.");
        } catch (CommitFailedException e) {
            assertEquals(77, e.getCode());
        }
    }
    
    @Test
    public void testCreateDynamicGroup() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("thirdGroup", idp.getName());
        Group gr = null;
        try {
            gr = userManager.createGroup(ref.getId(), new PrincipalImpl(ref.getId()), "some/intermediate/path");
            gr.setProperty(REP_EXTERNAL_ID, getValueFactory(r).createValue(ref.getString()));
            r.commit();

            root.refresh();
            assertNotNull(getUserManager(root).getAuthorizable(ref.getId()));
        } finally {
            if (gr != null) {
                gr.remove();
                r.commit();
            }
        }
    }

    @Test
    public void testCreateGroupIncompleteExtId() throws Exception {
        Group gr = null;
        try {
            gr = userManager.createGroup("thirdGroup", new PrincipalImpl("thirdGroup"), "some/intermediate/path");
            gr.setProperty(REP_EXTERNAL_ID, getValueFactory(r).createValue("thirdGroup"));
            r.commit();

            root.refresh();
            assertNotNull(getUserManager(root).getAuthorizable("thirdGroup"));
        } finally {
            if (gr != null) {
                gr.remove();
                r.commit();
            }
        }
    }

    @Test
    public void testCreateGroupDifferentIDP() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("thirdGroup", "anotherIDP");
        Group gr = null;
        try {
            gr = userManager.createGroup(ref.getId(), new PrincipalImpl(ref.getId()), "some/intermediate/path");
            gr.setProperty(REP_EXTERNAL_ID, getValueFactory(r).createValue(ref.getString()));
            r.commit();

            root.refresh();
            assertNotNull(getUserManager(root).getAuthorizable(ref.getId()));
        } finally {
            if (gr != null) {
                gr.remove();
                r.commit();
            }
        }
    }

    @Test
    public void testCreateLocalGroup() throws Exception {
        Group gr = null;
        try {
            String id = "testGroup"+ UUID.randomUUID();
            gr = userManager.createGroup(id);
            r.commit();

            root.refresh();
            assertNotNull(getUserManager(root).getAuthorizable(id));
        } finally {
            if (gr != null) {
                gr.remove();
                r.commit();
            }
        }
    }
}