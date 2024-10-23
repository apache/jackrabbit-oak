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

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_MEMBER_REFERENCES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class RepMembersConflictHandlerTest extends AbstractSecurityTest {

    @Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { 0, "Empty Group" },
                new Object[] { 5, "Tiny Group" },
                new Object[] { MembershipWriter.DEFAULT_MEMBERSHIP_THRESHOLD - 1, "Border Group" },
                new Object[] { MembershipWriter.DEFAULT_MEMBERSHIP_THRESHOLD * 2, "Large Group" });
    }

    /**
     * The id of the test group
     */
    private static final String GROUP_ID = "test-groupId";

    private final int count;
    private Group group;
    private User[] users;

    public RepMembersConflictHandlerTest(int count, String name) {
        this.count = count;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters
                .of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT));
    }

    @Override
    public void before() throws Exception {
        super.before();
        UserManager um = getUserManager(root);
        // create a group to receive users
        group = um.createGroup(GROUP_ID);
        fillGroup(group, count);
        // create future members of the above group
        User u1 = um.createUser("u1", "pass");
        User u2 = um.createUser("u2", "pass");
        User u3 = um.createUser("u3", "pass");
        User u4 = um.createUser("u4", "pass");
        User u5 = um.createUser("u5", "pass");
        root.commit();
        users = new User[] { u1, u2, u3, u4, u5 };
    }

    void fillGroup(@NotNull Group group, int count) throws Exception {
        List<String> memberIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            memberIds.add("member" + i);
        }
        assertTrue(group.addMembers(memberIds.toArray(new String[0])).isEmpty());
    }

    @Test
    public void addExistingProperty() throws Exception {
        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        add(ours, users[1].getID());
        add(theirs, users[2].getID());

        root.refresh();
        assertTrue(group.isDeclaredMember(users[1]));
        assertTrue(group.isDeclaredMember(users[2]));
    }

    /**
     * Add-Add test
     */
    @Test
    public void changeChangedPropertyAA() throws Exception {
        add(root, users[0].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        add(ours, users[1].getID());
        add(theirs, users[2].getID());

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertTrue(group.isDeclaredMember(users[1]));
        assertTrue(group.isDeclaredMember(users[2]));
    }

    /**
     * Remove-Remove test
     */
    @Test
    public void changeChangedPropertyRR() throws Exception {
        add(root, users[0].getID(), users[1].getID(), users[2].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        rm(ours, users[1].getID());
        rm(theirs, users[2].getID());

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
        assertFalse(group.isDeclaredMember(users[2]));
    }

    /**
     * Remove-Add with different ids test. Depending on the size of the group
     * the addition might work, but the removal has to definitely work.
     */
    @Test
    public void changeChangedPropertyRA() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        rm(ours, users[1].getID());
        add(theirs, users[2].getID());

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
        // group.isDeclaredMember(users[2]) might be true or false
    }

    /**
     * Add-Remove with different ids test. Depending on the size of the group
     * the addition might work, but the removal has to definitely work.
     */
    @Test
    public void changeChangedPropertyAR() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        add(ours, users[2].getID());
        rm(theirs, users[1].getID());

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
        // group.isDeclaredMember(users[2]) might be true or false
    }

    /**
     * Remove-Add same value test. value was already part of the group
     */
    @Test
    public void changeChangedPropertyRA2() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        String id = users[1].getID();
        rm(ours, id);
        add(theirs, id);

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
    }

    /**
     * Add-Remove same value test. value was already part of the group
     */
    @Test
    public void changeChangedPropertyAR2() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        String id = users[1].getID();
        add(ours, id);
        rm(theirs, id);

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
    }

    /**
     * Remove-Add same value test. value was not part of the group
     */
    @Test
    public void changeChangedPropertyRA3() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        String id = users[2].getID();
        // we are removing an item that does not yet exist, this should
        // not overlap/conflict with the other session adding the same item
        rm(ours, id);
        add(theirs, id);

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertTrue(group.isDeclaredMember(users[1]));
        assertTrue(group.isDeclaredMember(users[2]));
    }

    /**
     * Add-Remove same value test. value was not part of the group
     */
    @Test
    public void changeChangedPropertyAR3() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        String id = users[2].getID();
        add(ours, id);
        // we are removing an item that does not yet exist, this should
        // not overlap/conflict with the other session adding the same item
        rm(theirs, id);

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertTrue(group.isDeclaredMember(users[1]));
        assertTrue(group.isDeclaredMember(users[2]));
    }

    /**
     * Add-Remove wiht single element, generating zero item group. Depending on
     * the addition might work, but the removal has to definitely work.
     */
    @Test
    public void changeChangedPropertyAR4() throws Exception {
        add(root, users[0].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        add(ours, users[1].getID());
        rm(theirs, users[0].getID());

        root.refresh();
        assertFalse(group.isDeclaredMember(users[0]));
        // group.isDeclaredMember(users[1]) might be true or false
    }

    /**
     * Delete-Changed. Delete takes precedence
     */
    @Test
    public void deleteChangedProperty() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        add(theirs, users[2].getID());
        wipeGroup(ours);

        root.refresh();
        assertFalse(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
    }

    /**
     * Changed-Deleted. Delete takes precedence
     */
    @Test
    public void changeDeletedProperty() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        wipeGroup(theirs);
        add(ours, users[2].getID());

        root.refresh();
        assertFalse(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
    }

    @Test
    public void deleteDeletedTest() throws Exception {
        add(root, users[0].getID(), users[1].getID());

        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        wipeGroup(theirs);
        wipeGroup(ours);

        root.refresh();
        assertFalse(group.isDeclaredMember(users[0]));
        assertFalse(group.isDeclaredMember(users[1]));
    }

    @Test
    public void addAddedTest() throws Exception {
        Root ours = login(getAdminCredentials()).getLatestRoot();
        Root theirs = login(getAdminCredentials()).getLatestRoot();

        add(ours, users[0].getID());
        add(theirs, users[1].getID());

        root.refresh();
        assertTrue(group.isDeclaredMember(users[0]));
        assertTrue(group.isDeclaredMember(users[1]));
    }

    private void add(Root r, String... ids) throws Exception {
        UserManager um = getUserManager(r);
        Group g = (Group) um.getAuthorizable(GROUP_ID);
        for (String id : ids) {
            g.addMember(um.getAuthorizable(id));
        }
        r.commit();
    }

    private void rm(Root r, String... ids) throws Exception {
        UserManager um = getUserManager(r);
        Group g = (Group) um.getAuthorizable(GROUP_ID);
        for (String id : ids) {
            g.removeMember(um.getAuthorizable(id));
        }
        r.commit();
    }

    private void wipeGroup(Root r) throws Exception {
        UserManager um = getUserManager(r);
        Group g = (Group) um.getAuthorizable(GROUP_ID);

        List<String> members = new ArrayList<>();
        Iterator<Authorizable> it = g.getDeclaredMembers();
        while (it.hasNext()) {
            members.add(it.next().getID());
        }
        assertTrue(g.removeMembers(members.toArray(new String[0])).isEmpty());
        // needed to trigger conflict events in the 'small group' case
        r.getTree(g.getPath()).removeProperty(UserConstants.REP_MEMBERS);
        r.commit();
    }

    @Test
    public void testAddExistingPropertyOther() throws Exception {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState ours = PropertyStates.createProperty("any", "value");
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.addExistingProperty(mock(NodeBuilder.class), ours, mock(PropertyState.class)));
    }

    @Test
    public void testChangeDeletedPropertyRepMembers() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState ours = PropertyStates.createProperty(UserConstants.REP_MEMBERS, "value");
        assertSame(ThreeWayConflictHandler.Resolution.THEIRS, handler.changeDeletedProperty(mock(NodeBuilder.class), ours, mock(PropertyState.class)));
    }

    @Test
    public void testChangeDeletedPropertyOther() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState ours = PropertyStates.createProperty("any", "value");
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.changeDeletedProperty(mock(NodeBuilder.class), ours, mock(PropertyState.class)));
    }

    @Test
    public void testDeleteDeletedPropertyRepMembers() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState base = PropertyStates.createProperty(UserConstants.REP_MEMBERS, "value");
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, handler.deleteDeletedProperty(mock(NodeBuilder.class), base));
    }

    @Test
    public void testDeleteDeletedPropertyOther() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState base = PropertyStates.createProperty("any", "value");
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteDeletedProperty(mock(NodeBuilder.class), base));
    }

    @Test
    public void testDeleteChangedPropertyRepMembers() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState theirs = PropertyStates.createProperty(UserConstants.REP_MEMBERS, "value");
        assertSame(ThreeWayConflictHandler.Resolution.OURS, handler.deleteChangedProperty(mock(NodeBuilder.class), theirs, mock(PropertyState.class)));
    }

    @Test
    public void testDeleteChangedPropertyOther() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        PropertyState theirs = PropertyStates.createProperty("any", "value");
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteChangedProperty(mock(NodeBuilder.class), theirs, mock(PropertyState.class)));
    }

    @Test
    public void testAddExistingNode() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.addExistingNode(mock(NodeBuilder.class), "name", mock(NodeState.class), mock(NodeState.class)));
    }

    @Test
    public void testChangeDeletedNodeMemberRef() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        NodeState base = when(mock(NodeState.class).getName(JCR_PRIMARYTYPE)).thenReturn(NT_REP_MEMBER_REFERENCES).getMock();
        assertSame(ThreeWayConflictHandler.Resolution.THEIRS, handler.changeDeletedNode(mock(NodeBuilder.class), "name", mock(NodeState.class), base));
    }

    @Test
    public void testChangeDeletedNodeOther() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.changeDeletedNode(mock(NodeBuilder.class), "name", mock(NodeState.class), mock(NodeState.class)));
    }

    @Test
    public void testDeleteChangedNodeMemberRef() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        NodeState base = when(mock(NodeState.class).getName(JCR_PRIMARYTYPE)).thenReturn(NT_REP_MEMBER_REFERENCES).getMock();
        assertSame(ThreeWayConflictHandler.Resolution.OURS, handler.deleteChangedNode(mock(NodeBuilder.class), "name", mock(NodeState.class), base));
    }

    @Test
    public void testDeleteChangedNodeOther() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteChangedNode(mock(NodeBuilder.class), "name", mock(NodeState.class), mock(NodeState.class)));
    }

    @Test
    public void testDeleteDeletedNodeMemberRef() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        NodeState base = when(mock(NodeState.class).getName(JCR_PRIMARYTYPE)).thenReturn(NT_REP_MEMBER_REFERENCES).getMock();
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, handler.deleteDeletedNode(mock(NodeBuilder.class), "name", base));
    }

    @Test
    public void testDeleteDeletedNodeOther() {
        RepMembersConflictHandler handler = new RepMembersConflictHandler();
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, handler.deleteDeletedNode(mock(NodeBuilder.class), "name", mock(NodeState.class)));
    }
}
