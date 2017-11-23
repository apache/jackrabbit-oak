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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

public class RepMembersConflictHandlerTest extends AbstractSecurityTest {

    /**
     * The id of the test group
     */
    private static final String GROUP_ID = "test-groupId";

    private Group group;
    private User[] users;

    @Override
    public void before() throws Exception {
        super.before();
        UserManager um = getUserManager(root);
        // create a group to receive users
        group = um.createGroup(GROUP_ID);
        // create future members of the above group
        User u1 = um.createUser("u1", "pass");
        User u2 = um.createUser("u2", "pass");
        User u3 = um.createUser("u3", "pass");
        User u4 = um.createUser("u4", "pass");
        User u5 = um.createUser("u5", "pass");
        root.commit();
        users = new User[] { u1, u2, u3, u4, u5 };
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
     * Remove-Add with different ids test
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
        assertTrue(group.isDeclaredMember(users[2]));
    }

    /**
     * Add-Remove with different ids test
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
        assertTrue(group.isDeclaredMember(users[2]));
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
        r.getTree(g.getPath()).removeProperty(UserConstants.REP_MEMBERS);
        r.commit();
    }
}
