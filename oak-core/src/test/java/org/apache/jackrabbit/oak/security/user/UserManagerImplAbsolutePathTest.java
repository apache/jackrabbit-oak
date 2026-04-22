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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import java.security.Principal;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link UserManagerImpl#createUserWithAbsolutePath} and
 * {@link UserManagerImpl#createGroupWithAbsolutePath}.
 *
 * Verifies that the authorizable node is created exactly at the given absolute
 * path, with the last path segment used as the node name.
 */
public class UserManagerImplAbsolutePathTest extends AbstractUserTest {

    private static final String USER_PATH = UserConstants.DEFAULT_USER_PATH + "/test/absolute";
    private static final String GROUP_PATH = UserConstants.DEFAULT_GROUP_PATH + "/test/absolute";

    // ---- helpers ----

    private User createUser(String absolutePath) throws RepositoryException {
        String id = UUID.randomUUID().toString();
        return getUserManager(root).createUserWithAbsolutePath(id, null, new PrincipalImpl(id), absolutePath);
    }

    private Group createGroup(String absolutePath) throws RepositoryException {
        String id = UUID.randomUUID().toString();
        return getUserManager(root).createGroupWithAbsolutePath(id, new PrincipalImpl(id), absolutePath);
    }

    // ---- user: exact path placement ----

    @Test
    public void testUserCreatedAtExactPath() throws Exception {
        User user = createUser(USER_PATH);
        assertEquals(USER_PATH, user.getPath());
    }

    @Test
    public void testUserNodeNameIsLastPathSegment() throws Exception {
        String expectedNodeName = "my-user-node";
        String path = UserConstants.DEFAULT_USER_PATH + "/test/" + expectedNodeName;
        User user = createUser(path);
        assertEquals(expectedNodeName, PathUtils.getName(user.getPath()));
    }

    @Test
    public void testUserIntermediateFoldersCreated() throws Exception {
        // parent folders do not pre-exist; they must be created by the implementation
        String deepPath = UserConstants.DEFAULT_USER_PATH + "/deep/nested/folders/myuser";
        User user = createUser(deepPath);
        assertEquals(deepPath, user.getPath());
    }

    @Test
    public void testUserRetrievableByExactPath() throws Exception {
        User user = createUser(USER_PATH);
        root.commit();
        Authorizable found = getUserManager(root).getAuthorizableByPath(USER_PATH);
        assertNotNull(found);
        assertEquals(user.getID(), found.getID());
    }

    @Test
    public void testUserPasswordSet() throws Exception {
        String id = UUID.randomUUID().toString();
        User user = getUserManager(root).createUserWithAbsolutePath(id, "s3cr3t", new PrincipalImpl(id), USER_PATH);
        assertNotNull(user);
        assertEquals(USER_PATH, user.getPath());
        // password authentication is validated through credentials check, not directly readable
    }

    @Test
    public void testUserPrincipalSet() throws Exception {
        String id = UUID.randomUUID().toString();
        Principal principal = new PrincipalImpl("my-principal-" + id);
        User user = getUserManager(root).createUserWithAbsolutePath(id, null, principal, USER_PATH);
        assertEquals(principal.getName(), user.getPrincipal().getName());
    }

    // ---- user: error cases ----

    @Test(expected = ConstraintViolationException.class)
    public void testUserOutsideUserRoot() throws Exception {
        createUser(UserConstants.DEFAULT_GROUP_PATH + "/wrong/location");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testUserPathCollision() throws Exception {
        createUser(USER_PATH);
        // different ID, same absolute path → node already exists
        createUser(USER_PATH);
    }

    @Test(expected = AuthorizableExistsException.class)
    public void testUserDuplicateId() throws Exception {
        String id = UUID.randomUUID().toString();
        String path1 = UserConstants.DEFAULT_USER_PATH + "/dup/first";
        String path2 = UserConstants.DEFAULT_USER_PATH + "/dup/second";
        getUserManager(root).createUserWithAbsolutePath(id, null, new PrincipalImpl(id), path1);
        getUserManager(root).createUserWithAbsolutePath(id, null, new PrincipalImpl(id + "_2"), path2);
    }

    // ---- group: exact path placement ----

    @Test
    public void testGroupCreatedAtExactPath() throws Exception {
        Group group = createGroup(GROUP_PATH);
        assertEquals(GROUP_PATH, group.getPath());
    }

    @Test
    public void testGroupNodeNameIsLastPathSegment() throws Exception {
        String expectedNodeName = "my-group-node";
        String path = UserConstants.DEFAULT_GROUP_PATH + "/test/" + expectedNodeName;
        Group group = createGroup(path);
        assertEquals(expectedNodeName, PathUtils.getName(group.getPath()));
    }

    @Test
    public void testGroupIntermediateFoldersCreated() throws Exception {
        String deepPath = UserConstants.DEFAULT_GROUP_PATH + "/deep/nested/folders/mygroup";
        Group group = createGroup(deepPath);
        assertEquals(deepPath, group.getPath());
    }

    @Test
    public void testGroupRetrievableByExactPath() throws Exception {
        Group group = createGroup(GROUP_PATH);
        root.commit();
        Authorizable found = getUserManager(root).getAuthorizableByPath(GROUP_PATH);
        assertNotNull(found);
        assertEquals(group.getID(), found.getID());
    }

    @Test
    public void testGroupPrincipalSet() throws Exception {
        String id = UUID.randomUUID().toString();
        Principal principal = new PrincipalImpl("my-group-principal-" + id);
        Group group = getUserManager(root).createGroupWithAbsolutePath(id, principal, GROUP_PATH);
        assertEquals(principal.getName(), group.getPrincipal().getName());
    }

    // ---- group: error cases ----

    @Test(expected = ConstraintViolationException.class)
    public void testGroupOutsideGroupRoot() throws Exception {
        createGroup(UserConstants.DEFAULT_USER_PATH + "/wrong/location");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testGroupPathCollision() throws Exception {
        createGroup(GROUP_PATH);
        // different ID, same absolute path → node already exists
        createGroup(GROUP_PATH);
    }

    @Test(expected = AuthorizableExistsException.class)
    public void testGroupDuplicateId() throws Exception {
        String id = UUID.randomUUID().toString();
        String path1 = UserConstants.DEFAULT_GROUP_PATH + "/dup/first";
        String path2 = UserConstants.DEFAULT_GROUP_PATH + "/dup/second";
        getUserManager(root).createGroupWithAbsolutePath(id, new PrincipalImpl(id), path1);
        getUserManager(root).createGroupWithAbsolutePath(id, new PrincipalImpl(id + "_2"), path2);
    }
}
