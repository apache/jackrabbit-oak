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

import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test user/group creation with intermediate path parameter.
 */
public class IntermediatePathTest extends AbstractSecurityTest {

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private Authorizable createAuthorizable(boolean createGroup, @Nullable String intermediatePath) throws RepositoryException {
        String id = UUID.randomUUID().toString();
        if (createGroup) {
            return getUserManager(root).createGroup(id, new PrincipalImpl(id), intermediatePath);
        } else {
            return getUserManager(root).createUser(id, null, new PrincipalImpl(id), intermediatePath);
        }
    }

    @Test
    public void testUserNullPath() throws Exception {
        assertNotNull(createAuthorizable(false, null));
    }

    @Test
    public void testGroupNullPath() throws Exception {
        assertNotNull(createAuthorizable(true, null));
    }

    @Test
    public void testUserEmptyPath() throws Exception {
        Authorizable authorizable = createAuthorizable(false, "");
        assertFalse(UserConstants.DEFAULT_USER_PATH.equals(PathUtils.getAncestorPath(authorizable.getPath(), 1)));
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_USER_PATH));
    }

    @Test
    public void testGroupEmptyPath() throws Exception {
        Authorizable authorizable = createAuthorizable(true, "");
        assertFalse(UserConstants.DEFAULT_GROUP_PATH.equals(PathUtils.getAncestorPath(authorizable.getPath(), 1)));
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_GROUP_PATH));
    }

    @Test
    public void testUserRelativePath() throws Exception {
        Authorizable authorizable = createAuthorizable(false, "a/b/c");
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_USER_PATH + "/a/b/c"));
    }

    @Test
    public void testGroupRelativePath() throws Exception {
        Authorizable authorizable = createAuthorizable(true, "a/b/c");
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_GROUP_PATH + "/a/b/c"));
    }

    @Test
    public void testUserAbsolutePath() throws Exception {
        Authorizable authorizable = createAuthorizable(false, UserConstants.DEFAULT_USER_PATH + "/a/b/c");
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_USER_PATH + "/a/b/c"));
    }

    @Test
    public void testGroupAbsolutePath() throws Exception {
        Authorizable authorizable = createAuthorizable(true, UserConstants.DEFAULT_GROUP_PATH + "/a/b/c");
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_GROUP_PATH + "/a/b/c"));
    }

    @Test
    public void testUserRootPath() throws Exception {
        Authorizable authorizable = createAuthorizable(false, UserConstants.DEFAULT_USER_PATH);
        assertFalse(UserConstants.DEFAULT_USER_PATH.equals(PathUtils.getAncestorPath(authorizable.getPath(), 1)));
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_USER_PATH));
    }

    @Test
    public void testGroupRootPath() throws Exception {
        Authorizable authorizable = createAuthorizable(true, UserConstants.DEFAULT_GROUP_PATH);
        assertFalse(UserConstants.DEFAULT_GROUP_PATH.equals(PathUtils.getAncestorPath(authorizable.getPath(), 1)));
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_GROUP_PATH));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testUserWrongRoot() throws Exception {
        createAuthorizable(false, UserConstants.DEFAULT_GROUP_PATH);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testGroupWrongRoot() throws Exception {
        createAuthorizable(true, UserConstants.DEFAULT_USER_PATH);
    }

    @Test
    public void testInvalidAbsolutePaths() throws Exception {
        new NodeUtil(root.getTree("/")).addChild("testNode", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        List<String> invalidPaths = ImmutableList.of(
                "/",
                PathUtils.getAncestorPath(UserConstants.DEFAULT_GROUP_PATH, 1),
                PathUtils.getAncestorPath(UserConstants.DEFAULT_GROUP_PATH, 2),
                "/testNode",
                "/nonExisting");
        for (String absPath : invalidPaths) {
            try {
                createAuthorizable(false, absPath);
                fail("Invalid path " + absPath + " outside of configured scope.");
            } catch (ConstraintViolationException e) {
                // success
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testAbsolutePathsWithParentElements() throws Exception {
        Authorizable authorizable = createAuthorizable(true, UserConstants.DEFAULT_GROUP_PATH + "/a/../b");
        assertTrue(authorizable.getPath().startsWith(UserConstants.DEFAULT_GROUP_PATH + "/b"));
    }

    @Test
    public void testAbsolutePathsWithInvalidParentElements() throws Exception {
        try {
            String invalidPath = UserConstants.DEFAULT_GROUP_PATH + "/../a";
            Authorizable authorizable = createAuthorizable(true, invalidPath);
            assertTrue(authorizable.getPath().startsWith(PathUtils.getAncestorPath(UserConstants.DEFAULT_GROUP_PATH, 1) + "/a"));
            root.commit();
            fail("Invalid path " + invalidPath + " outside of configured scope.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(28, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRelativePaths() throws Exception {
        new NodeUtil(root.getTree("/")).addChild("testNode", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        List<String> invalidPaths = ImmutableList.of("..", "../..", "../../..", "../../../testNode","a/b/../../../c");
        for (String relPath : invalidPaths) {
            try {
                Authorizable authorizable = createAuthorizable(false, relPath);
                // NOTE: requires commit to detect the violation
                root.commit();
                fail("Invalid path " + relPath + " outside of configured scope.");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isConstraintViolation());
                assertEquals(28, e.getCode());
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testCurrentRelativePath() throws Exception {
        Authorizable authorizable = createAuthorizable(false, ".");
        assertEquals(UserConstants.DEFAULT_USER_PATH, PathUtils.getAncestorPath(authorizable.getPath(), 1));
    }
}