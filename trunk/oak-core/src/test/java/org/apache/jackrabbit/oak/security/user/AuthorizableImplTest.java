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

import java.util.HashMap;
import java.util.Map;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Implementation specific tests for {@code AuthorizableImpl} and subclasses.
 */
public class AuthorizableImplTest extends AbstractSecurityTest {

    private UserManager userMgr;
    private User testUser;
    private Group testGroup;

    private AuthorizableImpl authorizable;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        userMgr = getUserManager(root);
        testUser = getTestUser();
        testGroup = userMgr.createGroup("testGroup");
        root.commit();

        authorizable = (AuthorizableImpl) testUser;
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            if (testGroup != null) {
                testGroup.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testEqualAuthorizables() throws Exception {
        Authorizable user = userMgr.getAuthorizable(testUser.getID());
        Authorizable group = userMgr.getAuthorizable(testGroup.getID());

        Map<Authorizable, Authorizable> equalAuthorizables = new HashMap();
        equalAuthorizables.put(testUser, testUser);
        equalAuthorizables.put(testGroup, testGroup);
        equalAuthorizables.put(user, user);
        equalAuthorizables.put(group, group);
        equalAuthorizables.put(testUser, user);
        equalAuthorizables.put(testGroup, group);

        for (Map.Entry entry : equalAuthorizables.entrySet()) {
            assertEquals(entry.getKey(), entry.getValue());
            assertEquals(entry.getValue(), entry.getKey());
        }
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testNotEqualAuthorizables() throws Exception {
        UserManager otherUserManager = getUserConfiguration().getUserManager(root, getNamePathMapper());
        Authorizable user = otherUserManager.getAuthorizable(testUser.getID());
        Authorizable group = otherUserManager.getAuthorizable(testGroup.getID());

        Map<Authorizable, Authorizable> notEqual = new HashMap();
        notEqual.put(testUser, testGroup);
        notEqual.put(user, group);
        notEqual.put(testUser, user);
        notEqual.put(testGroup, group);

        for (Map.Entry entry : notEqual.entrySet()) {
            assertFalse(entry.getKey().equals(entry.getValue()));
            assertFalse(entry.getValue().equals(entry.getKey()));
        }
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testHashCode() throws Exception {
        Authorizable user = userMgr.getAuthorizable(testUser.getID());
        Authorizable group = userMgr.getAuthorizable(testGroup.getID());

        Map<Authorizable, Authorizable> sameHashCode = new HashMap();
        sameHashCode.put(testUser, testUser);
        sameHashCode.put(testGroup, testGroup);
        sameHashCode.put(user, user);
        sameHashCode.put(group, group);
        sameHashCode.put(testUser, user);
        sameHashCode.put(testGroup, group);

        for (Map.Entry entry : sameHashCode.entrySet()) {
            assertEquals(entry.getKey().hashCode(), entry.getValue().hashCode());
        }

        UserManager otherUserManager = getUserConfiguration().getUserManager(root, getNamePathMapper());
        user = otherUserManager.getAuthorizable(testUser.getID());
        group = otherUserManager.getAuthorizable(testGroup.getID());

        Map<Authorizable, Authorizable> notSameHashCode = new HashMap();
        notSameHashCode.put(testUser, testGroup);
        notSameHashCode.put(user, group);
        notSameHashCode.put(testUser, user);
        notSameHashCode.put(testGroup, group);

        for (Map.Entry entry : notSameHashCode.entrySet()) {
            assertFalse(entry.getKey().hashCode() == entry.getValue().hashCode());
        }
    }

    @Test
    public void testGetTree() throws Exception {
        Tree t = root.getTree(authorizable.getPath());
        assertEquals(t.getPath(), authorizable.getTree().getPath());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTreeNotExisting() throws Exception {
        root.getTree(authorizable.getPath()).remove();

        // getTree must throw
        authorizable.getTree();
    }

    @Test(expected = RepositoryException.class)
    public void testGetPrincipalNamePropertyMissing() throws Exception {
        Tree t = root.getTree(authorizable.getPath());
        t.removeProperty(UserConstants.REP_PRINCIPAL_NAME);

        // getPrincipalName must throw
        authorizable.getPrincipalName();
    }}