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

import java.security.Principal;
import java.util.Collections;
import java.util.UUID;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for user mgt related tests
 */
public abstract class AbstractUserTest extends AbstractJCRTest {

    protected String testPw = "pw";

    protected UserManager userMgr;
    protected User user;
    protected Group group;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        userMgr = getUserManager(superuser);

        user = userMgr.createUser(createUserId(), testPw);
        group = userMgr.createGroup(createGroupId());
        superuser.save();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        try {
            if (user != null) {
                user.remove();
            }
            if (group != null) {
                group.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    protected static UserManager getUserManager(Session session) throws RepositoryException, NotExecutableException {
        if (!(session instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }
        try {
            return ((JackrabbitSession) session).getUserManager();
        } catch (UnsupportedRepositoryOperationException e) {
            throw new NotExecutableException(e.getMessage());
        } catch (UnsupportedOperationException e) {
            throw new NotExecutableException(e.getMessage());
        }
    }

    protected static Subject buildSubject(Principal p) {
        return new Subject(true, Collections.singleton(p), Collections.emptySet(), Collections.emptySet());
    }

    protected static Node getNode(Authorizable authorizable, Session session) throws NotExecutableException, RepositoryException {
        String path = authorizable.getPath();
        if (session.nodeExists(path)) {
            return session.getNode(path);
        } else {
            throw new NotExecutableException("Cannot access node for authorizable " + authorizable.getID());
        }
    }

    protected String createUserId() throws RepositoryException {
        return "testUser_" + UUID.randomUUID();
    }

    protected String createGroupId() throws RepositoryException {
        return "testGroup_" + UUID.randomUUID();
    }

    protected Principal getTestPrincipal() throws RepositoryException {
        String pn = "testPrincipal_" + UUID.randomUUID();
        return getTestPrincipal(pn);
    }

    protected Principal getTestPrincipal(String name) throws RepositoryException {
        return new PrincipalImpl(name);
    }

    protected User getTestUser(Session session) throws NotExecutableException, RepositoryException {
        Authorizable auth = getUserManager(session).getAuthorizable(session.getUserID());
        if (auth != null && !auth.isGroup()) {
            return (User) auth;
        }
        // should never happen. An Session should always have a corresponding User.
        throw new NotExecutableException("Unable to retrieve a User.");
    }

}