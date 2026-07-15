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

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableTypeException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.test.NotExecutableException;

public class AuthorizableByTypeTestTest extends AbstractUserTest {

    private Authorizable auth;
    private Group testGroup;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        String uid = superuser.getUserID();
        auth = userMgr.getAuthorizable(uid);
        if (auth == null) {
            throw new NotExecutableException();
        }
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testGroup != null) {
                testGroup.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    private Group createTestGroup() throws RepositoryException {
        testGroup = userMgr.createGroup(createGroupId());
        superuser.save();
        return testGroup;
    }

    public void testByIdAndType() throws Exception {
        User user = userMgr.getAuthorizable(auth.getID(), User.class);
        assertEquals("Equal ID expected", auth.getID(), user.getID());

        Authorizable auth2 = userMgr.getAuthorizable(auth.getID(), auth.getClass());
        assertEquals("Equal ID expected", auth.getID(), auth2.getID());
        assertFalse(auth2.isGroup());

        auth2 = userMgr.getAuthorizable(auth.getID(), Authorizable.class);
        assertEquals("Equal ID expected", auth.getID(), auth2.getID());
        assertFalse(auth2.isGroup());
    }

    public void testByIdAndWrongType() throws Exception {
        try {
            userMgr.getAuthorizable(auth.getID(), Group.class);
            fail("Wrong Authorizable type is not detected.");
        } catch (AuthorizableTypeException ignore) {
            // success
        }
    }

    public void testNonExistingByIdAndType() throws NotExecutableException, RepositoryException {
        Authorizable nonExisting = userMgr.getAuthorizable("nonExistingAuthorizable", User.class);
        assertNull(nonExisting);

        assertNull(userMgr.getAuthorizable("nonExistingAuthorizable", Authorizable.class));
        assertNull(userMgr.getAuthorizable("nonExistingAuthorizable", User.class));
        assertNull(userMgr.getAuthorizable("nonExistingAuthorizable", Group.class));
    }

    public void testGroupByIdAndType() throws Exception {
        Group testGroup = createTestGroup();
        Group gr = userMgr.getAuthorizable(testGroup.getID(), Group.class);
        assertEquals("Equal ID expected", testGroup.getID(), gr.getID());

        Authorizable auth2 = userMgr.getAuthorizable(testGroup.getID(), testGroup.getClass());
        assertEquals("Equal ID expected", testGroup.getID(), auth2.getID());
        assertTrue(auth2.isGroup());

        auth2 = userMgr.getAuthorizable(testGroup.getID(), Authorizable.class);
        assertEquals("Equal ID expected", testGroup.getID(), auth2.getID());
        assertTrue(auth2.isGroup());
    }

    public void testGroupByIdAndWrongType() throws Exception {
        try {
            Group testGroup = createTestGroup();
            userMgr.getAuthorizable(testGroup.getID(), User.class);
            fail("Wrong Authorizable type is not detected.");
        } catch (AuthorizableTypeException ignore) {
            // success
        }
    }

    public void testByIdAndNullType() throws Exception {
        try {
            userMgr.getAuthorizable(superuser.getUserID(), null);
            fail("Wrong Authorizable type is not detected.");
        } catch (AuthorizableTypeException e) {
            // success
        }
    }

    public void testNonExistingByNullType() throws Exception {
        Authorizable authorizable = userMgr.getAuthorizable("nonExistingAuthorizable", null);
        assertNull(authorizable);
    }
}
