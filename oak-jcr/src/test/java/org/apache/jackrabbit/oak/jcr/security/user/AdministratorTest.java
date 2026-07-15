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

import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the administrator user.
 */
public class AdministratorTest extends AbstractUserTest {

    private User admin;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Authorizable a = userMgr.getAuthorizable(superuser.getUserID());
        if (a == null || a.isGroup()) {
            throw new NotExecutableException("Admin user does not exist");
        }
        admin = (User) a;
    }

    @Test
    public void testIsAdmin() throws NotExecutableException, RepositoryException {
        assertTrue(admin.isAdmin());
    }

    @Test
    public void testDisable() throws NotExecutableException, RepositoryException {
        try {
            admin.disable("-> out");
            superuser.save();
            fail("The admin cannot be disabled");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testRemoveAdmin() throws NotExecutableException {
        try {
            admin.remove();
            superuser.save();
            fail("The admin user cannot be removed.");
        } catch (RepositoryException e) {
            // OK superuser cannot be removed. not even by the superuser itself.
        }
    }

    @Test
    public void testRemoveAdminNode() throws RepositoryException, NotExecutableException {
        String adminId = admin.getID();
        // access the node corresponding to the admin user and remove it
        Node adminNode = superuser.getNode(admin.getPath());

        try {
            adminNode.remove();
            // use session obtained from the node as usermgr may point to a dedicated
            // system workspace different from the superusers workspace.
            superuser.save();
            fail("Admin user node cannot be removed.");
        } catch (Exception e) {
            // success -> get rid of possibly pending transient modifications
            superuser.refresh(false);
        }
    }
}