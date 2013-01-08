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
import java.util.ArrayList;
import java.util.List;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * CreateGroupTest...
 */
public class CreateGroupTest extends AbstractUserTest {

    private List<Authorizable> createdGroups = new ArrayList();

    @Override
    protected void tearDown() throws Exception {
        // remove all created groups again
        for (Authorizable createdGroup : createdGroups) {
            try {
                createdGroup.remove();
                superuser.save();
            } finally {
                super.tearDown();
            }
        }
    }

    private Group createGroup(Principal p) throws RepositoryException {
        Group gr = userMgr.createGroup(p);
        superuser.save();
        return gr;
    }

    private Group createGroup(Principal p, String iPath) throws RepositoryException {
        Group gr = userMgr.createGroup(p, iPath);
        superuser.save();
        return gr;
    }

    @Test
    public void testCreateGroup() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        Group gr = createGroup(p);
        createdGroups.add(gr);

        assertNotNull(gr.getID());
        assertEquals(p.getName(), gr.getPrincipal().getName());
        assertFalse("A new group must not have members.",gr.getMembers().hasNext());
    }

    // TODO: check again.
//    @Test
//    public void testCreateGroupWithPath() throws RepositoryException, NotExecutableException {
//        Principal p = getTestPrincipal();
//        Group gr = createGroup(p, "/any/path/to/the/new/group");
//        createdGroups.add(gr);
//
//        assertNotNull(gr.getID());
//        assertEquals(p.getName(), gr.getPrincipal().getName());
//        assertFalse("A new group must not have members.",gr.getMembers().hasNext());
//    }

    @Test
    public void testCreateGroupWithNullPrincipal() throws RepositoryException {
        try {
            Group gr = createGroup(null);
            createdGroups.add(gr);

            fail("A Group cannot be built from 'null' Principal");
        } catch (Exception e) {
            // ok
        }

        try {
            Group gr = createGroup(null, "/any/path/to/the/new/group");
            createdGroups.add(gr);

            fail("A Group cannot be built from 'null' Principal");
        } catch (Exception e) {
            // ok
        }
    }

    @Test
    public void testCreateDuplicateGroup() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        Group gr = createGroup(p);
        createdGroups.add(gr);

        try {
            Group gr2 = createGroup(p);
            createdGroups.add(gr2);
            fail("Creating 2 groups with the same Principal should throw AuthorizableExistsException.");
        } catch (AuthorizableExistsException e) {
            // success.
        }
    }
}