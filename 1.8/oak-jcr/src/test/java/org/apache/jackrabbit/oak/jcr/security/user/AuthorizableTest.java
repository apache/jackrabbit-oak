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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the {@link Authorizable} implementation.
 */
public class AuthorizableTest extends AbstractUserTest {

    /**
     * Removing an authorizable that is still listed as member of a group.
     * @throws javax.jcr.RepositoryException
     * @throws org.apache.jackrabbit.test.NotExecutableException
     */
    public void testRemoveListedAuthorizable() throws RepositoryException, NotExecutableException {
        String newUserId = null;
        Group newGroup = null;

        try {
            Principal uP = getTestPrincipal();
            User newUser = userMgr.createUser(uP.getName(), uP.getName());
            superuser.save();
            newUserId = newUser.getID();

            newGroup = userMgr.createGroup(getTestPrincipal());
            newGroup.addMember(newUser);
            superuser.save();

            // remove the new user that is still listed as member.
            newUser.remove();
            superuser.save();
        } finally {
            if (newUserId != null) {
                Authorizable u = userMgr.getAuthorizable(newUserId);
                if (u != null) {
                    if (newGroup != null) {
                        newGroup.removeMember(u);
                    }
                    u.remove();
                }
            }
            if (newGroup != null) {
                newGroup.remove();
            }
            superuser.save();
        }
    }

    @Test
    public void testObjectMethods() throws Exception {
        final Authorizable user = getTestUser(superuser);
        Authorizable user2 = getTestUser(superuser);

        assertEquals(user, user2);
        assertEquals(user.hashCode(), user2.hashCode());
        Set<Authorizable> s = new HashSet<Authorizable>();
        s.add(user);
        assertFalse(s.add(user2));

        Authorizable user3 = new Authorizable() {

            public String getID() throws RepositoryException {
                return user.getID();
            }

            public boolean isGroup() {
                return user.isGroup();
            }

            public Principal getPrincipal() throws RepositoryException {
                return user.getPrincipal();
            }

            public Iterator<Group> declaredMemberOf() throws RepositoryException {
                return user.declaredMemberOf();
            }

            public Iterator<Group> memberOf() throws RepositoryException {
                return user.memberOf();
            }

            public void remove() throws RepositoryException {
                user.remove();
            }

            public Iterator<String> getPropertyNames() throws RepositoryException {
                return user.getPropertyNames();
            }

            public Iterator<String> getPropertyNames(String relPath) throws RepositoryException {
                return user.getPropertyNames(relPath);
            }

            public boolean hasProperty(String name) throws RepositoryException {
                return user.hasProperty(name);
            }

            public void setProperty(String name, Value value) throws RepositoryException {
                user.setProperty(name, value);
            }

            public void setProperty(String name, Value[] values) throws RepositoryException {
                user.setProperty(name, values);
            }

            public Value[] getProperty(String name) throws RepositoryException {
                return user.getProperty(name);
            }

            public boolean removeProperty(String name) throws RepositoryException {
                return user.removeProperty(name);
            }

            public String getPath() throws RepositoryException {
                return user.getPath();
            }
        };

        assertFalse(user.equals(user3));
        assertTrue(s.add(user3));
    }
}