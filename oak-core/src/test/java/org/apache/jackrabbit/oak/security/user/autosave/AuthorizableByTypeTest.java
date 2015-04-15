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
package org.apache.jackrabbit.oak.security.user.autosave;

import java.util.UUID;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableTypeException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizableByTypeTest extends AbstractAutoSaveTest {

    private User user;
    private Group group;

    @Override
    public void before() throws Exception {
        super.before();

        user = getTestUser();
        group = mgr.createGroup("testGroup" + UUID.randomUUID());
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (group != null) {
                group.remove();
                root.commit();
            }

        } finally {
            super.after();
        }
    }

    @Test
    public void testUserByIdAndType() throws Exception {
        User u = mgr.getAuthorizable(user.getID(), User.class);
        assertTrue(u instanceof UserImpl);

        Authorizable auth = mgr.getAuthorizable(user.getID(), user.getClass());
        assertTrue(auth instanceof UserImpl);

        auth = mgr.getAuthorizable(user.getID(), Authorizable.class);
        assertTrue(auth instanceof AuthorizableImpl);
    }

    @Test
    public void testGroupByIdAndType() throws Exception {
        Group g = mgr.getAuthorizable(group.getID(), Group.class);
        assertTrue(g instanceof GroupImpl);

        Authorizable auth = mgr.getAuthorizable(group.getID(), group.getClass());
        assertTrue(auth instanceof GroupImpl);

        auth = mgr.getAuthorizable(group.getID(), Authorizable.class);
        assertTrue(auth instanceof AuthorizableImpl);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testUserByIdAndWrongType() throws Exception {
        mgr.getAuthorizable(user.getID(), Group.class);
        fail("Wrong Authorizable type is not detected.");
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testGroupByIdAndWrongType() throws Exception {
        mgr.getAuthorizable(group.getID(), User.class);
        fail("Wrong Authorizable type is not detected.");
    }
}
