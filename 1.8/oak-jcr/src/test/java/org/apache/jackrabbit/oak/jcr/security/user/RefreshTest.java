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
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.test.RepositoryStub;
import org.junit.Test;

/**
 * Tests asserting that auto-refresh on session is properly propagated to the
 * user management API.
 */
public class RefreshTest extends AbstractUserTest {

    private Session adminSession = null;
    private UserManager adminUserManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        String adminId = getHelper().getProperty(RepositoryStub.PROP_PREFIX + '.' + RepositoryStub.PROP_SUPERUSER_NAME);
        String adminPw = getHelper().getProperty(RepositoryStub.PROP_PREFIX + '.' + RepositoryStub.PROP_SUPERUSER_PWD);
        SimpleCredentials credentials = new SimpleCredentials(adminId, adminPw.toCharArray());
        credentials.setAttribute(RepositoryImpl.REFRESH_INTERVAL, 0);
        adminSession = getHelper().getRepository().login(credentials);
        adminUserManager = ((JackrabbitSession) adminSession).getUserManager();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (adminSession != null) {
                adminSession.logout();
            }
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testGetAuthorizable() throws RepositoryException {
        User user = null;
        try {
            String uid = createUserId();
            user = userMgr.createUser(uid, uid);
            superuser.save();

            assertNotNull(adminUserManager.getAuthorizable(uid));
        } finally {
            if (user != null) {
                user.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAuthorizableGetProperty() throws RepositoryException {
        User user = null;
        try {
            String uid = createUserId();
            user = userMgr.createUser(uid, uid);
            superuser.save();

            Authorizable a = adminUserManager.getAuthorizable(uid);

            user.setProperty("prop", superuser.getValueFactory().createValue("val"));
            superuser.save();

            assertNotNull(a.getProperty("prop"));

        } finally {
            if (user != null) {
                user.remove();
                superuser.save();
            }
        }
    }
}