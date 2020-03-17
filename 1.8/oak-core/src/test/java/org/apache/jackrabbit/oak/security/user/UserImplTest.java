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

import java.util.UUID;

import javax.jcr.Credentials;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.UserIdCredentials;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UserImplTest extends AbstractSecurityTest {

    private UserManager userMgr;
    private String uid;
    private User user;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        userMgr = getUserManager(root);
        uid = "testUser" + UUID.randomUUID();
    }

    @Override
    public void after() throws Exception {
        try {
            if (user != null) {
                user.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetCredentials() throws Exception {
        user = userMgr.createUser(uid, uid);
        root.commit();

        Credentials creds = user.getCredentials();
        assertTrue(creds instanceof CredentialsImpl);

        CredentialsImpl cImpl = (CredentialsImpl) creds;
        assertEquals(uid, cImpl.getUserId());
        assertTrue(PasswordUtil.isSame(cImpl.getPasswordHash(), uid));
    }

    @Test
    public void testGetCredentialsUserWithoutPassword() throws Exception {
        user = userMgr.createUser(uid, null);
        root.commit();

        Credentials creds = user.getCredentials();
        assertTrue(creds instanceof UserIdCredentials);
        assertEquals(uid, ((UserIdCredentials) creds).getUserId());
    }
}