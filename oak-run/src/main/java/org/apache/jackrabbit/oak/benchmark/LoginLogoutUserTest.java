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
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;

public class LoginLogoutUserTest extends AbstractTest {

    private final static String USER = "user";

    @Override
    public void setUp(Repository repository, Credentials credentials) throws Exception {
        super.setUp(repository, new SimpleCredentials(USER, USER.toCharArray()));

        // create test user
        JackrabbitSession adminSession = (JackrabbitSession) loginAdministrative();
        User user = adminSession.getUserManager().createUser(USER, USER);
        AccessControlUtils.addAccessControlEntry(adminSession, user.getPath(), user.getPrincipal(), new String[]{Privilege.JCR_ALL}, true);
        AccessControlUtils.addAccessControlEntry(adminSession, "/", user.getPrincipal(), new String[]{Privilege.JCR_ALL}, true);
        adminSession.save();
    }

    @Override
    public void runTest() throws RepositoryException {
        Repository repository = getRepository();
        for (int i = 0; i < LoginUserTest.COUNT; i++) {
            Session session = repository.login(getCredentials());
            try {
                session.getRootNode();
            } finally {
                session.logout();
            }
        }
    }
}
