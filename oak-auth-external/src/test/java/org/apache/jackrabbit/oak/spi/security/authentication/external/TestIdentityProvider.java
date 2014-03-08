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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

public class TestIdentityProvider implements ExternalIdentityProvider {

    private final Map<String, TestGroup> externalGroups = new HashMap<String, TestGroup>();
    private final Map<String, TestUser> externalUsers = new HashMap<String, TestUser>();


    public TestIdentityProvider() {
        addGroup(new TestGroup("a").withGroups("aa", "aaa"));
        addGroup(new TestGroup("b").withGroups("a"));
        addGroup(new TestGroup("c"));

        addUser(new TestUser("testUser")
                .withProperty("name", "Test User")
                .withProperty("profile/name", "Public Name")
                .withProperty("profile/age", 72)
                .withProperty("email", "test@testuser.com")
                .withGroups("a", "b", "c")
        );
    }

    private void addUser(TestIdentity user) {
        externalUsers.put(user.getId(), (TestUser) user);
    }

    private void addGroup(TestIdentity group) {
        externalGroups.put(group.getId(), (TestGroup) group);
    }

    @Nonnull
    @Override
    public String getName() {
        return "test";
    }

    @Override
    public ExternalIdentity getIdentity(@Nonnull ExternalIdentityRef ref) throws ExternalIdentityException {
        return null;
    }

    @Override
    public ExternalUser getUser(@Nonnull String userId) throws ExternalIdentityException {
        return externalUsers.get(userId);
    }

    @Override
    public ExternalUser authenticate(@Nonnull Credentials credentials) throws ExternalIdentityException, LoginException {
        if (!(credentials instanceof SimpleCredentials)) {
            return null;
        }
        SimpleCredentials creds = (SimpleCredentials) credentials;
        TestUser user = (TestUser) getUser(creds.getUserID());
        if (user != null) {
            if (!new String(creds.getPassword()).equals(user.getPassword())) {
                throw new LoginException("Invalid User/Password");
            }
        }
        return user;
    }

    @Override
    public ExternalGroup getGroup(@Nonnull String name) throws ExternalIdentityException {
        return externalGroups.get(name);
    }

    private static class TestIdentity implements ExternalIdentity {

        private final String userId;
        private final ExternalIdentityRef id;

        private final Set<ExternalIdentityRef> groups = new HashSet<ExternalIdentityRef>();
        private final Map<String, Object> props = new HashMap<String, Object>();

        private TestIdentity(String userId) {
            this.userId = userId;
            id = new ExternalIdentityRef(userId, "test");
        }

        @Override
        public String getId() {
            return userId;
        }

        @Override
        public String getPrincipalName() {
            return userId;
        }

        @Nonnull
        @Override
        public ExternalIdentityRef getExternalId() {
            return id;
        }

        @Override
        public String getIntermediatePath() {
            return null;
        }

        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return groups;
        }

        @Override
        public Map<String, ?> getProperties() {
            return props;
        }

        protected TestIdentity withProperty(String name, Object value) {
            props.put(name, value);
            return this;
        }

        protected TestIdentity withGroups(String ... grps) {
            for (String grp: grps) {
                groups.add(new ExternalIdentityRef(grp, "test"));
            }
            return this;
        }
    }

    private static class TestUser extends TestIdentity implements ExternalUser {

        private TestUser(String userId) {
            super(userId);
        }

        public String getPassword() {
            return "";
        }

    }

    private static class TestGroup extends TestIdentity implements ExternalGroup {

        private TestGroup(String userId) {
            super(userId);
        }

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredMembers() throws ExternalIdentityException {
            return null;
        }
    }
}