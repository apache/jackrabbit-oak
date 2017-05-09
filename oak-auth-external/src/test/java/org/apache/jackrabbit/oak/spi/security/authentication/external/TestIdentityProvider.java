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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableList;

public class TestIdentityProvider implements ExternalIdentityProvider {

    public static final String ID_TEST_USER = "testUser";
    public static final String ID_SECOND_USER = "secondUser";
    public static final String ID_WILDCARD_USER = "wildcardUser";

    public static final String ID_EXCEPTION = "throw!";

    public static final String DEFAULT_IDP_NAME = "test";

    private final Map<String, ExternalGroup> externalGroups = new HashMap<String, ExternalGroup>();
    private final Map<String, ExternalUser> externalUsers = new HashMap<String, ExternalUser>();

    private final String idpName;

    public TestIdentityProvider() {
        this(DEFAULT_IDP_NAME);
    }

    public TestIdentityProvider(@Nonnull String idpName) {
        this.idpName = idpName;

        addGroup(new TestGroup("aa", getName()));
        addGroup(new TestGroup("aaa", getName()));
        addGroup(new TestGroup("a", getName()).withGroups("aa", "aaa"));
        addGroup(new TestGroup("b", getName()).withGroups("a"));
        addGroup(new TestGroup("c", getName()));
        addGroup(new TestGroup("secondGroup", getName()));
        addGroup(new TestGroup("_gr_u_", getName()));
        addGroup(new TestGroup("g%r%", getName()));

        addUser(new TestUser(ID_TEST_USER, getName())
                .withProperty("name", "Test User")
                .withProperty("profile/name", "Public Name")
                .withProperty("profile/age", 72)
                .withProperty("email", "test@testuser.com")
                .withGroups("a", "b", "c")
        );

        addUser(new TestUser(ID_SECOND_USER, getName())
                .withProperty("profile/name", "Second User")
                .withProperty("age", 24)
                .withProperty("col", ImmutableList.of("v1", "v2", "v3"))
                .withProperty("boolArr", new Boolean[]{true, false})
                .withProperty("charArr", new char[]{'t', 'o', 'b'})
                .withProperty("byteArr", new byte[0])
                .withGroups("secondGroup"));

        addUser(new TestUser(ID_WILDCARD_USER, getName())
                .withGroups("_gr_u_", "g%r%"));
    }

    public void addUser(TestIdentity user) {
        externalUsers.put(user.getId().toLowerCase(), (ExternalUser) user);
    }

    private void addGroup(TestIdentity group) {
        externalGroups.put(group.getId().toLowerCase(), (ExternalGroup) group);
    }

    @Nonnull
    @Override
    public String getName() {
        return idpName;
    }

    @Override
    public ExternalIdentity getIdentity(@Nonnull ExternalIdentityRef ref) throws ExternalIdentityException {
        if (ID_EXCEPTION.equals(ref.getId())) {
            throw new ExternalIdentityException(ID_EXCEPTION);
        }

        ExternalIdentity id = externalUsers.get(ref.getId().toLowerCase());
        if (id != null) {
            return id;
        }
        return externalGroups.get(ref.getId().toLowerCase());
    }

    @Override
    public ExternalUser getUser(@Nonnull String userId) throws ExternalIdentityException {
        if (ID_EXCEPTION.equals(userId)) {
            throw new ExternalIdentityException(ID_EXCEPTION);
        }
        return externalUsers.get(userId.toLowerCase());
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
        if (ID_EXCEPTION.equals(name)) {
            throw new ExternalIdentityException(ID_EXCEPTION);
        }
        return externalGroups.get(name.toLowerCase());
    }

    @Nonnull
    @Override
    public Iterator<ExternalUser> listUsers() throws ExternalIdentityException {
        return externalUsers.values().iterator();
    }

    @Nonnull
    @Override
    public Iterator<ExternalGroup> listGroups() throws ExternalIdentityException {
        return externalGroups.values().iterator();
    }

    public static class TestIdentity implements ExternalIdentity {

        private final String userId;
        private final String principalName;
        private final ExternalIdentityRef id;

        private final Set<ExternalIdentityRef> groups = new HashSet<ExternalIdentityRef>();
        private final Map<String, Object> props = new HashMap<String, Object>();

        public TestIdentity() {
            this("externalId", "principalName", DEFAULT_IDP_NAME);
        }

        public TestIdentity(@Nonnull String userId) {
            this(userId, userId, DEFAULT_IDP_NAME);
        }

        public TestIdentity(@Nonnull String userId, @Nonnull String principalName, @Nonnull String idpName) {
            this.userId = userId;
            this.principalName = principalName;
            id = new ExternalIdentityRef(userId, idpName);
        }

        public TestIdentity(@Nonnull ExternalIdentity base) {
            userId = base.getId();
            principalName = base.getPrincipalName();
            id = base.getExternalId();
        }

        @Nonnull
        @Override
        public String getId() {
            return userId;
        }

        @Nonnull
        @Override
        public String getPrincipalName() {
            return principalName;
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

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return groups;
        }

        @Nonnull
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
                groups.add(new ExternalIdentityRef(grp, id.getProviderName()));
            }
            return this;
        }
    }

    public static class TestUser extends TestIdentity implements ExternalUser {

        public TestUser(String userId, @Nonnull String idpName) {
            super(userId, userId, idpName);
        }

        public String getPassword() {
            return "";
        }

    }

    public static class TestGroup extends TestIdentity implements ExternalGroup {

        public TestGroup(@Nonnull String userId, @Nonnull String idpName) {
            super(userId, userId, idpName);
        }

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredMembers() throws ExternalIdentityException {
            return null;
        }
    }

    public static final class ForeignExternalUser extends TestIdentityProvider.TestIdentity implements ExternalUser {

        public ForeignExternalUser() {
            super("externalId", "principalName", "AnotherExternalIDP");
        }
    }

    public static final class ForeignExternalGroup extends TestIdentityProvider.TestIdentity implements ExternalGroup {

        public ForeignExternalGroup() {
            super("externalId", "principalName", "AnotherExternalIDP");
        }

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredMembers() {
            return ImmutableList.of();
        }
    }
}