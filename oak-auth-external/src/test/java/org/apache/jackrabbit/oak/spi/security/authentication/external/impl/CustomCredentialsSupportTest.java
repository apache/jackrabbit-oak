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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModuleTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test login against the {@link ExternalLoginModule} with a setup that includes
 * a custom implementation of {@link CredentialsSupport} and an {@link ExternalIdentityProvider}
 * that deals with these supported credentials.
 */
public class CustomCredentialsSupportTest extends ExternalLoginModuleTestBase {

    private static void assertAttributes(@Nonnull Map<String, ?> expected, @Nonnull AuthInfo info) {
        assertEquals(expected.size(), info.getAttributeNames().length);
        for (String aName : info.getAttributeNames()) {
            assertEquals(expected.get(aName), info.getAttribute(aName));
        }
    }

    @Test
    public void testLogin() throws Exception {
        TestCredentials creds = new TestCredentials("testUser");

        ContentSession cs = login(creds);
        try {
            AuthInfo info = cs.getAuthInfo();
            assertEquals("testUser", info.getUserID());
            assertAttributes(getCredentialsSupport().getAttributes(creds), info);
        } finally {
            cs.close();
        }
    }

    @Test
    public void testLoginWithUnsupportedCredentials() throws Exception {
        List<Credentials> creds = ImmutableList.of(
                new SimpleCredentials("testUser", new char[0]),
                new GuestCredentials());

        for (Credentials c : creds) {
            try {
                login(c).close();
                fail("login must fail for credentials " + c);
            } catch (LoginException e) {
                // success
            }
        }
    }

    @Override
    protected ExternalIdentityProvider createIDP() {
        return new IDP();
    }

    static Credentials createTestCredentials() {
        return new TestCredentials(USER_ID);
    }

    protected CredentialsSupport getCredentialsSupport() {
        return (IDP) idp;
    }

    private static final class TestCredentials implements Credentials {

        private final String uid;

        private TestCredentials(@Nonnull String uid) {
            this.uid = uid;
        }
    }

    static final class IDP implements ExternalIdentityProvider, CredentialsSupport {

        private final Map attributes = Maps.newHashMap(ImmutableMap.of("a", "a"));

        @Nonnull
        @Override
        public String getName() {
            return "creds_test";
        }

        @CheckForNull
        @Override
        public ExternalIdentity getIdentity(@Nonnull ExternalIdentityRef ref) {
            throw new UnsupportedOperationException();
        }

        @CheckForNull
        @Override
        public ExternalUser getUser(@Nonnull String userId) {
            throw new UnsupportedOperationException();
        }

        @CheckForNull
        @Override
        public ExternalUser authenticate(@Nonnull Credentials credentials) {
            if (credentials instanceof TestCredentials) {
                final String uid = ((TestCredentials) credentials).uid;
                return new ExternalUser() {
                    @Nonnull
                    @Override
                    public ExternalIdentityRef getExternalId() {
                        return new ExternalIdentityRef(uid, getName());
                    }

                    @Nonnull
                    @Override
                    public String getId() {
                        return uid;
                    }

                    @Nonnull
                    @Override
                    public String getPrincipalName() {
                        return "principal" + uid;
                    }

                    @CheckForNull
                    @Override
                    public String getIntermediatePath() {
                        return null;
                    }

                    @Nonnull
                    @Override
                    public Iterable<ExternalIdentityRef> getDeclaredGroups() {
                        return Collections.emptySet();
                    }

                    @Nonnull
                    @Override
                    public Map<String, ?> getProperties() {
                        return Collections.emptyMap();
                    }
                };
            } else {
                return null;
            }
        }

        @CheckForNull
        @Override
        public ExternalGroup getGroup(@Nonnull String name) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Iterator<ExternalUser> listUsers() {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Iterator<ExternalGroup> listGroups() {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Set<Class> getCredentialClasses() {
            return ImmutableSet.<Class>of(TestCredentials.class);
        }

        @CheckForNull
        @Override
        public String getUserId(@Nonnull Credentials credentials) {
            if (credentials instanceof TestCredentials) {
                return ((TestCredentials) credentials).uid;
            } else {
                return null;
            }
        }

        @Nonnull
        @Override
        public Map<String, ?> getAttributes(@Nonnull Credentials credentials) {
            if (credentials instanceof TestCredentials) {
                return attributes;
            } else {
                return ImmutableMap.of();
            }
        }

        @Override
        public boolean setAttributes(@Nonnull Credentials credentials, @Nonnull Map<String, ?> attributes) {
            if (credentials instanceof TestCredentials) {
                this.attributes.putAll(attributes);
                return true;
            } else {
                return false;
            }
        }
    }
}