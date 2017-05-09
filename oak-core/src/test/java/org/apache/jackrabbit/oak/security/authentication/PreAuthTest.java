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
package org.apache.jackrabbit.oak.security.authentication;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Set;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class PreAuthTest extends AbstractSecurityTest {

    private Set<Principal> principals;

    @Override
    public void before() throws Exception {
        super.before();

        principals = Collections.<Principal>singleton(new TestPrincipal());
    }

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {

            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[0];
            }
        };
    }

    @Test
    public void testValidSubject() throws Exception {
        final Subject subject = new Subject(true, principals, Collections.<Object>emptySet(), Collections.<Object>emptySet());
        ContentSession cs = Subject.doAsPrivileged(subject, new PrivilegedAction<ContentSession>() {
            @Override
            public ContentSession run() {
                try {
                    return login(null);
                } catch (Exception e) {
                    return null;
                }
            }
        }, null);

        try {
            AuthInfo authInfo = cs.getAuthInfo();
            assertNotSame(AuthInfo.EMPTY, authInfo);
            assertEquals(principals, authInfo.getPrincipals());
            assertNull(authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testValidSubjectWithCredentials() throws Exception {
        Set<SimpleCredentials> publicCreds = Collections.singleton(new SimpleCredentials("testUserId", new char[0]));
        final Subject subject = new Subject(false, principals, publicCreds, Collections.<Object>emptySet());
        ContentSession cs = Subject.doAsPrivileged(subject, new PrivilegedAction<ContentSession>() {
            @Override
            public ContentSession run() {
                try {
                    return login(null);
                } catch (Exception e) {
                    return null;
                }
            }
        }, null);

        try {
            AuthInfo authInfo = cs.getAuthInfo();
            assertNotSame(AuthInfo.EMPTY, authInfo);
            assertEquals(principals, authInfo.getPrincipals());
            assertEquals("testUserId", authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testValidReadSubjectWithCredentials() throws Exception {
        Set<SimpleCredentials> publicCreds = Collections.singleton(new SimpleCredentials("testUserId", new char[0]));
        final Subject subject = new Subject(true, principals, publicCreds, Collections.<Object>emptySet());
        ContentSession cs = Subject.doAsPrivileged(subject, new PrivilegedAction<ContentSession>() {
            @Override
            public ContentSession run() {
                try {
                    return login(null);
                } catch (Exception e) {
                    return null;
                }
            }
        }, null);

        try {
            AuthInfo authInfo = cs.getAuthInfo();
            assertNotSame(AuthInfo.EMPTY, authInfo);
            assertEquals(principals, authInfo.getPrincipals());
            assertEquals("testUserId", authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testValidSubjectWithAuthInfo() throws Exception {
        AuthInfo info = new AuthInfoImpl("testUserId", Collections.<String, Object>emptyMap(), Collections.<Principal>emptySet());
        Set<AuthInfo> publicCreds = Collections.singleton(info);
        final Subject subject = new Subject(false, Collections.singleton(new TestPrincipal()), publicCreds, Collections.<Object>emptySet());
        ContentSession cs = Subject.doAsPrivileged(subject, new PrivilegedAction<ContentSession>() {
            @Override
            public ContentSession run() {
                try {
                    return login(null);
                } catch (Exception e) {
                    return null;
                }
            }
        }, null);

        try {
            assertSame(info, cs.getAuthInfo());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSubjectAndCredentials() throws Exception {
        final Subject subject = new Subject(true, principals, Collections.<Object>emptySet(), Collections.<Object>emptySet());
        ContentSession cs = Subject.doAsPrivileged(subject, new PrivilegedAction<ContentSession>() {
            @Override
            public ContentSession run() {
                ContentSession cs;
                try {
                    cs = login(new GuestCredentials());
                    return cs;
                } catch (Exception e) {
                    return null;
                }
            }
        }, null);

        assertNull("Login should have failed.", cs);
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(null);
            fail("Null login without pre-auth subject should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSystemSubject() throws Exception {
        ContentSession cs = Subject.doAsPrivileged(SystemSubject.INSTANCE, new PrivilegedAction<ContentSession>() {
            @Override
            public ContentSession run() {
                try {
                    return login(null);
                } catch (Exception e) {
                    return null;
                }
            }
        }, null);

        try {
            AuthInfo authInfo = cs.getAuthInfo();
            assertNotSame(AuthInfo.EMPTY, authInfo);
            assertEquals(SystemSubject.INSTANCE.getPrincipals(), authInfo.getPrincipals());
            assertEquals(null, authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    private class TestPrincipal implements Principal {

        @Override
        public String getName() {
            return "test";
        }
    }
}