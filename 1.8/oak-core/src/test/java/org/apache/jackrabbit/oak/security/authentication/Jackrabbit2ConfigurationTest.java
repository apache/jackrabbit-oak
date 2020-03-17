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
import java.util.Collections;
import java.util.Set;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test login with {@link ConfigurationUtil#getJackrabbit2Configuration(org.apache.jackrabbit.oak.spi.security.ConfigurationParameters)}
 */
public class Jackrabbit2ConfigurationTest extends AbstractSecurityTest {

    @Override
    protected Configuration getConfiguration() {
        return ConfigurationUtil.getJackrabbit2Configuration(ConfigurationParameters.EMPTY);
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = login(null);
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new GuestCredentials());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testInvalidSimpleCredentials() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = new SimpleCredentials("test", new char[0]);
            cs = login(sc);
            fail("Invalid simple credentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testInvalidSimpleCredentialsWithAttribute() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = new SimpleCredentials("test", new char[0]);
            sc.setAttribute(".token", "");

            cs = login(sc);
            fail("Invalid simple credentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleCredentials() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(getAdminCredentials());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleCredentialsWithAttribute() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
            sc.setAttribute(".token", "");
            cs = login(sc);
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testTokenAuthInfo() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
            sc.setAttribute(".token", "");
            cs = login(sc);
            assertEquals("userid must be correct", "admin", cs.getAuthInfo().getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testTokenCreationAndLogin() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
            sc.setAttribute(".token", "");
            cs = login(sc);

            Object token = sc.getAttribute(".token").toString();
            assertNotNull(token);
            TokenCredentials tc = new TokenCredentials(token.toString());

            cs.close();
            cs = login(tc);
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testTokenCreationAndImpersonation() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
            sc.setAttribute(".token", "");

            ImpersonationCredentials ic = new ImpersonationCredentials(sc, new AuthInfoImpl(((SimpleCredentials) getAdminCredentials()).getUserID(), Collections.<String, Object>emptyMap(), Collections.<Principal>emptySet()));
            cs = login(ic);

            Object token = sc.getAttribute(".token").toString();
            assertNotNull(token);
            TokenCredentials tc = new TokenCredentials(token.toString());

            cs.close();
            cs = login(tc);
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testInvalidTokenCredentials() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new TokenCredentials("invalid"));
            fail("Invalid token credentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testValidTokenCredentials() throws Exception {
        Root root = adminSession.getLatestRoot();
        TokenConfiguration tc = getSecurityProvider().getConfiguration(TokenConfiguration.class);
        TokenProvider tp = tc.getTokenProvider(root);

        SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
        TokenInfo info = tp.createToken(sc.getUserID(), Collections.<String, Object>emptyMap());

        ContentSession cs = login(new TokenCredentials(info.getToken()));
        try {
            assertEquals(sc.getUserID(), cs.getAuthInfo().getUserID());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testTokenCreationWithAttributes() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
            sc.setAttribute(".token", "");
            sc.setAttribute(".token.mandatory", "something");
            sc.setAttribute("attr", "val");

            cs = login(sc);

            AuthInfo ai = cs.getAuthInfo();
            Set<String> attrNames = ImmutableSet.copyOf(ai.getAttributeNames());
            assertTrue(attrNames.contains("attr"));
            assertFalse(attrNames.contains(".token"));
            assertFalse(attrNames.contains(".token.mandatory"));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testTokenCreationWithImpersonationAttributes() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
            sc.setAttribute(".token", "");
            sc.setAttribute(".token.mandatory", "something");
            sc.setAttribute("attr", "val");

            ImpersonationCredentials ic = new ImpersonationCredentials(sc, new AuthInfoImpl(((SimpleCredentials) getAdminCredentials()).getUserID(), Collections.<String, Object>emptyMap(), Collections.<Principal>emptySet()));
            cs = login(ic);

            AuthInfo ai = cs.getAuthInfo();
            Set<String> attrNames = ImmutableSet.copyOf(ai.getAttributeNames());
            assertTrue(attrNames.contains("attr"));
            assertFalse(attrNames.contains(".token"));
            assertFalse(attrNames.contains(".token.mandatory"));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testTokenLoginForDisabledUser() throws Exception {
        ContentSession cs = null;
        try {
            User user = getTestUser();
            SimpleCredentials sc = new SimpleCredentials(user.getID(), user.getID().toCharArray());
            sc.setAttribute(".token", "");
            cs = login(sc);

            user.disable("disabled");
            root.commit();

            Object token = sc.getAttribute(".token").toString();
            assertNotNull(token);
            TokenCredentials tc = new TokenCredentials(token.toString());

            cs.close();
            cs = login(tc);
            fail("token login for a disabled user must fail.");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }
}