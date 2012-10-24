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

import java.util.Collections;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * LoginTest...
 */
public class TokenLoginModuleTest extends AbstractSecurityTest {

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                        TokenLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Collections.<String, Object>emptyMap());

                return new AppConfigurationEntry[] {defaultEntry};
            }
        };
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(null);
            fail("Null login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new GuestCredentials());
            fail("GuestCredentials login should fail");
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
            SimpleCredentials sc = new SimpleCredentials("admin", "admin".toCharArray());
            cs = login(sc);
            fail("Unsupported credentials login should fail");
        } catch (LoginException e) {
            // success
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
            SimpleCredentials sc = new SimpleCredentials("test", new char[0]);
            sc.setAttribute(".token", "");

            cs = login(sc);
            fail("Unsupported credentials login should fail");
        } catch (LoginException e) {
            // success
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
        Root root = admin.getLatestRoot();
        TokenProvider tp = getSecurityProvider().getTokenProvider(root);

        SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
        TokenInfo info = tp.createToken(sc.getUserID(), Collections.<String, Object>emptyMap());

        ContentSession cs = login(new TokenCredentials(info.getToken()));
        try {
            assertEquals(sc.getUserID(), cs.getAuthInfo().getUserID());
        } finally {
            cs.close();
        }

    }
}