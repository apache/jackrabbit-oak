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

package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * 2 test cases copied from {@link LdapIdentityProviderTest} to be executed with different combinations of 
 * {@link LdapProviderConfig#useSSL()} and {@link LdapProviderConfig#useTLS()}.
 */
@RunWith(Parameterized.class)
public class LdapIdentityProviderUseSSLTest extends AbstractLdapIdentityProviderTest {
    
    private static final String PROTOCOL = "TLSv1.2";
    
    @Parameterized.Parameters(name = "LdapConfiguration with {2}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] {false, false, null, "useSSL=false, useTLS=false, enabled_protocols=NA"},
                new Object[] {true, false, null, "useSSL=true, useTLS=false, enabled_protocols=NA"},
                new Object[] {true, false, new String[] {PROTOCOL}, "useSSL=true, useTLS=false, enabled_protocols=["+PROTOCOL+"]"},
                new Object[] {false, true, null, "useSSL=false, useTLS=true, enabled_protocols=NA"},
                new Object[] {false, true, new String[] {PROTOCOL}, "useSSL=false, useTLS=true, enabled_protocols=["+PROTOCOL+"]"},
                new Object[] {true, true, new String[0], "useSSL=true, useTLS=true, enabled_protocols=[]"}
        );
    }
    
    private final String[] enabledProtocols;

    public LdapIdentityProviderUseSSLTest(boolean useSSL, boolean useTLS, @Nullable String[] enabledProtocols, @NotNull String name) {
        super();
        this.useSSL = useSSL;
        this.useTLS = useTLS;
        this.enabledProtocols = enabledProtocols;
    }

    @Override
    @NotNull
    protected LdapProviderConfig createProviderConfig(@NotNull String[] userProperties) {
        LdapProviderConfig config = super.createProviderConfig(userProperties);
        config.setUseSSL(useSSL);
        config.setUseTLS(useTLS);
        config.setNoCertCheck(true);
        if (enabledProtocols != null) {
            config.setEnabledProtocols(enabledProtocols);
        }
        return config;
    }

    @Test
    public void testAuthenticate() throws Exception {
        assertAuthenticate(idp, TEST_USER1_UID, TEST_USER1_DN, TEST_USER1_DN);
    }

    @Test
    public void testGetUserByUserId() throws Exception {
        ExternalUser user = idp.getUser(TEST_USER1_UID);
        assertNotNull("User 1 must exist", user);
        assertEquals("User Ref", TEST_USER1_DN, ((LdapUser)user).getEntry().getDn().getName());
    }
}
