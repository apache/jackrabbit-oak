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

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.SimpleCredentials;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class LookupOnValidateTest extends AbstractLdapIdentityProviderTest {

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { false, false, false, "False False" },
                new Object[] { false, true, false, "False True" },
                new Object[] { true, false, false, "True False"},
                new Object[] { true, true, false, "True True" },
                new Object[] { false, false, true, "False False useUidForExtId" },
                new Object[] { false, true, true, "False True useUidForExtId" },
                new Object[] { true, false, true, "True False useUidForExtId"},
                new Object[] { true, true, true, "True True useUidForExtId" });
    }

    private final boolean adminPoolLookupOnValidate;
    private final boolean userPoolLookupOnValidate;
    private final boolean useUidForExtId;

    private final String expectedId;

    public LookupOnValidateTest(boolean adminPoolLookupOnValidate, boolean userPoolLookupOnValidate, boolean useUidForExtId, String name) {
        this.adminPoolLookupOnValidate = adminPoolLookupOnValidate;
        this.userPoolLookupOnValidate = userPoolLookupOnValidate;
        this.useUidForExtId = useUidForExtId;

        if (useUidForExtId) {
            expectedId = TEST_USER1_UID;
        } else {
            expectedId = TEST_USER1_DN;
        }
    }

    @Override
    @NotNull
    protected LdapProviderConfig createProviderConfig(@NotNull String[] userProperties) {
        LdapProviderConfig config = super.createProviderConfig(userProperties);
        config.getAdminPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(adminPoolLookupOnValidate);
        config.getUserPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(userPoolLookupOnValidate);
        config.setUseUidForExtId(useUidForExtId);
        return config;
    }

    @Test
    public void testAuthenticateValidate() throws Exception {
        assertAuthenticate(idp, new SimpleCredentials(TEST_USER1_UID, "pass".toCharArray()), expectedId, TEST_USER1_DN);
    }
}