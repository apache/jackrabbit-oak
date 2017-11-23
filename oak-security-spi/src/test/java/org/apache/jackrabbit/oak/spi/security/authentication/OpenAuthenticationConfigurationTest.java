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
package org.apache.jackrabbit.oak.spi.security.authentication;

import java.security.Principal;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OpenAuthenticationConfigurationTest {

    private final OpenAuthenticationConfiguration authConfig = new OpenAuthenticationConfiguration();

    private LoginContextProvider lcp;

    @Before
    public void before() {
        lcp = authConfig.getLoginContextProvider(null);
    }

    @Test
    public void testLoginContextProvider() {
        assertNotNull(lcp);
    }

    @Test
    public void testLoginContextSubject() throws LoginException {
        Credentials creds = new Credentials() {};
        LoginContext ctx = lcp.getLoginContext(creds, null);
        assertNotNull(ctx);

        Subject subject = ctx.getSubject();
        assertEquals(new Subject(true, ImmutableSet.<Principal>of(), ImmutableSet.of(), ImmutableSet.of(creds)), subject);
    }

    @Test
    public void testLogin() throws LoginException {
        // nop => must not throw
        lcp.getLoginContext(new Credentials() {}, null).login();
    }

    @Test
    public void testLogout() throws LoginException {
        // nop => must not throw
        lcp.getLoginContext(new Credentials() {}, null).logout();
    }
}