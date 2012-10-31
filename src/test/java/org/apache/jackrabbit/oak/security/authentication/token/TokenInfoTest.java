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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * TokenInfoTest...
 */
public class TokenInfoTest extends AbstractSecurityTest {

    private Root root;
    private TokenProviderImpl tokenProvider;

    private String userId;
    private UserManager userManager;

    @Before
    public void before() throws Exception {
        super.before();

        root = admin.getLatestRoot();
        tokenProvider = new TokenProviderImpl(root,
                ConfigurationParameters.EMPTY,
                getSecurityProvider().getUserConfiguration());

        userId = "testUser";
        userManager = getSecurityProvider().getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);

        userManager.createUser(userId, "pw");
        root.commit();
    }

    @After
    public void after() throws Exception {
        try {
            Authorizable a = userManager.getAuthorizable(userId);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetUserId() {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertEquals(userId, info.getUserId());

        info = tokenProvider.getTokenInfo(info.getToken());
        assertEquals(userId, info.getUserId());
    }

    @Test
    public void testGetToken() {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertNotNull(info.getToken());

        info = tokenProvider.getTokenInfo(info.getToken());
        assertNotNull(info.getToken());
    }

    @Test
    public void testIsExpired() {
        long loginTime = new Date().getTime();

        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertFalse(info.isExpired(loginTime));

        loginTime = new Date().getTime() + 3600000;
        assertFalse(info.isExpired(loginTime));

        long expiredTime = new Date().getTime() + 7200000;
        assertTrue(info.isExpired(expiredTime));
    }

    @Test
    public void testMatches() {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(info.matches(new TokenCredentials(info.getToken())));

        Map<String,String> attributes = new HashMap<String, String>();
        attributes.put("something", "value");
        info = tokenProvider.createToken(userId, attributes);
        assertTrue(info.matches(new TokenCredentials(info.getToken())));

        attributes.put(".token-something", "mandatory");
        info = tokenProvider.createToken(userId, attributes);
        assertFalse(info.matches(new TokenCredentials(info.getToken())));
        TokenCredentials tc = new TokenCredentials(info.getToken());
        tc.setAttribute(".token-something", "mandatory");
        assertTrue(info.matches(tc));
        tc.setAttribute("another", "value");
        assertTrue(info.matches(tc));
        tc.setAttribute(".token_ignored", "value");
        assertTrue(info.matches(tc));
    }

    @Test
    public void testGetAttributes() {
        Map<String, String> reserved = new HashMap<String, String>();
        reserved.put(".token", "value");
        reserved.put(".token.key", "value");
        reserved.put(".token.exp", "value");

        Map<String, String> privateAttributes = new HashMap<String, String>();
        privateAttributes.put(".token_exp", "value");
        privateAttributes.put(".tokenTest", "value");
        privateAttributes.put(".token_something", "value");

        Map<String, String> publicAttributes = new HashMap<String, String>();
        publicAttributes.put("any", "value");
        publicAttributes.put("another", "value");

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.putAll(reserved);
        attributes.putAll(publicAttributes);
        attributes.putAll(privateAttributes);

        TokenInfo info = tokenProvider.createToken(userId, attributes);

        Map<String,String> pubAttr = info.getPublicAttributes();
        assertEquals(publicAttributes.size(), pubAttr.size());
        for (String key : publicAttributes.keySet()) {
            assertTrue(pubAttr.containsKey(key));
            assertEquals(publicAttributes.get(key), pubAttr.get(key));
        }

        Map<String,String> privAttr = info.getPrivateAttributes();
        assertEquals(privateAttributes.size(), privAttr.size());
        for (String key : privateAttributes.keySet()) {
            assertTrue(privAttr.containsKey(key));
            assertEquals(privateAttributes.get(key), privAttr.get(key));
        }

        for (String key : reserved.keySet()) {
            assertFalse(privAttr.containsKey(key));
            assertFalse(pubAttr.containsKey(key));
        }
    }
}