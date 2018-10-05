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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * TokenInfoTest...
 */
public class TokenInfoTest extends AbstractTokenTest {

    private String userId;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
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

        long expiredTime = new Date().getTime() + 7200001;
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
        reserved.put(TOKEN_ATTRIBUTE, "value");
        reserved.put(TOKEN_ATTRIBUTE_KEY, "value");
        reserved.put(TOKEN_ATTRIBUTE_EXPIRY, "value");

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
        assertEquals("public attributes",publicAttributes.size(), pubAttr.size());
        for (String key : publicAttributes.keySet()) {
            assertTrue("public attribute "+key+" not contained",pubAttr.containsKey(key));
            assertEquals("public attribute " + key,publicAttributes.get(key), pubAttr.get(key));
        }

        Map<String,String> privAttr = info.getPrivateAttributes();
        assertEquals("private attributes",privateAttributes.size(), privAttr.size());
        for (String key : privateAttributes.keySet()) {
            assertTrue("private attribute "+key+" not contained",privAttr.containsKey(key));
            assertEquals("private attribute" + key,privateAttributes.get(key), privAttr.get(key));
        }

        for (String key : reserved.keySet()) {
            assertFalse("reserved attribute "+key,privAttr.containsKey(key));
            assertFalse("reserved attribute "+key,pubAttr.containsKey(key));
        }
    }

    @Test
    public void testRemoveToken() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(info.remove());
    }

    @Test
    public void testRemoveToken2() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(info.remove());
    }

    @Test
    public void testRemoveTokenRemovesNode() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        Tree tokens = userTree.getChild(TOKENS_NODE_NAME);
        String tokenNodePath = tokens.getChildren().iterator().next().getPath();

        info.remove();
        assertFalse(root.getTree(tokenNodePath).exists());
    }

    @Test
    public void testResetTokenExpirationExpiredToken() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        long expiredTime = new Date().getTime() + 7200001;
        assertTrue(info.isExpired(expiredTime));
        assertFalse(info.resetExpiration(expiredTime));
    }

    @Test
    public void testResetTokenExpiration() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        assertFalse(info.resetExpiration(new Date().getTime()));

        long loginTime = new Date().getTime() + 3600000;
        assertFalse(info.isExpired(loginTime));
        assertTrue(info.resetExpiration(loginTime));
    }
}