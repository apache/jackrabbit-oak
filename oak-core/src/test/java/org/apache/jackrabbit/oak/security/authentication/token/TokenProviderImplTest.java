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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * TokenProviderImplTest...
 */
public class TokenProviderImplTest extends AbstractSecurityTest {

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
    public void testDoCreateToken() throws Exception {
        assertFalse(tokenProvider.doCreateToken(new GuestCredentials()));
        assertFalse(tokenProvider.doCreateToken(new TokenCredentials("token")));
        assertFalse(tokenProvider.doCreateToken(getAdminCredentials()));

        SimpleCredentials sc = new SimpleCredentials("uid", "pw".toCharArray());
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute("any_attribute", "value");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(".token_key", "value");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(".token", "existing");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(".token", "");
        assertTrue(tokenProvider.doCreateToken(sc));
    }

    @Test
    public void testCreateTokenFromInvalidCredentials() throws Exception {
        List<Credentials> invalid = new ArrayList<Credentials>();
        invalid.add(new GuestCredentials());
        invalid.add(new TokenCredentials("sometoken"));
        invalid.add(new ImpersonationCredentials(new GuestCredentials(), null));
        invalid.add(new SimpleCredentials("unknownUserId", new char[0]));

        for (Credentials creds : invalid) {
            assertNull(tokenProvider.createToken(creds));
        }
    }

    @Test
    public void testCreateTokenFromCredentials() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        List<Credentials> valid = new ArrayList<Credentials>();
        valid.add(sc);
        valid.add(new ImpersonationCredentials(sc, null));

        for (Credentials creds : valid) {
            TokenInfo info = tokenProvider.createToken(creds);
            assertTokenInfo(info, userId);
        }
    }

    @Test
    public void testCreateTokenFromInvalidUserId() throws Exception {
        TokenInfo info = tokenProvider.createToken("unknownUserId", Collections.<String, Object>emptyMap());
        assertNull(info);
    }

    @Test
    public void testCreateTokenFromUserId() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTokenInfo(info, userId);
    }

    @Test
    public void testTokenNode() throws Exception {
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

        Tree userTree = root.getTree(userManager.getAuthorizable(userId).getPath());
        Tree tokens = userTree.getChild(".tokens");
        assertNotNull(tokens);
        assertEquals(1, tokens.getChildrenCount());

        Tree tokenNode = tokens.getChildren().iterator().next();
        assertNotNull(tokenNode.getProperty(".token.key"));
        assertNotNull(tokenNode.getProperty(".token.exp"));

        for (String key : reserved.keySet()) {
            PropertyState p = tokenNode.getProperty(key);
            if (p != null) {
                assertFalse(reserved.get(key).equals(p.getValue(Type.STRING)));
            }
        }

        for (String key : privateAttributes.keySet()) {
            assertEquals(privateAttributes.get(key), tokenNode.getProperty(key).getValue(Type.STRING));
        }

        for (String key : publicAttributes.keySet()) {
            assertEquals(publicAttributes.get(key), tokenNode.getProperty(key).getValue(Type.STRING));
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidToken() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add("/invalid");
        invalid.add(UUID.randomUUID().toString());

        for (String token : invalid) {
            TokenInfo info = tokenProvider.getTokenInfo(token);
            assertNull(info);
        }

        try {
            assertNull(tokenProvider.getTokenInfo("invalidToken"));
        } catch (Exception e) {
            // success
        }
    }

    @Test
    public void testGetTokenInfo() throws Exception {
        String token = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap()).getToken();
        TokenInfo info = tokenProvider.getTokenInfo(token);
        assertTokenInfo(info, userId);
    }

    @Test
    public void testRemoveTokenInvalidInfo() throws Exception {
        assertFalse(tokenProvider.removeToken(new InvalidTokenInfo()));
    }

    @Test
    public void testRemoveToken() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(tokenProvider.removeToken(info));
    }

    @Test
    public void testRemoveToken2() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(tokenProvider.removeToken(tokenProvider.getTokenInfo(info.getToken())));
    }

    @Test
    public void testRemoveTokenRemovesNode() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        Tree userTree = root.getTree(userManager.getAuthorizable(userId).getPath());
        Tree tokens = userTree.getChild(".tokens");
        String tokenNodePath = tokens.getChildren().iterator().next().getPath();

        tokenProvider.removeToken(info);
        assertNull(root.getTree(tokenNodePath));
    }

    @Test
    public void testResetTokenExpirationInvalidToken() throws Exception {
        assertFalse(tokenProvider.resetTokenExpiration(new InvalidTokenInfo(), new Date().getTime()));
    }

    @Test
    public void testResetTokenExpiration() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        assertFalse(tokenProvider.resetTokenExpiration(info, new Date().getTime()));

        long loginTime = new Date().getTime() + 3600000;
        assertFalse(info.isExpired(loginTime));
        assertTrue(tokenProvider.resetTokenExpiration(info, loginTime));

        // TODO: check again if expired tokens should be resettable
        long expiredTime = new Date().getTime() + 7200000;
        assertTrue(info.isExpired(expiredTime));
        assertTrue(tokenProvider.resetTokenExpiration(info, expiredTime));
    }

    //--------------------------------------------------------------------------
    private static void assertTokenInfo(TokenInfo info, String userId) {
        assertNotNull(info);
        assertNotNull(info.getToken());
        assertEquals(userId, info.getUserId());
        assertFalse(info.isExpired(new Date().getTime()));
    }

    private final class InvalidTokenInfo implements TokenInfo {
        @Nonnull
        @Override
        public String getUserId() {
            return "invalid";
        }
        @Nonnull
        @Override
        public String getToken() {
            return "invalid";
        }
        @Override
        public boolean isExpired(long loginTime) {
            return true;
        }
        @Override
        public boolean matches(TokenCredentials tokenCredentials) {
            return false;
        }
        @Nonnull
        @Override
        public Map<String, String> getPrivateAttributes() {
            return null;
        }
        @Nonnull
        @Override
        public Map<String, String> getPublicAttributes() {
            return null;
        }
    }
}