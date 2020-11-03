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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.SimpleCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authentication.token.TokenProviderImpl.PARAM_TOKEN_CLEANUP_THRESHOLD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenProviderImplTest extends AbstractTokenTest {

    private String userId;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
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

        sc.setAttribute("rep:token_key", "value");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(".token", "existing");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(".token", "");
        assertTrue(tokenProvider.doCreateToken(sc));
    }

    @Test
    public void testCreateTokenFromInvalidCredentials() {
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
    public void testCreateTokenFromCredentials() {
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
    public void testCreateTokenFromCredentialsSetsAttribute() {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        tokenProvider.createToken(sc);

        assertArrayEquals(new String[] {TOKEN_ATTRIBUTE}, sc.getAttributeNames());
    }

    @Test
    public void testCreateTokenCredentialsSupportDoesntSetAttribute() {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);

        CredentialsSupport credentialsSupport = mock(CredentialsSupport.class);
        when(credentialsSupport.getCredentialClasses()).thenReturn(SimpleCredentialsSupport.getInstance().getCredentialClasses());
        when(credentialsSupport.getUserId(sc)).thenReturn(SimpleCredentialsSupport.getInstance().getUserId(sc));
        when(credentialsSupport.setAttributes(any(Credentials.class), any(Map.class))).thenReturn(false);

        TokenProvider tp = createTokenProvider(root, getTokenConfig(), getUserConfiguration(), credentialsSupport);
        tp.createToken(sc);

        assertEquals(0, sc.getAttributeNames().length);
    }

    @Test
    public void testCreateTokenInvalidAlgorithm() {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        ConfigurationParameters options = ConfigurationParameters.of(UserConstants.PARAM_PASSWORD_HASH_ALGORITHM, "invalid");
        TokenProvider tp = createTokenProvider(root, options, getUserConfiguration(), SimpleCredentialsSupport.getInstance());
        assertNull(tp.createToken(sc));
    }

    @Test
    public void testCreateTokenFromInvalidUserId() {
        TokenInfo info = tokenProvider.createToken("unknownUserId", Collections.<String, Object>emptyMap());
        assertNull(info);
    }

    @Test
    public void testCreateTokenFromGroupId() throws Exception {
        Group gr = getUserManager(root).createGroup("groupId");
        assertNull(tokenProvider.createToken("groupId", Collections.<String, Object>emptyMap()));
    }

    @Test
    public void testCreateTokenFromUserId() {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTokenInfo(info, userId);
    }

    @Test
    public void testTokenNode() {
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
        assertNotNull(info);
        Tree tokenTree = getTokenTree(info);
        PropertyState prop = tokenTree.getProperty(TOKEN_ATTRIBUTE_KEY);
        assertNotNull(prop);
        assertEquals(Type.STRING, prop.getType());

        prop = tokenTree.getProperty(TOKEN_ATTRIBUTE_EXPIRY);
        assertNotNull(prop);
        assertEquals(Type.DATE, prop.getType());

        reserved.forEach((key, value) -> {
            PropertyState p = tokenTree.getProperty(key);
            if (p != null) {
                assertNotEquals(value, p.getValue(Type.STRING));
            }
        });

        privateAttributes.forEach((key, value) -> assertEquals(value, tokenTree.getProperty(key).getValue(Type.STRING)));

        publicAttributes.forEach((key, value) -> assertEquals(value, tokenTree.getProperty(key).getValue(Type.STRING)));
    }

    @Test
    public void testGetTokenInfoFromInvalidToken() {
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
    public void testGetTokenInfoFromDisabledUser() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        getTestUser().disable("disabled");

        assertNull(tokenProvider.getTokenInfo(info.getToken()));
    }

    @Test
    public void testGetTokenInfoFromGroup() throws Exception {
        Group gr = getUserManager(root).createGroup("gr");
        Tree groupNode = root.getTree(gr.getPath());
        Tree parent = TreeUtil.addChild(groupNode, TokenConstants.TOKENS_NODE_NAME, TokenConstants.TOKENS_NT_NAME);
        Tree tokenNode = TreeUtil.addChild(parent, "tokenName", TokenConstants.TOKEN_NT_NAME);
        String tokenUUID = UUID.randomUUID().toString();
        tokenNode.setProperty(JcrConstants.JCR_UUID, tokenUUID);
        String token = tokenUUID + "_generatedKey";
        tokenNode.setProperty(TokenConstants.TOKEN_ATTRIBUTE_KEY, token);

        assertNull(tokenProvider.getTokenInfo(token));
    }

    @Test
    public void testGetTokenInfoFromRegularNode() throws Exception {
        Tree node = TreeUtil.addChild(root.getTree("/"), "testNode", JcrConstants.NT_UNSTRUCTURED);
        Tree parent = TreeUtil.addChild(node, TokenConstants.TOKENS_NODE_NAME, TokenConstants.TOKENS_NT_NAME);
        Tree tokenNode = TreeUtil.addChild(parent, "tokenName", TokenConstants.TOKEN_NT_NAME);
        String tokenUUID = UUID.randomUUID().toString();
        tokenNode.setProperty(JcrConstants.JCR_UUID, tokenUUID);
        String token = tokenUUID + "_generatedKey";
        tokenNode.setProperty(TokenConstants.TOKEN_ATTRIBUTE_KEY, token);

        assertNull(tokenProvider.getTokenInfo(token));
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree node = TreeUtil.addChild(root.getTree("/"), "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            replaceTokenTree(info, node, TOKEN_NT_NAME);
            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            node.remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation2() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = getUserTree(userId);
        Tree node = TreeUtil.addChild(userTree, "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            replaceTokenTree(info, node, TOKEN_NT_NAME);

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation3() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = getUserTree(userId);
        try {
            replaceTokenTree(info, userTree.getChild(TOKENS_NODE_NAME), JcrConstants.NT_UNSTRUCTURED);

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation4() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        TokenInfo info2 = null;
        try {
            String uid = adminSession.getAuthInfo().getUserID();
            Tree adminTree = getUserTree(uid);
            Tree node = TreeUtil.getOrAddChild(adminTree, TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
            assertTrue(root.move(tokenTree.getPath(), node.getPath() + '/' + tokenTree.getName()));

            info2 = tokenProvider.getTokenInfo(info.getToken());
            assertNotNull(info2);
            assertFalse(info2.matches(new TokenCredentials(info.getToken())));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetTokenInfo() {
        String token = createTokenInfo(tokenProvider, userId).getToken();
        TokenInfo info = tokenProvider.getTokenInfo(token);
        assertTokenInfo(info, userId);
    }

    @Test
    public void testCreateTokenWithExpirationParam() {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        sc.setAttribute(TokenProvider.PARAM_TOKEN_EXPIRATION, 100000);

        TokenInfo info = tokenProvider.createToken(sc);
        assertTokenInfo(info, userId);

        Tree tokenTree = getTokenTree(info);
        assertNotNull(tokenTree);
        assertTrue(tokenTree.exists());
        assertTrue(tokenTree.hasProperty(TokenProvider.PARAM_TOKEN_EXPIRATION));
        assertEquals(100000, tokenTree.getProperty(TokenProvider.PARAM_TOKEN_EXPIRATION).getValue(Type.LONG).longValue());
    }

    @Test
    public void testCreateTokenWithInvalidExpirationParam() {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        sc.setAttribute(TokenProvider.PARAM_TOKEN_EXPIRATION, "invalid");

        try {
            tokenProvider.createToken(sc);
            fail();
        } catch (NumberFormatException e) {
            // success
        }
    }

    @Test
    public void testFailingCleanupExpired() throws Exception {
        User u = getTestUser();
        // grant user principal access to read/create tokens but not removing them
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, u.getPath());
        acl.addAccessControlEntry(u.getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_MODIFY_PROPERTIES));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        try (ContentSession cs = login(new SimpleCredentials(u.getID(), u.getID().toCharArray()))) {
            Root testRoot = cs.getLatestRoot();
            ConfigurationParameters options = ConfigurationParameters.of(PARAM_TOKEN_CLEANUP_THRESHOLD, 1);
            TokenProvider tp = createTokenProvider(testRoot, options, getUserConfiguration(), SimpleCredentialsSupport.getInstance());

            SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
            sc.setAttribute(TokenProvider.PARAM_TOKEN_EXPIRATION, 1);

            TokenInfo info = tp.createToken(sc);
            waitUntilExpired(info);

            // create new infos until the cleanup is triggered or until max cnt has been reached.
            // in either case the expired info must still exist
            TokenInfo ti;
            int cnt = 0;
            do {
                ti = tp.createToken(sc);
                cnt++;
            } while (ti.getToken().charAt(0) >= '2' && cnt < 50);

            root.refresh();
            assertTrue(getTokenTree(info).exists());
        }
    }

    @Test
    public void testCleanupThresholdNotReached() {
        ConfigurationParameters options = ConfigurationParameters.of(PARAM_TOKEN_CLEANUP_THRESHOLD, 100);
        TokenProvider tp = createTokenProvider(root, options, getUserConfiguration(), SimpleCredentialsSupport.getInstance());

        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        sc.setAttribute(TokenProvider.PARAM_TOKEN_EXPIRATION, 1);

        TokenInfo info = tp.createToken(sc);
        waitUntilExpired(info);

        // create new infos until the cleanup is triggered or until max cnt has been reached.
        // -> since PARAM_TOKEN_CLEANUP_THRESHOLD is set to 100, the expired token is still not removed
        TokenInfo ti;
        int cnt = 0;
        do {
            ti = tp.createToken(sc);
            cnt++;
        } while (ti.getToken().charAt(0) >= '2' && cnt < 50);

        assertTrue(getTokenTree(info).exists());
    }

    /**
     *@see <a href="https://issues.apache.org/jira/browse/OAK-1697">OAK-1697</a>
     */
    @Test
    public void testValidTokenCredentialsWithConflict() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        List<ContentSession> sessions = new ArrayList<ContentSession>();

        try {
            TokenConfiguration tc = getSecurityProvider().getConfiguration(
                    TokenConfiguration.class);
            SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();

            List<TokenProvider> tokenProviders = new ArrayList<TokenProvider>();

            for (int i = 0; i < 10; i++) {
                ContentSession session = login(getAdminCredentials());
                Root r = session.getLatestRoot();
                tokenProviders.add(tc.getTokenProvider(r));
                sessions.add(session);
            }

            ArrayList<DataFuture> list = new ArrayList<DataFuture>();

            for (TokenProvider tokenProvider : tokenProviders) {
                list.add(createDataFuture(pool, tokenProvider, sc.getUserID(),
                        Collections.<String, Object> emptyMap()));
            }

            for (DataFuture df : list) {
                assertNotNull(df.future.get());
            }
        } finally {
            for (ContentSession session : sessions) {
                if (session != null) {
                    session.close();
                }
            }

            if (pool != null) {
                pool.shutdown();
            }
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1985">OAK-1985</a>
     */
    @Test
    public void testTokenValidationIsCaseInsensitive() {
        Root root = adminSession.getLatestRoot();
        TokenConfiguration tokenConfig = getSecurityProvider().getConfiguration(TokenConfiguration.class);
        TokenProvider tp = tokenConfig.getTokenProvider(root);

        String userId = ((SimpleCredentials) getAdminCredentials()).getUserID();
        TokenInfo info = tp.createToken(userId.toUpperCase(), Collections.<String, Object>emptyMap());

        assertTrue(info.matches(new TokenCredentials(info.getToken())));
        assertEquals(userId, info.getUserId());

        info = tp.getTokenInfo(info.getToken());

        assertTrue(info.matches(new TokenCredentials(info.getToken())));
        assertEquals(userId, info.getUserId());
    }

    //--------------------------------------------------------------------------
    private static void assertTokenInfo(TokenInfo info, String userId) {
        assertNotNull(info);
        assertNotNull(info.getToken());
        assertEquals(userId, info.getUserId());
        assertFalse(info.isExpired(new Date().getTime()));
    }
    
    private static class DataFuture {
        public Future<TokenInfo> future;

        DataFuture(Future<TokenInfo> future) {
            super();
            this.future = future;
        }
    }

    @NotNull
    private DataFuture createDataFuture(ExecutorService pool , final TokenProvider tp,final String userId, final Map<String, ?> attributes){
        Future<TokenInfo> future = pool.submit(() -> tp.createToken(userId, attributes));
        return new DataFuture(future);
    }
}