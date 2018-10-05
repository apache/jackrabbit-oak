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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void testCreateTokenFromGroupId() throws Exception {
        Group gr = getUserManager(root).createGroup("groupId");
        assertNull(tokenProvider.createToken("groupId", Collections.<String, Object>emptyMap()));
    }

    @Test
    public void testCreateTokenFromUserId() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTokenInfo(info, userId);
    }

    @Test
    public void testTokenNode() throws Exception {
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
        Tree tokenTree = getTokenTree(info);
        PropertyState prop = tokenTree.getProperty(TOKEN_ATTRIBUTE_KEY);
        assertNotNull(prop);
        assertEquals(Type.STRING, prop.getType());

        prop = tokenTree.getProperty(TOKEN_ATTRIBUTE_EXPIRY);
        assertNotNull(prop);
        assertEquals(Type.DATE, prop.getType());

        for (String key : reserved.keySet()) {
            PropertyState p = tokenTree.getProperty(key);
            if (p != null) {
                assertFalse(reserved.get(key).equals(p.getValue(Type.STRING)));
            }
        }

        for (String key : privateAttributes.keySet()) {
            assertEquals(privateAttributes.get(key), tokenTree.getProperty(key).getValue(Type.STRING));
        }

        for (String key : publicAttributes.keySet()) {
            assertEquals(publicAttributes.get(key), tokenTree.getProperty(key).getValue(Type.STRING));
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
    public void testGetTokenInfoFromDisabledUser() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        getTestUser().disable("disabled");

        assertNull(tokenProvider.getTokenInfo(info.getToken()));
    }

    @Test
    public void testGetTokenInfoFromGroup() throws Exception {
        Group gr = getUserManager(root).createGroup("gr");
        NodeUtil groupNode = new NodeUtil(root.getTree(gr.getPath()));
        NodeUtil parent = groupNode.addChild(TokenConstants.TOKENS_NODE_NAME, TokenConstants.TOKENS_NT_NAME);
        NodeUtil tokenNode = parent.addChild("tokenName", TokenConstants.TOKEN_NT_NAME);
        String tokenUUID = UUID.randomUUID().toString();
        tokenNode.setString(JcrConstants.JCR_UUID, tokenUUID);
        String token = tokenUUID + "_generatedKey";
        tokenNode.setString(TokenConstants.TOKEN_ATTRIBUTE_KEY, token);

        assertNull(tokenProvider.getTokenInfo(token));
    }

    @Test
    public void testGetTokenInfoFromRegularNode() throws Exception {
        NodeUtil node = new NodeUtil(root.getTree("/")).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        NodeUtil parent = node.addChild(TokenConstants.TOKENS_NODE_NAME, TokenConstants.TOKENS_NT_NAME);
        NodeUtil tokenNode = parent.addChild("tokenName", TokenConstants.TOKEN_NT_NAME);
        String tokenUUID = UUID.randomUUID().toString();
        tokenNode.setString(JcrConstants.JCR_UUID, tokenUUID);
        String token = tokenUUID + "_generatedKey";
        tokenNode.setString(TokenConstants.TOKEN_ATTRIBUTE_KEY, token);

        assertNull(tokenProvider.getTokenInfo(token));
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        NodeUtil node = new NodeUtil(root.getTree("/")).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            createTokenTree(info, node, TOKEN_NT_NAME);
            tokenTree.remove();

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            node.getTree().remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation2() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            createTokenTree(info, node, TOKEN_NT_NAME);
            tokenTree.remove();

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation3() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree.getChild(TOKENS_NODE_NAME));
        try {
            createTokenTree(info, node, JcrConstants.NT_UNSTRUCTURED);
            tokenTree.remove();

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation4() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        TokenInfo info2 = null;
        try {
            Tree adminTree = root.getTree(getUserManager(root).getAuthorizable(adminSession.getAuthInfo().getUserID()).getPath());
            NodeUtil node = new NodeUtil(adminTree).getOrAddChild(TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
            assertTrue(root.move(tokenTree.getPath(), node.getTree().getPath() + '/' + tokenTree.getName()));

            info2 = tokenProvider.getTokenInfo(info.getToken());
            assertNotNull(info2);
            assertFalse(info2.matches(new TokenCredentials(info.getToken())));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetTokenInfo() throws Exception {
        String token = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap()).getToken();
        TokenInfo info = tokenProvider.getTokenInfo(token);
        assertTokenInfo(info, userId);
    }

    @Test
    public void testCreateTokenWithExpirationParam() throws Exception {
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
    public void testCreateTokenWithInvalidExpirationParam() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        sc.setAttribute(TokenProvider.PARAM_TOKEN_EXPIRATION, "invalid");

        try {
            tokenProvider.createToken(sc);
            fail();
        } catch (NumberFormatException e) {
            // success
        }
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
    public void testTokenValidationIsCaseInsensitive() throws Exception {
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

        public DataFuture(Future<TokenInfo> future) {
            super();
            this.future = future;
        }
    }
    
    private DataFuture createDataFuture(ExecutorService pool , final TokenProvider tp,final String userId, final Map<String, ?> attributes){
        Future<TokenInfo> future = pool.submit(new Callable<TokenInfo>() {
            @Override
            public TokenInfo call() throws Exception {
                return tp.createToken(userId, attributes);
            }
        });
        return new DataFuture(future);
    }
}