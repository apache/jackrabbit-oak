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
import javax.jcr.AccessDeniedException;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
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

/**
 * TokenProviderImplTest...
 */
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
    public void testCreateTokenFromUserId() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTokenInfo(info, userId);
    }

    @Test
    public void testTokenNode() throws Exception {
        Map<String, String> reserved = new HashMap<String, String>();
        reserved.put(".token", "value");
        reserved.put("rep:token.key", "value");
        reserved.put("rep:token.exp", "value");

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
        PropertyState prop = tokenTree.getProperty("rep:token.key");
        assertNotNull(prop);
        assertEquals(Type.STRING, prop.getType());

        prop = tokenTree.getProperty("rep:token.exp");
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
    public void testGetTokenInfoFromInvalidLocation() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        NodeUtil node = new NodeUtil(root.getTree("/")).addChild("testNode", "nt:unstructured");
        try {
            createTokenTree(info, node, "rep:Token");
            tokenTree.remove();
            root.commit();

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            node.getTree().remove();
            root.commit();
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation2() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree).addChild("testNode", "nt:unstructured");
        try {
            createTokenTree(info, node, "rep:Token");
            tokenTree.remove();
            root.commit();

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            node.getTree().remove();
            root.commit();
        }
    }

    @Test
    public void testGetTokenInfoFromInvalidLocation3() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree.getChild(".tokens"));
        try {
            createTokenTree(info, node, "nt:unstructured");
            tokenTree.remove();
            root.commit();

            assertNull(tokenProvider.getTokenInfo(info.getToken()));
        } finally {
            node.getTree().remove();
            root.commit();
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
            NodeUtil node = new NodeUtil(adminTree).getOrAddChild(".tokens", "nt:unstructured");
            assertTrue(root.move(tokenTree.getPath(), node.getTree().getPath() + "/" + tokenTree.getName()));
            root.commit();

            info2 = tokenProvider.getTokenInfo(info.getToken());
            assertNotNull(info2);
            assertFalse(info2.matches(new TokenCredentials(info.getToken())));
        } finally {
            Tree t = getTokenTree(info2);
            t.remove();
            root.commit();
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

    //--------------------------------------------------------------------------
    private static void assertTokenInfo(TokenInfo info, String userId) {
        assertNotNull(info);
        assertNotNull(info.getToken());
        assertEquals(userId, info.getUserId());
        assertFalse(info.isExpired(new Date().getTime()));
    }

    private Tree getTokenTree(TokenInfo info) {
        String token = info.getToken();
        int pos = token.indexOf('_');
        String nodeId = (pos == -1) ? token : token.substring(0, pos);
        return new IdentifierManager(root).getTree(nodeId);
    }

    private void createTokenTree(TokenInfo base, NodeUtil parent, String ntName) throws AccessDeniedException {
        Tree tokenTree = getTokenTree(base);
        Tree tree = parent.addChild("token", ntName).getTree();
        tree.setProperty(tokenTree.getProperty(JcrConstants.JCR_UUID));
        tree.setProperty(tokenTree.getProperty("rep:token.key"));
        tree.setProperty(tokenTree.getProperty("rep:token.exp"));
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