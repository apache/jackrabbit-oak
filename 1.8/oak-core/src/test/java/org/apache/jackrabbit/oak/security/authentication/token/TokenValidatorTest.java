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
import java.util.UUID;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TokenValidatorTest extends AbstractTokenTest {

    private String userId;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    @Test
    public void testCreateReservedKeyProperty() throws Exception {
        NodeUtil node = new NodeUtil(root.getTree("/")).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            node.setString(TOKEN_ATTRIBUTE_KEY, "anyValue");
            root.commit(CommitMarker.asCommitAttributes());
            fail("The reserved token key property must not used with other node types.");
        } catch (CommitFailedException e) {
            assertEquals(60, e.getCode());
        } finally {
            node.getTree().remove();
            root.commit();
        }
    }

    @Test
    public void testCreateReservedKeyProperty2() throws Exception {
        NodeUtil node = new NodeUtil(root.getTree("/")).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            node.setString(TOKEN_ATTRIBUTE_KEY, "anyValue");
            root.commit();
            fail("The reserved token key property must only be created by the TokenProvider.");
        } catch (CommitFailedException e) {
            assertEquals(63, e.getCode());
        } finally {
            node.getTree().remove();
            root.commit();
        }
    }

    @Test
    public void testChangingTokenKey() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        NodeUtil tokenTree = new NodeUtil(getTokenTree(info));

        try {
            tokenTree.setString(TOKEN_ATTRIBUTE_KEY, PasswordUtil.buildPasswordHash("anotherValue"));
            root.commit(CommitMarker.asCommitAttributes());
            fail("The token key must never be modified.");
        } catch (CommitFailedException e) {
            assertEquals(61, e.getCode());
        }
    }

    @Test
    public void testPlaintextTokenKey() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        NodeUtil tokenTree = new NodeUtil(getTokenTree(info));

        try {
            tokenTree.setString(TOKEN_ATTRIBUTE_KEY, "anotherValue");
            root.commit(CommitMarker.asCommitAttributes());
            fail("The token key must not be plaintext.");
        } catch (CommitFailedException e) {
            assertEquals(66, e.getCode());
        }
    }

    @Test
    public void testManuallyModifyExpirationDate() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        NodeUtil tokenTree = new NodeUtil(getTokenTree(info));

        try {
            tokenTree.setDate(TOKEN_ATTRIBUTE_EXPIRY, new Date().getTime());
            root.commit();
            fail("The token expiry must not manually be changed");
        } catch (CommitFailedException e) {
            assertEquals(63, e.getCode());
        }
    }

    @Test
    public void testModifyExpirationDate() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        NodeUtil tokenTree = new NodeUtil(getTokenTree(info));

        tokenTree.setDate(TOKEN_ATTRIBUTE_EXPIRY, new Date().getTime());
        root.commit(CommitMarker.asCommitAttributes());
    }

    @Test
    public void testCreateTokenAtInvalidLocationBelowTestNode() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        NodeUtil node = new NodeUtil(root.getTree("/")).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            createTokenTree(info, node, TOKEN_NT_NAME);
            tokenTree.remove();
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token not  at '/testNode' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(64, e.getCode());
        } finally {
            node.getTree().remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testCreateTokenAtInvalidLocationInsideUser() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            createTokenTree(info, node, TOKEN_NT_NAME);
            tokenTree.remove();
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token '" + node.getTree().getPath() + "' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(65, e.getCode());
        } finally {
            node.getTree().remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testCreateTokenAtInvalidLocationInsideUser2() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree).getOrAddChild(TOKENS_NODE_NAME, TOKENS_NT_NAME);
        try {
            node = node.addChild("invalid", JcrConstants.NT_UNSTRUCTURED);
            createTokenTree(info, node, TOKEN_NT_NAME);
            tokenTree.remove();
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token '" + node.getTree().getPath() + "' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(65, e.getCode());
        } finally {
            node.getTree().remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testManuallyCreateToken() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        NodeUtil tokensNode = new NodeUtil(tokenTree.getParent());
        try {
            // create a valid token node using the test root
            createTokenTree(info, tokensNode, TOKEN_NT_NAME);
            tokenTree.remove();
            root.commit();

            fail("Manually creating a token node must fail.");
        } catch (CommitFailedException e) {
            assertEquals(63, e.getCode());
        } finally {
            root.refresh();
            root.commit();
        }
    }

    @Test
    public void testCreateTokenWithInvalidNodeType() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        Tree tokenTree = getTokenTree(info);

        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree.getChild(TOKENS_NODE_NAME));
        Tree t = null;
        try {
            t = createTokenTree(info, node, JcrConstants.NT_UNSTRUCTURED);
            tokenTree.remove();
            root.commit(CommitMarker.asCommitAttributes());

            fail("The token node must be of type rep:Token.");
        } catch (CommitFailedException e) {
            assertEquals(60, e.getCode());
        } finally {
            if (t != null) {
                t.remove();
                root.commit(CommitMarker.asCommitAttributes());
            }
        }
    }

    @Test
    public void testRemoveTokenNode() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        getTokenTree(info).remove();
        root.commit();
    }

    @Test
    public void testInvalidTokenParentNode() throws Exception {
        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree).addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            // Invalid node type of '.tokens' node
            node.addChild(TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token '" + node.getTree().getPath() + "' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(68, e.getCode());
        } finally {
            node.getTree().remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testManuallyCreateTokenParent() throws Exception {
        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree);

        node.addChild(TOKENS_NODE_NAME, TOKENS_NT_NAME);
        root.commit();
    }

    @Test
    public void testManuallyCreateTokenParentWithNtUnstructured() throws Exception {
        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree);

        node.addChild(TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
        root.commit();
    }

    @Test
    public void testTokensNodeBelowRoot() throws Exception {
        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil n = null;
        try {
            // Invalid node type of '.tokens' node
            n = rootNode.addChild(TOKENS_NODE_NAME, TOKENS_NT_NAME);
            root.commit();

            fail("The token parent node must be located below the configured user root.");
        } catch (CommitFailedException e) {
            assertEquals(64, e.getCode());
        } finally {
            if (n != null) {
                n.getTree().remove();
                root.commit(CommitMarker.asCommitAttributes());
            }
        }
    }

    @Test
    public void testTokensNodeAtInvalidPathBelowUser() throws Exception {
        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil userNode = new NodeUtil(userTree);
        NodeUtil n = null;
        try {
            // Invalid node type of '.tokens' node
            n = userNode.addChild("test", JcrConstants.NT_UNSTRUCTURED);
            n.addChild(TOKENS_NODE_NAME, TOKENS_NT_NAME);
            root.commit();

            fail("The token parent node must be located below the user home node.");
        } catch (CommitFailedException e) {
            assertEquals(68, e.getCode());
        } finally {
            if (n != null) {
                n.getTree().remove();
                root.commit(CommitMarker.asCommitAttributes());
            }
        }
    }

    @Test
    public void testChangeTokenParentPrimaryTypeToRepUnstructured() throws Exception {
        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil node = new NodeUtil(userTree);

        node = node.addChild(TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        node.setName(JcrConstants.JCR_PRIMARYTYPE, TOKENS_NT_NAME);
        root.commit();
    }

    @Test
    public void testChangeTokenParentPrimaryType() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());

        try {
            Tree tokensTree = getTokenTree(info).getParent();
            tokensTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);

            root.commit();
            fail("The primary type of the token parent must not be changed from rep:Unstructured to another type.");
        } catch (CommitFailedException e) {
            assertEquals(69, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testChangeRegularRepUnstructuredPrimaryType() throws Exception {
        Tree userTree = root.getTree(getUserManager(root).getAuthorizable(userId).getPath());
        NodeUtil n = new NodeUtil(userTree).getOrAddChild("test", NodeTypeConstants.NT_REP_UNSTRUCTURED);
        root.commit();

        n.setName(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED);
        root.commit();
    }

    @Test
    public void testChangeToReservedTokenNodeType() throws Exception {
        String parentPath = getTestUser().getPath() + "/"+TokenConstants.TOKENS_NODE_NAME;
        String path = parentPath+"/node";
        try {
            Tree t = root.getTree(getTestUser().getPath()).addChild(TokenConstants.TOKENS_NODE_NAME);
            t.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
            t.addChild("node").setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
            root.commit();

            NodeUtil node = new NodeUtil(root.getTree(path));
            node.setName(JcrConstants.JCR_PRIMARYTYPE, TokenConstants.TOKEN_NT_NAME);
            node.setString(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
            node.setString(TokenConstants.TOKEN_ATTRIBUTE_KEY, PasswordUtil.buildPasswordHash("key"));
            node.setDate(TokenConstants.TOKEN_ATTRIBUTE_EXPIRY, new Date().getTime());
            root.commit(CommitMarker.asCommitAttributes());
        } catch (CommitFailedException e) {
            assertEquals(62, e.getCode());
        } finally {
            root.refresh();
            root.getTree(parentPath).remove();
            root.commit();
        }
    }
}