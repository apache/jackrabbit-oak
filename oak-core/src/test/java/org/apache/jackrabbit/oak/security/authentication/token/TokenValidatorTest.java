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

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenValidatorTest extends AbstractTokenTest {

    private String userId;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    private static String getDateValue() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(new Date().getTime());
        return ISO8601.format(calendar);
    }

    @Test
    public void testCreateReservedKeyProperty() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree("/"), "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            tree.setProperty(TOKEN_ATTRIBUTE_KEY, "anyValue");
            root.commit(CommitMarker.asCommitAttributes());
            fail("The reserved token key property must not used with other node types.");
        } catch (CommitFailedException e) {
            assertEquals(60, e.getCode());
        } finally {
            tree.remove();
            if (root.hasPendingChanges()) {
                root.commit();
            }
        }
    }

    @Test
    public void testCreateReservedKeyProperty2() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree("/"), "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            tree.setProperty(TOKEN_ATTRIBUTE_KEY, "anyValue");
            root.commit();
            fail("The reserved token key property must only be created by the TokenProvider.");
        } catch (CommitFailedException e) {
            assertEquals(63, e.getCode());
        } finally {
            tree.remove();
            if (root.hasPendingChanges()) {
                root.commit();
            }
        }
    }

    @Test
    public void testChangingTokenKey() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        Tree tokenTree = getTokenTree(info);

        try {
            tokenTree.setProperty(TOKEN_ATTRIBUTE_KEY, PasswordUtil.buildPasswordHash("anotherValue"));
            root.commit(CommitMarker.asCommitAttributes());
            fail("The token key must never be modified.");
        } catch (CommitFailedException e) {
            assertEquals(61, e.getCode());
        }
    }

    @Test
    public void testPlaintextTokenKey() {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        Tree tokenTree = getTokenTree(info);

        try {
            tokenTree.setProperty(TOKEN_ATTRIBUTE_KEY, "anotherValue");
            root.commit(CommitMarker.asCommitAttributes());
            fail("The token key must not be plaintext.");
        } catch (CommitFailedException e) {
            assertEquals(66, e.getCode());
        }
    }

    @Test
    public void testManuallyModifyExpirationDate() {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        Tree tokenTree = getTokenTree(info);

        try {
            tokenTree.setProperty(TOKEN_ATTRIBUTE_EXPIRY, getDateValue(), DATE);
            root.commit();
            fail("The token expiry must not manually be changed");
        } catch (CommitFailedException e) {
            assertEquals(63, e.getCode());
        }
    }

    @Test
    public void testModifyExpirationDate() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        Tree tokenTree = getTokenTree(info);

        tokenTree.setProperty(TOKEN_ATTRIBUTE_EXPIRY, getDateValue(), DATE);
        root.commit(CommitMarker.asCommitAttributes());
    }

    @Test
    public void testCreateTokenAtInvalidLocationBelowTestNode() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree tree = TreeUtil.addChild(root.getTree("/"), "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            replaceTokenTree(info, tree, TOKEN_NT_NAME);
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token not  at '/testNode' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(64, e.getCode());
        } finally {
            tree.remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testCreateTokenAtInvalidLocationInsideUser() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = getUserTree(userId);
        Tree tree = TreeUtil.addChild(userTree, "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            replaceTokenTree(info, tree, TOKEN_NT_NAME);
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token '" + tree.getPath() + "' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(65, e.getCode());
        } finally {
            tree.remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testCreateTokenAtInvalidLocationInsideUser2() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = getUserTree(userId);
        Tree tree = TreeUtil.addChild(userTree, TOKENS_NODE_NAME, TOKENS_NT_NAME);
        try {
            tree = TreeUtil.addChild(tree, "invalid", JcrConstants.NT_UNSTRUCTURED);
            replaceTokenTree(info, tree, TOKEN_NT_NAME);
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token '" + tree.getPath() + "' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(65, e.getCode());
        } finally {
            tree.remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testManuallyCreateToken() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = getUserTree(userId);
        try {
            // create a valid token node using the test root
            replaceTokenTree(info, userTree.getChild(TOKENS_NODE_NAME), TOKEN_NT_NAME);
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
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertNotNull(tokenProvider.getTokenInfo(info.getToken()));

        Tree userTree = getUserTree(userId);
        Tree t = null;
        try {
            t = replaceTokenTree(info, userTree.getChild(TOKENS_NODE_NAME), JcrConstants.NT_UNSTRUCTURED);
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
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        getTokenTree(info).remove();
        root.commit();
    }

    @Test
    public void testInvalidTokenParentNode() throws Exception {
        Tree userTree = getUserTree(userId);
        Tree node = TreeUtil.addChild(userTree, "testNode", JcrConstants.NT_UNSTRUCTURED);
        try {
            // Invalid node type of '.tokens' node
            TreeUtil.addChild(node, TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
            root.commit(CommitMarker.asCommitAttributes());

            fail("Creating a new token '" + node.getPath() + "' must fail.");
        } catch (CommitFailedException e) {
            assertEquals(68, e.getCode());
        } finally {
            node.remove();
            root.commit(CommitMarker.asCommitAttributes());
        }
    }

    @Test
    public void testManuallyCreateTokenParent() throws Exception {
        Tree userTree = getUserTree(userId);
        TreeUtil.addChild(userTree, TOKENS_NODE_NAME, TOKENS_NT_NAME);
        root.commit();
    }

    @Test
    public void testManuallyCreateTokenParentWithNtUnstructured() throws Exception {
        Tree userTree = getUserTree(userId);

        TreeUtil.addChild(userTree, TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
        root.commit();
    }

    @Test
    public void testTokensNodeBelowRoot() throws Exception {
        Tree rootNode = root.getTree("/");
        Tree n = null;
        try {
            // Invalid node type of '.tokens' node
            n = TreeUtil.addChild(rootNode, TOKENS_NODE_NAME, TOKENS_NT_NAME);
            root.commit();

            fail("The token parent node must be located below the configured user root.");
        } catch (CommitFailedException e) {
            assertEquals(64, e.getCode());
        } finally {
            if (n != null) {
                n.remove();
                root.commit(CommitMarker.asCommitAttributes());
            }
        }
    }

    @Test
    public void testTokensNodeAtInvalidPathBelowUser() throws Exception {
        Tree userTree = getUserTree(userId);
        Tree n = null;
        try {
            // Invalid node type of '.tokens' node
            n = TreeUtil.addChild(userTree, "test", JcrConstants.NT_UNSTRUCTURED);
            TreeUtil.addChild(n, TOKENS_NODE_NAME, TOKENS_NT_NAME);
            root.commit();

            fail("The token parent node must be located below the user home node.");
        } catch (CommitFailedException e) {
            assertEquals(68, e.getCode());
        } finally {
            if (n != null) {
                n.remove();
                root.commit(CommitMarker.asCommitAttributes());
            }
        }
    }

    @Test
    public void testChangeTokenParentPrimaryTypeToRepUnstructured() throws Exception {
        Tree userTree = getUserTree(userId);

        Tree node = TreeUtil.addChild(userTree, TOKENS_NODE_NAME, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        node.setProperty(JcrConstants.JCR_PRIMARYTYPE, TOKENS_NT_NAME, Type.NAME);
        root.commit();
    }

    @Test
    public void testChangeTokenParentPrimaryType() {
        TokenInfo info = createTokenInfo(tokenProvider, userId);

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
        Tree userTree = getUserTree(userId);
        Tree n = TreeUtil.getOrAddChild(userTree,"test", NodeTypeConstants.NT_REP_UNSTRUCTURED);
        root.commit();

        n.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
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

            Tree node = root.getTree(path);
            node.setProperty(JcrConstants.JCR_PRIMARYTYPE, TokenConstants.TOKEN_NT_NAME, Type.NAME);
            node.setProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
            node.setProperty(TokenConstants.TOKEN_ATTRIBUTE_KEY, PasswordUtil.buildPasswordHash("key"));
            node.setProperty(TokenConstants.TOKEN_ATTRIBUTE_EXPIRY, getDateValue(), Type.DATE);
            root.commit(CommitMarker.asCommitAttributes());
        } catch (CommitFailedException e) {
            assertEquals(62, e.getCode());
        } finally {
            root.refresh();
            root.getTree(parentPath).remove();
            root.commit();
        }
    }

    @Test
    public void testReservedPropertyAddedValidParent() throws Exception {
        Tree tokenTree = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "name", TOKEN_NT_NAME);
        Validator v = createRootValidator(tokenTree, tokenTree);
        v.propertyAdded(PropertyStates.createProperty(TokenConstants.TOKEN_ATTRIBUTE_EXPIRY, "anyValue"));
    }

    @Test(expected = CommitFailedException.class)
    public void testReservedPropertyAddedInvalidParent() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        try {
            Validator v = createRootValidator(rootTree, rootTree);
            v.propertyAdded(PropertyStates.createProperty(TokenConstants.TOKEN_ATTRIBUTE_EXPIRY, "anyValue"));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(60, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddTokenTreeMissingKey() throws Exception {
        Tree tokenTree = getTokenTree(createTokenInfo(tokenProvider, userId));
        tokenTree.removeProperty(TokenConstants.TOKEN_ATTRIBUTE_KEY);

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        try {
            Validator v = createValidator(rootTree, rootTree, tokenTree.getParent().getPath(), false);
            v.childNodeAdded(tokenTree.getName(), getTreeProvider().asNodeState(tokenTree));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(66, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddTokenTreeMissingTokensParent() throws Exception {
        Tree tokenTree = getTokenTree(createTokenInfo(tokenProvider, userId));
        root.move(tokenTree.getPath(), PathUtils.concat(getTestUser().getPath(), tokenTree.getName()));
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);

        try {
            Validator v = createValidator(rootTree, rootTree, getTestUser().getPath(), true);
            v.childNodeAdded(tokenTree.getName(), mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(65, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddTokenTreeMissingUserGrandParent() throws Exception {
        // since adding/changing an invalid tokens-parent node will be detected, mocking is required to
        // reach the desired invalid state, where the .tokens node isn't located below the user home.
        Tree tokenTree = getTokenTree(createTokenInfo(tokenProvider, userId));
        Tree tokensTree = tokenTree.getParent();
        // move .tokens node one level up
        String destPath = PathUtils.concat(PathUtils.getParentPath(getTestUser().getPath()), tokensTree.getName());
        root.move(tokensTree.getPath(), destPath);
        try {
            // create a validator that has 'tokensTree' as parentBefore and parentAfter
            NodeState ns = getTreeProvider().asNodeState(tokensTree);
            TreeProvider tp = when(mock(TreeProvider.class).createReadOnlyTree(ns)).thenReturn(tokensTree).getMock();
            TokenValidatorProvider tvp = new TokenValidatorProvider(ConfigurationParameters.EMPTY, tp);
            Validator v = tvp.getRootValidator(ns, ns, new CommitInfo("sid", "uid", CommitMarker.asCommitAttributes()));
            assertNotNull(v);
            v.childNodeChanged(tokenTree.getName(), mock(NodeState.class), mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(65, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddTokenTreeInvalidKey() throws Exception {
        Tree tokenTree = getTokenTree(createTokenInfo(tokenProvider, userId));
        tokenTree.setProperty(TokenConstants.TOKEN_ATTRIBUTE_KEY, "someValue");

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        try {
            Validator v = createValidator(rootTree, rootTree, tokenTree.getParent().getPath(), true);
            v.childNodeAdded(tokenTree.getName(), getTreeProvider().asNodeState(tokenTree));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(66, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddTokenTreeMissingExpiry() throws Exception {
        Tree tokenTree = getTokenTree(createTokenInfo(tokenProvider, userId));
        tokenTree.removeProperty(TokenConstants.TOKEN_ATTRIBUTE_EXPIRY);

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Validator v = createValidator(rootTree, rootTree, tokenTree.getParent().getPath(), false);
        try {
            v.childNodeAdded(tokenTree.getName(), getTreeProvider().asNodeState(tokenTree));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(67, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalValidatorSequence() throws Exception {
        Tree tokenTree = getTokenTree(createTokenInfo(tokenProvider, userId));
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);

        // illegal sequence of adding nodes and the changing -> must be spotted by the validator
        Validator v = createValidator(rootTree, rootTree, tokenTree.getParent().getPath(), true);
        v.childNodeChanged(tokenTree.getName(), mock(NodeState.class), mock(NodeState.class));
    }

    @NotNull
    private Validator createRootValidator(@NotNull Tree before, @NotNull Tree after) {
        TokenValidatorProvider tvp = new TokenValidatorProvider(ConfigurationParameters.EMPTY, getTreeProvider());
        Validator v = tvp.getRootValidator(getTreeProvider().asNodeState(before), getTreeProvider().asNodeState(after), new CommitInfo("sid", "uid", CommitMarker.asCommitAttributes()));
        assertNotNull(v);
        return v;
    }

    @NotNull
    private Validator createValidator(@NotNull Tree before, @NotNull Tree after, @NotNull String path, boolean isAdd) throws CommitFailedException {
        TokenValidatorProvider tvp = new TokenValidatorProvider(ConfigurationParameters.EMPTY, getTreeProvider());
        NodeState b = getTreeProvider().asNodeState(before);
        NodeState a = getTreeProvider().asNodeState(after);
        Validator v = tvp.getRootValidator(b, a, new CommitInfo("sid", "uid", CommitMarker.asCommitAttributes()));
        for (String name : PathUtils.elements(path)) {
            assertNotNull(v);
            b = b.getChildNode(name);
            a = a.getChildNode(name);
            v = (isAdd) ? v.childNodeAdded(name, a) : v.childNodeChanged(name, b, a);
        }
        assertNotNull(v);
        return v;
    }
}