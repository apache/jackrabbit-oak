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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import java.util.UUID;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TokenProviderImplExceptionTest extends AbstractTokenTest  {

    private UserConfiguration uc;
    private TokenProviderImpl tp;
    private UserManager userManager;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        userManager = mock(UserManager.class);
        uc = mock(UserConfiguration.class);
        when(uc.getUserManager(any(Root.class), any(NamePathMapper.class))).thenReturn(userManager);

        tp = createTokenProvider(root, uc);
    }

    @Test
    public void testCreateToken() throws Exception {
        when(userManager.getAuthorizable(anyString())).thenThrow(new RepositoryException());

        assertNull(tp.createToken(new SimpleCredentials("uid", new char[0])));
        verify(userManager, times(1)).getAuthorizable("uid");
    }

    @Test
    public void testCreateTokenAccessDenied() throws Exception {
        User u = mock(User.class);
        when(u.getPath()).thenReturn("/testuser");
        when(userManager.getAuthorizable(anyString())).thenReturn(u);

        Tree tokenTree = when(mock(Tree.class).exists()).thenReturn(false).getMock();
        when(tokenTree.getProperty(JCR_UUID)).thenReturn(PropertyStates.createProperty(JCR_UUID, UUID.randomUUID().toString()));

        Tree tokenParent = mock(Tree.class);
        when(tokenParent.exists()).thenReturn(true);
        when(tokenParent.addChild(anyString())).thenReturn(tokenTree);

        String parentPath = "/testuser/" + TOKENS_NODE_NAME;
        Tree userTree = mock(Tree.class);
        when(userTree.exists()).thenReturn(true);
        when(userTree.getChild(TOKENS_NODE_NAME)).thenReturn(tokenParent);

        Root r = mock(Root.class);
        when(r.getTree(parentPath)).thenReturn(tokenParent);
        when(r.getTree("/testuser")).thenReturn(userTree);

        TokenProviderImpl tokenProvider = createTokenProvider(r, uc);
        assertNull(tokenProvider.createToken(new SimpleCredentials("uid", new char[0])));
    }

    @Test
    public void testCreateTokenRetry() throws Exception {
        User u = mock(User.class);
        when(u.getPath()).thenReturn("/testuser");
        when(userManager.getAuthorizable(anyString())).thenReturn(u);

        Tree tokenTree = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        when(tokenTree.getProperty(JCR_UUID)).thenReturn(PropertyStates.createProperty(JCR_UUID, UUID.randomUUID().toString()));

        Tree tokenParent = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        when(tokenParent.addChild(anyString())).thenReturn(tokenTree);

        String parentPath = "/testuser/" + TOKENS_NODE_NAME;
        Tree userTree = mock(Tree.class);
        when(userTree.exists()).thenReturn(true);
        when(userTree.getChild(TOKENS_NODE_NAME)).thenReturn(tokenParent);

        Root r = mock(Root.class);
        doAnswer(new Answer() {
            int cnt = 0;
            @Override
            public @Nullable Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (cnt++ == 0) {
                    throw new CommitFailedException(CommitFailedException.CONSTRAINT, 1, "conflict");
                } else {
                    return null;
                }
            }
        }).when(r).commit(CommitMarker.asCommitAttributes());
        when(r.getTree(parentPath)).thenReturn(tokenParent);
        when(r.getTree("/testuser")).thenReturn(userTree);

        TokenProviderImpl tokenProvider = createTokenProvider(r, uc);
        assertNotNull(tokenProvider.createToken(new SimpleCredentials("uid", new char[0])));
    }

    @Test
    public void testCreateTokenCommitParentFails() throws Exception {
        User u = mock(User.class);
        when(u.getPath()).thenReturn("/testuser");
        when(userManager.getAuthorizable(anyString())).thenReturn(u);

        Tree tokenTree = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        when(tokenTree.getProperty(JCR_UUID)).thenReturn(PropertyStates.createProperty(JCR_UUID, UUID.randomUUID().toString()));

        Tree tokenParent = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        when(tokenParent.addChild(anyString())).thenReturn(tokenTree);

        String parentPath = "/testuser/" + TOKENS_NODE_NAME;
        Tree userTree = mock(Tree.class);
        when(userTree.exists()).thenReturn(true);
        when(userTree.getChild(TOKENS_NODE_NAME)).thenReturn(tokenParent);

        Root r = mock(Root.class);
        doThrow(new CommitFailedException(CommitFailedException.CONSTRAINT, 1, "conflict")).when(r).commit();
        when(r.getTree(parentPath)).thenReturn(tokenParent);
        when(r.getTree("/testuser")).thenReturn(userTree);

        TokenProviderImpl tokenProvider = createTokenProvider(r, uc);
        assertNotNull(tokenProvider.createToken(new SimpleCredentials("uid", new char[0])));
    }

    @Test
    public void testCreateTokenUserWithoutPath() throws Exception {
        User u = when(mock(User.class).getPath()).thenThrow(new RepositoryException()).getMock();
        when(userManager.getAuthorizable("uid")).thenReturn(u);

        assertNull(tp.createToken(new SimpleCredentials("uid", new char[0])));
        verify(userManager, times(1)).getAuthorizable("uid");
        verify(u, times(1)).getPath();
    }

    @Test
    public void testGetTokenInfo() throws Exception {
        // generate valid token
        Authorizable user = getTestUser();
        TokenInfo valid = createTokenInfo(tokenProvider, user.getID());

        when(userManager.getAuthorizableByPath(anyString())).thenThrow(new RepositoryException());

        assertNull(tp.getTokenInfo(valid.getToken()));
        verify(userManager, times(1)).getAuthorizableByPath(user.getPath());
    }

}