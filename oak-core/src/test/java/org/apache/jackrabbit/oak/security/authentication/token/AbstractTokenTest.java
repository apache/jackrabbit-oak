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

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.SimpleCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import java.util.Collections;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertNotNull;

/**
 * AbstractTokenTest...
 */
public abstract class AbstractTokenTest extends AbstractSecurityTest implements TokenConstants {

    TokenProviderImpl tokenProvider;

    @Before
    public void before() throws Exception {
        super.before();

        root = adminSession.getLatestRoot();
        tokenProvider = createTokenProvider(root, getUserConfiguration());
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @NotNull
    TokenProviderImpl createTokenProvider(@NotNull Root root, @NotNull UserConfiguration userConfiguration) {
        return createTokenProvider(root, getTokenConfig(), userConfiguration, SimpleCredentialsSupport.getInstance());
    }

    @NotNull
    TokenProviderImpl createTokenProvider(@NotNull Root root, @NotNull ConfigurationParameters options, @NotNull UserConfiguration userConfiguration, @NotNull CredentialsSupport credentialsSupport) {
        return new TokenProviderImpl(root, options, userConfiguration, credentialsSupport);
    }

    @NotNull
    ConfigurationParameters getTokenConfig() {
        return ConfigurationParameters.EMPTY;
    }

    @NotNull
    Tree getTokenTree(@NotNull TokenInfo info) {
        String token = info.getToken();
        int pos = token.indexOf('_');
        String nodeId = (pos == -1) ? token : token.substring(0, pos);
        Tree t = new IdentifierManager(root).getTree(nodeId);
        assertNotNull(t);
        return t;
    }

    @NotNull
    Tree replaceTokenTree(@NotNull TokenInfo base, @NotNull Tree parent,
                          @NotNull String ntName) throws AccessDeniedException {
        Tree tokenTree = getTokenTree(base);
        Tree tree = TreeUtil.addChild(parent, "token", ntName);
        tree.setProperty(tokenTree.getProperty(JcrConstants.JCR_UUID));
        tree.setProperty(tokenTree.getProperty(TOKEN_ATTRIBUTE_KEY));
        tree.setProperty(tokenTree.getProperty(TOKEN_ATTRIBUTE_EXPIRY));
        tokenTree.remove();
        return tree;
    }

    @NotNull
    Tree getUserTree(@NotNull String uid) throws RepositoryException {
        return root.getTree(checkNotNull(getUserManager(root).getAuthorizable(uid)).getPath());
    }

    @NotNull
    static TokenInfo createTokenInfo(@NotNull TokenProvider tp, @NotNull String userId) {
        TokenInfo info = tp.createToken(userId, Collections.emptyMap());
        assertNotNull(info);
        return info;
    }

    static void waitUntilExpired(@NotNull TokenInfo info) {
        long now = System.currentTimeMillis();
        while (!info.isExpired(now)) {
            now = waitForSystemTimeIncrement(now);
        }
    }
}
