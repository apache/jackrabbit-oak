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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;

/**
 * AbstractTokenTest...
 */
public abstract class AbstractTokenTest extends AbstractSecurityTest implements TokenConstants {

    TokenProviderImpl tokenProvider;

    @Before
    public void before() throws Exception {
        super.before();

        root = adminSession.getLatestRoot();
        tokenProvider = new TokenProviderImpl(root,
                getTokenConfig(),
                getUserConfiguration());
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    ConfigurationParameters getTokenConfig() {
        return ConfigurationParameters.EMPTY;
    }

    @CheckForNull
    Tree getTokenTree(@Nonnull TokenInfo info) {
        String token = info.getToken();
        int pos = token.indexOf('_');
        String nodeId = (pos == -1) ? token : token.substring(0, pos);
        return new IdentifierManager(root).getTree(nodeId);
    }

    @Nonnull
    Tree createTokenTree(@Nonnull TokenInfo base, @Nonnull NodeUtil parent,
                         @Nonnull String ntName) throws AccessDeniedException {
        Tree tokenTree = getTokenTree(base);
        Tree tree = parent.addChild("token", ntName).getTree();
        tree.setProperty(tokenTree.getProperty(JcrConstants.JCR_UUID));
        tree.setProperty(tokenTree.getProperty(TOKEN_ATTRIBUTE_KEY));
        tree.setProperty(tokenTree.getProperty(TOKEN_ATTRIBUTE_EXPIRY));
        return tree;
    }
}