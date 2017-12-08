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

import static org.junit.Assert.assertEquals;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

public class TokenCleanupTest extends AbstractTokenTest {

    private String userId;

    @Override
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    @Override
    ConfigurationParameters getTokenConfig() {
        return ConfigurationParameters.of(TokenProviderImpl.PARAM_TOKEN_CLEANUP_THRESHOLD, 5);
    }

    private void assertTokenNodes(int expectedNumber) throws Exception {
        Tree tokenParent = root.getTree(getTestUser().getPath() + '/' + TokenConstants.TOKENS_NODE_NAME);
        assertEquals(expectedNumber, tokenParent.getChildrenCount(expectedNumber*2));
    }

    private void createExpiredTokens(int numberOfTokens) throws Exception {
        for (int i = 0; i < numberOfTokens; i++) {
            TokenInfo tokenInfo = tokenProvider.createToken(userId, ImmutableMap.of(TokenProvider.PARAM_TOKEN_EXPIRATION, 2));
            // wait until the info created has expired
            if (tokenInfo != null) {
                waitUntilExpired(tokenInfo);
            }
        }
    }

    private int createTokensUntilCleanup() throws Exception {
        int tkn = 0;
        boolean clean = false;
        while (!clean && tkn < 50) {
            TokenInfo tokenInfo = tokenProvider.createToken(userId, ImmutableMap.of());
            clean = TokenProviderImpl.shouldRunCleanup(tokenInfo.getToken());
            tkn++;
        }
        return tkn;
    }

    private void waitUntilExpired(@Nonnull TokenInfo info) {
        long now = System.currentTimeMillis();
        while (!info.isExpired(now)) {
            now = waitForSystemTimeIncrement(now);
        }
    }

    @Test
    public void testExpiredBelowThreshold() throws Exception {
        createExpiredTokens(4);
        assertTokenNodes(4);
    }

    @Test
    public void testExpiredReachingThreshold() throws Exception {
        // one under the cleanup limit so cleanup doesn't get triggered
        createExpiredTokens(4);
        int extras = createTokensUntilCleanup();
        assertTokenNodes(extras);
    }

    @Test
    public void testNotExpiredReachingThreshold() throws Exception {
        for (int i = 0; i < 10; i++) {
            tokenProvider.createToken(userId, ImmutableMap.of());
        }
        assertTokenNodes(10);
    }
}