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
import static org.junit.Assert.assertFalse;

import java.util.Map;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.SimpleCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.event.Level;

public class TokenCleanupTest extends AbstractTokenTest {

    private String userId;

    @Override
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    @NotNull
    @Override
    ConfigurationParameters getTokenConfig() {
        return ConfigurationParameters.of(TokenProviderImpl.PARAM_TOKEN_CLEANUP_THRESHOLD, 5);
    }

    private void assertTokenNodes(int expectedNumber) throws Exception {
        Tree tokenParent = root.getTree(getTestUser().getPath() + '/' + TokenConstants.TOKENS_NODE_NAME);
        assertEquals(expectedNumber, tokenParent.getChildrenCount(expectedNumber*2));
    }

    private void createExpiredTokens(int numberOfTokens) {
        for (int i = 0; i < numberOfTokens; i++) {
            TokenInfo tokenInfo = tokenProvider.createToken(userId, Map.of(TokenProvider.PARAM_TOKEN_EXPIRATION, 2));
            // wait until the info created has expired
            if (tokenInfo != null) {
                waitUntilExpired(tokenInfo);
            }
        }
    }

    private int createTokensUntilCleanup() {
        int tkn = 0;
        boolean clean = false;
        while (!clean && tkn < 50) {
            TokenInfo tokenInfo = createTokenInfo(tokenProvider, userId);
            clean = TokenProviderImpl.shouldRunCleanup(tokenInfo.getToken());
            tkn++;
        }
        return tkn;
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
            tokenProvider.createToken(userId, Map.of());
        }
        assertTokenNodes(10);
    }

    @Test
    public void testBatchSizeLimitsCleanup() throws Exception {
        int batchSize = 3;
        // create more expired tokens than the batch size
        int expiredCount = batchSize + 2;
        // keep the cleanup threshold above the number of expired tokens so that
        // cleanup is not triggered while the expired tokens are being created;
        // this guarantees a single cleanup run during the extras loop below and
        // makes the batch-size assertion deterministic.
        int threshold = expiredCount + 1;
        TokenProviderImpl tp = createTokenProvider(root,
                ConfigurationParameters.of(
                        TokenProviderImpl.PARAM_TOKEN_CLEANUP_THRESHOLD, threshold,
                        TokenProviderImpl.PARAM_TOKEN_CLEANUP_BATCH_SIZE, batchSize),
                getUserConfiguration(), SimpleCredentialsSupport.getInstance());

        for (int i = 0; i < expiredCount; i++) {
            TokenInfo info = tp.createToken(userId, Map.of(TokenProvider.PARAM_TOKEN_EXPIRATION, 2));
            if (info != null) {
                waitUntilExpired(info);
            }
        }

        // create non-expired tokens until one triggers cleanup
        int extras = 0;
        boolean cleaned = false;
        while (!cleaned && extras < 50) {
            TokenInfo info = createTokenInfo(tp, userId);
            cleaned = TokenProviderImpl.shouldRunCleanup(info.getToken());
            extras++;
        }

        // only batchSize expired tokens should have been removed; the rest remain
        assertTokenNodes((expiredCount - batchSize) + extras);
    }

    @Test
    public void testWarnThresholdLogged() {
        int warnThreshold = 5;
        TokenProviderImpl tp = createTokenProvider(root,
                ConfigurationParameters.of(
                        TokenProviderImpl.PARAM_TOKEN_CLEANUP_THRESHOLD, warnThreshold,
                        TokenProviderImpl.PARAM_TOKEN_WARN_THRESHOLD, warnThreshold),
                getUserConfiguration(), SimpleCredentialsSupport.getInstance());

        LogCustomizer log = LogCustomizer
                .forLogger(TokenProviderImpl.class.getName())
                .enable(Level.WARN)
                .create();
        log.starting();
        try {
            // stay below warn threshold — no warning expected
            for (int i = 0; i < warnThreshold - 1; i++) {
                createTokenInfo(tp, userId);
            }
            assertEquals(0, log.getLogs().size());

            // cross the warn threshold and trigger cleanup
            boolean cleaned = false;
            while (!cleaned) {
                TokenInfo info = createTokenInfo(tp, userId);
                cleaned = TokenProviderImpl.shouldRunCleanup(info.getToken());
            }
            assertFalse("expected a warn log entry for excessive token count", log.getLogs().isEmpty());
        } finally {
            log.finished();
        }
    }
}
