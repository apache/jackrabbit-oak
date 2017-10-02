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
package org.apache.jackrabbit.oak.spi.security.authentication.token;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import com.google.common.collect.ImmutableList;

/**
 * Aggregates a collection of {@link TokenProvider}s into a single
 * provider.
 */
public final class CompositeTokenProvider implements TokenProvider {

    private final List<TokenProvider> providers;

    private CompositeTokenProvider(@Nonnull List<? extends TokenProvider> providers) {
        this.providers = ImmutableList.copyOf(providers);
    }

    @Nonnull
    public static TokenProvider newInstance(@Nonnull TokenProvider... providers) {
        return newInstance(Arrays.<TokenProvider>asList(providers));
    }

    @Nonnull
    public static TokenProvider newInstance(@Nonnull List<? extends TokenProvider> providers) {
        switch (providers.size()) {
            case 0: return NULL_PROVIDER;
            case 1: return providers.iterator().next();
            default: return new CompositeTokenProvider(providers);
        }
    }

    @Override
    public boolean doCreateToken(@Nonnull Credentials credentials) {
        for (TokenProvider tp : providers) {
            if (tp.doCreateToken(credentials)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TokenInfo createToken(@Nonnull Credentials credentials) {
        for (TokenProvider tp : providers) {
            TokenInfo info = tp.createToken(credentials);
            if (info != null) {
                return info;
            }
        }
        return null;
    }

    @Override
    public TokenInfo createToken(@Nonnull String userId, @Nonnull Map<String, ?> attributes) {
        for (TokenProvider tp : providers) {
            TokenInfo info = tp.createToken(userId, attributes);
            if (info != null) {
                return info;
            }
        }
        return null;
    }

    @Override
    public TokenInfo getTokenInfo(@Nonnull String token) {
        for (TokenProvider tp : providers) {
            TokenInfo info = tp.getTokenInfo(token);
            if (info != null) {
                return info;
            }
        }
        return null;
    }

    private static final TokenProvider NULL_PROVIDER = new TokenProvider() {
        @Override
        public boolean doCreateToken(@Nonnull Credentials credentials) {
            return false;
        }

        @Override
        public TokenInfo createToken(@Nonnull Credentials credentials) {
            return null;
        }

        @Override
        public TokenInfo createToken(@Nonnull String userId, @Nonnull Map<String, ?> attributes) {
            return null;
        }

        @Override
        public TokenInfo getTokenInfo(@Nonnull String token) {
            return null;
        }
    };
}