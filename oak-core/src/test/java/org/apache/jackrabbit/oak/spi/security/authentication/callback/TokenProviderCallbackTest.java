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
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class TokenProviderCallbackTest {

    @Test
    public void testCallback() {
        TokenProviderCallback cb = new TokenProviderCallback();

        TokenProvider tp = new TokenProvider() {

            @Override
            public boolean doCreateToken(@Nonnull Credentials credentials) {
                throw new UnsupportedOperationException();
            }

            @CheckForNull
            @Override
            public TokenInfo createToken(@Nonnull Credentials credentials) {
                throw new UnsupportedOperationException();
            }

            @CheckForNull
            @Override
            public TokenInfo createToken(@Nonnull String userId, @Nonnull Map<String, ?> attributes) {
                throw new UnsupportedOperationException();
            }

            @CheckForNull
            @Override
            public TokenInfo getTokenInfo(@Nonnull String token) {
                throw new UnsupportedOperationException();
            }
        };
        cb.setTokenProvider(tp);

        assertSame(tp, cb.getTokenProvider());
    }

}