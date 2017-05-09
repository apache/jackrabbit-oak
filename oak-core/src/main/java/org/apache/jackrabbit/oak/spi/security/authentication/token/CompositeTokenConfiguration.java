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

import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

/**
* {@link TokenConfiguration} that combines different token provider implementations.
*/
public class CompositeTokenConfiguration extends CompositeConfiguration<TokenConfiguration> implements TokenConfiguration {

    public CompositeTokenConfiguration() {
        super(TokenConfiguration.NAME);
    }

    public CompositeTokenConfiguration(@Nonnull SecurityProvider securityProvider) {
        super(TokenConfiguration.NAME, securityProvider);
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(final Root root) {
        List<TokenProvider> providers = Lists.transform(getConfigurations(), new Function<TokenConfiguration, TokenProvider>() {
            @Override
            public TokenProvider apply(TokenConfiguration tokenConfiguration) {
                return tokenConfiguration.getTokenProvider(root);
            }
        });
        return CompositeTokenProvider.newInstance(providers);
    }
}
