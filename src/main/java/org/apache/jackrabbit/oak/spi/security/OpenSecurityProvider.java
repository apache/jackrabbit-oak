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
package org.apache.jackrabbit.oak.spi.security;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.OpenLoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAccessControlProvider;
import org.apache.jackrabbit.oak.spi.security.principal.OpenPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserContext;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * OpenSecurityProvider... TODO: review if we really have the need for that once TODO in InitialContent is resolved
 */
public class OpenSecurityProvider implements SecurityProvider {

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(NodeStore nodeStore) {
        return new OpenLoginContextProvider();
    }

    @Nonnull
    @Override
    public AccessControlProvider getAccessControlProvider() {
        return new OpenAccessControlProvider();
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root, ConfigurationParameters options) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public UserContext getUserContext() {
        // TODO
        return new UserContext() {
            @Nonnull
            @Override
            public UserConfig getUserConfig() {
                return new UserConfig();
            }

            @Nonnull
            @Override
            public UserProvider getUserProvider(Root root) {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public MembershipProvider getMembershipProvider(Root root) {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public List<ValidatorProvider> getValidatorProviders() {
                return Collections.emptyList();
            }

            @Nonnull
            @Override
            public UserManager getUserManager(Session session, Root root, NamePathMapper namePathMapper) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Nonnull
    @Override
    public PrincipalConfiguration getPrincipalConfiguration() {
        return new PrincipalConfiguration() {
            @Nonnull
            @Override
            public PrincipalManager getPrincipalManager(Session session, Root root, NamePathMapper namePathMapper) {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
                return new OpenPrincipalProvider();
            }
        };
    }
}