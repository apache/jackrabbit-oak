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
package org.apache.jackrabbit.oak.security;

import javax.annotation.Nonnull;
import javax.jcr.Session;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.authentication.ConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.LoginContextProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.AccessControlProviderImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalProviderImpl;
import org.apache.jackrabbit.oak.security.user.UserContextImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserContext;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class SecurityProviderImpl implements SecurityProvider {

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(NodeStore nodeStore) {
        // TODO: use configurable authentication config
        Configuration configuration = new ConfigurationImpl();
        return new LoginContextProviderImpl(configuration, nodeStore, this);
    }

    @Nonnull
    @Override
    public AccessControlProvider getAccessControlProvider() {
        return new AccessControlProviderImpl();
    }

    @Nonnull
    @Override
    public UserContext getUserContext() {
        return new UserContextImpl();
    }

    @Nonnull
    @Override
    public PrincipalConfiguration getPrincipalConfiguration() {
        return new PrincipalConfiguration() {
            @Nonnull
            @Override
            public PrincipalManager getPrincipalManager(Session session, Root root, NamePathMapper namePathMapper) {
                PrincipalProvider principalProvider = getPrincipalProvider(root, namePathMapper);
                return new PrincipalManagerImpl(principalProvider);
            }

            @Nonnull
            @Override
            public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
                UserContext userContext = getUserContext();
                UserProvider userProvider = userContext.getUserProvider(root);
                MembershipProvider msProvider = userContext.getMembershipProvider(root);
                return new PrincipalProviderImpl(userProvider, msProvider, namePathMapper);
            }
        };
    }
}
