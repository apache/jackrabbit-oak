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
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.OpenLoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * OpenSecurityProvider... TODO: review if we really have the need for that once TODO in InitialContent is resolved
 */
public class OpenSecurityProvider implements SecurityProvider {

    @Nonnull
    @Override
    public Iterable<SecurityConfiguration> getSecurityConfigurations() {
        return Collections.<SecurityConfiguration>singletonList(getAccessControlProvider());
    }

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(NodeStore nodeStore, QueryIndexProvider indexProvider) {
        return new OpenLoginContextProvider();
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public AccessControlConfiguration getAccessControlProvider() {
        return new OpenAccessControlConfiguration();
    }

    @Nonnull
    @Override
    public PrivilegeConfiguration getPrivilegeConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public UserConfiguration getUserConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public PrincipalConfiguration getPrincipalConfiguration() {
        throw new UnsupportedOperationException();
    }
}
