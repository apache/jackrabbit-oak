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
package org.apache.jackrabbit.oak.security.principal;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;

/**
 * Default implementation of the {@code PrincipalConfiguration}
 */
@Component(
        service = {PrincipalConfiguration.class, SecurityConfiguration.class},
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl")
public class PrincipalConfigurationImpl extends ConfigurationBase implements PrincipalConfiguration {

    @SuppressWarnings("UnusedDeclaration")
    public PrincipalConfigurationImpl() {
        super();
    }

    public PrincipalConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }


    //---------------------------------------------< PrincipalConfiguration >---
    @Nonnull
    @Override
    public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
        PrincipalProvider principalProvider = getPrincipalProvider(root, namePathMapper);
        return new PrincipalManagerImpl(principalProvider);
    }

    @Nonnull
    @Override
    public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
        UserConfiguration uc = getSecurityProvider().getConfiguration(UserConfiguration.class);
        PrincipalProvider principalProvider = uc.getUserPrincipalProvider(root, namePathMapper);
        if (principalProvider != null) {
            // use user-implementation specific principal provider implementation
            return principalProvider;
        } else {
            // use default implementation acting on user management API
            return new PrincipalProviderImpl(root, uc, namePathMapper);
        }
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }
}
