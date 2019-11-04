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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EmptyPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@code PrincipalConfiguration} interface that provides
 * principal management for {@code Group principals} associated with
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity external identities}
 * managed outside of the scope of the repository by an
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider}.
 *
 * @since Oak 1.5.3
 * @see <a href="https://issues.apache.org/jira/browse/OAK-4101">OAK-4101</a>
 */
@Component(
        metatype = true,
        label = "Apache Jackrabbit Oak External PrincipalConfiguration",
        immediate = true
)
@Service({PrincipalConfiguration.class, SecurityConfiguration.class})
@Properties({
        @Property(name = ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDS,
                label = "External Identity Protection",
                description = "If disabled rep:externalId properties won't be properly protected (backwards compatible behavior). NOTE: for security reasons it is strongly recommend to keep the protection enabled!",
                boolValue = ExternalIdentityConstants.DEFAULT_PROTECT_EXTERNAL_IDS),
        @Property(name = OAK_SECURITY_NAME,
                propertyPrivate= true, 
                value = "org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration")
})
public class ExternalPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ExternalPrincipalConfiguration.class);

    private SyncConfigTracker syncConfigTracker;
    private SyncHandlerMappingTracker syncHandlerMappingTracker;

    @SuppressWarnings("UnusedDeclaration")
    public ExternalPrincipalConfiguration() {
        super();
    }

    public ExternalPrincipalConfiguration(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    //---------------------------------------------< PrincipalConfiguration >---
    @NotNull
    @Override
    public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
        return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
    }

    @NotNull
    @Override
    public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
        if (dynamicMembershipEnabled()) {
            UserConfiguration uc = getSecurityProvider().getConfiguration(UserConfiguration.class);
            return new ExternalGroupPrincipalProvider(root, uc, namePathMapper, syncConfigTracker.getAutoMembership());
        } else {
            return EmptyPrincipalProvider.INSTANCE;
        }
    }

    //----------------------------------------------< SecurityConfiguration >---
    @NotNull
    @Override
    public String getName() {
        return NAME;
    }

    @NotNull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new ExternalIdentityRepositoryInitializer(protectedExternalIds());
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull MoveTracker moveTracker) {
        return Collections.singletonList(new ExternalIdentityValidatorProvider(principals, protectedExternalIds()));
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.singletonList(new ExternalIdentityImporter());
    }

    @NotNull
    @Override
    public List<ThreeWayConflictHandler> getConflictHandlers() {
        return Collections.singletonList(new ExternalIdentityConflictHandler());
    }

    //----------------------------------------------------< SCR integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(BundleContext bundleContext, Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));

        syncHandlerMappingTracker = new SyncHandlerMappingTracker(bundleContext);
        syncHandlerMappingTracker.open();

        syncConfigTracker = new SyncConfigTracker(bundleContext, syncHandlerMappingTracker);
        syncConfigTracker.open();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        if (syncConfigTracker != null) {
            syncConfigTracker.close();
        }
        if (syncHandlerMappingTracker != null) {
            syncHandlerMappingTracker.close();
        }
    }

    //------------------------------------------------------------< private >---

    private boolean dynamicMembershipEnabled() {
        return syncConfigTracker != null && syncConfigTracker.isEnabled();
    }

    private boolean protectedExternalIds() {
        return getParameters().getConfigValue(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDS, ExternalIdentityConstants.DEFAULT_PROTECT_EXTERNAL_IDS);
    }
}
