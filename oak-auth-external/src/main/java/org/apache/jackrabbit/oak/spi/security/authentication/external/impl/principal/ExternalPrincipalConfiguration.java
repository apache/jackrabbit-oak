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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
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
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor.ExternalIdentityMonitor;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor.ExternalIdentityMonitorImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EmptyPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipService;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDENTITIES;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN;

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
        immediate = true,
        service = {
                PrincipalConfiguration.class,
                SecurityConfiguration.class
        },
        property = {
                "oak.security.name=org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration",
                "protectExternalId:Boolean=true",
        }
)
@Designate(
        ocd = ExternalPrincipalConfiguration.Configuration.class
)
public class ExternalPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration",
            name = "Apache Jackrabbit Oak External PrincipalConfiguration"
    )
    @interface Configuration {
        @AttributeDefinition(
                name = "External Identity Protection",
                description = "If disabled rep:externalId properties won't be properly protected (backwards compatible behavior). NOTE: for security reasons it is strongly recommend to keep the protection enabled!"
        )
        boolean protectExternalId() default ExternalIdentityConstants.DEFAULT_PROTECT_EXTERNAL_IDS;

        @AttributeDefinition(
                name = "External User and Group Protection",
                description = "If 'None' is selected the synchronized external users/groups won't be protected (backwards compatible behavior) and can be edited like local users/groups. NOTE: in order to avoid having inconsistencies between the IDP that defines the external identities and local synced identities it is recommend to enable the protection. With option 'Warn' the protection is disabled but warnings will be logged.",
                options = {
                        @Option(
                                label = VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE,
                                value = VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE
                        ),
                        @Option(
                                label = VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN,
                                value = VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN
                        ),
                        @Option(
                                label = VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED,
                                value = VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED
                        )
                }
        )
        String protectExternalIdentities();

        @AttributeDefinition(
                name = "System Principal Names",
                description = "Names of additional 'SystemUserPrincipal' instances that are excluded from the protection check. Note that this configuration does not grant the required permission to perform the operation.",
                cardinality = 10
        )
        String[] systemPrincipalNames();
    }
    
    private SyncConfigTracker syncConfigTracker;
    private SyncHandlerMappingTracker syncHandlerMappingTracker;
    private ProtectionConfigTracker protectionConfigTracker;
    
    private ServiceRegistration automembershipRegistration;
    private ServiceRegistration dynamicMembershipRegistration;

    private ExternalIdentityMonitor monitor = ExternalIdentityMonitor.NOOP;

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
            return new ExternalGroupPrincipalProvider(root, uc, namePathMapper, syncConfigTracker);
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
        boolean isSystem = new SystemPrincipalConfig(getPrincipalNames()).containsSystemPrincipal(principals);
       
        ImmutableList.Builder<ValidatorProvider> vps = new ImmutableList.Builder<>();
        vps.add(new ExternalIdentityValidatorProvider(isSystem, protectedExternalIds()));

        Set<String> idpNamesWithDynamicGroups = getIdpNamesWithDynamicGroups();
        if (!idpNamesWithDynamicGroups.isEmpty()) {
            vps.add(new DynamicGroupValidatorProvider(getRootProvider(), getTreeProvider(), getSecurityProvider(), idpNamesWithDynamicGroups));
        }
        
        IdentityProtectionType ipt = getIdentityProtectionType();
        if (ipt != IdentityProtectionType.NONE && !isSystem) {
            vps.add(new ExternalUserValidatorProvider(getRootProvider(), getTreeProvider(), getSecurityProvider(), ipt, getProtectionConfig()));
        }
        return vps.build();
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.singletonList(new ExternalIdentityImporter(new SystemPrincipalConfig(getPrincipalNames())));
    }

    @Override
    public @NotNull Iterable<Monitor<?>> getMonitors(@NotNull StatisticsProvider statisticsProvider) {
        monitor = new ExternalIdentityMonitorImpl(statisticsProvider);
        return Collections.singleton(monitor);
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

        protectionConfigTracker = new ProtectionConfigTracker(bundleContext);
        protectionConfigTracker.open();
        
        automembershipRegistration = bundleContext.registerService(DynamicMembershipService.class.getName(), new AutomembershipService(syncConfigTracker), null);
        dynamicMembershipRegistration = bundleContext.registerService(DynamicMembershipService.class.getName(), new DynamicGroupMembershipService(syncConfigTracker), null);
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
        if (protectionConfigTracker != null) {
            protectionConfigTracker.close();
        }
        if (automembershipRegistration != null) {
            automembershipRegistration.unregister();
        }
        if (dynamicMembershipRegistration != null) {
            dynamicMembershipRegistration.unregister();
        }
    }

    //------------------------------------------------------------< private >---

    private boolean dynamicMembershipEnabled() {
        return syncConfigTracker != null && syncConfigTracker.isEnabled();
    }
    
    private @NotNull Set<String> getIdpNamesWithDynamicGroups() {
        return (syncConfigTracker == null) ? Collections.emptySet() : syncConfigTracker.getIdpNamesWithDynamicGroups();
    }

    private boolean protectedExternalIds() {
        return getParameters().getConfigValue(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDS, ExternalIdentityConstants.DEFAULT_PROTECT_EXTERNAL_IDS);
    }

    private @NotNull IdentityProtectionType getIdentityProtectionType() {
        return IdentityProtectionType.fromLabel(getParameters().getConfigValue(PARAM_PROTECT_EXTERNAL_IDENTITIES, VALUE_PROTECT_EXTERNAL_IDENTITIES_NONE));
    }
    
    private @NotNull ProtectionConfig getProtectionConfig() {
        return (protectionConfigTracker == null) ? ProtectionConfig.DEFAULT : protectionConfigTracker;
    }
    
    @NotNull
    private Set<String> getPrincipalNames() {
        return getParameters().getConfigValue(ExternalIdentityConstants.PARAM_SYSTEM_PRINCIPAL_NAMES, Collections.emptySet());
    }
}
