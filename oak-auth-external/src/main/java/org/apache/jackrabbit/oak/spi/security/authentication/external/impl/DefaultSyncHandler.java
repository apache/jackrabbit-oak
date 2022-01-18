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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import com.google.common.collect.Iterators;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipAware;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * {@code DefaultSyncHandler} implements an sync handler that synchronizes users and groups from an external identity
 * provider with the repository users.
 * <p>
 * Please refer to {@link DefaultSyncConfigImpl} for configuration options.
 */
@Component(
        // note that the metatype information is generated from DefaultSyncConfig
        policy = ConfigurationPolicy.REQUIRE
)
@Service
public class DefaultSyncHandler implements SyncHandler, AutoMembershipAware {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncHandler.class);

    /**
     * internal configuration
     */
    private DefaultSyncConfig config;
    
    private AutoMembershipTracker autoMembershipTracker;

    /**
     * Default constructor for OSGi
     */
    @SuppressWarnings("UnusedDeclaration")
    public DefaultSyncHandler() {
    }

    /**
     * Constructor for non-OSGi cases.
     *
     * @param config the configuration
     */
    public DefaultSyncHandler(DefaultSyncConfig config) {
        this.config = config;
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    public void activate(@NotNull BundleContext bundleContext, Map<String, Object> properties) {
        ConfigurationParameters cfg = ConfigurationParameters.of(properties);
        config = DefaultSyncConfigImpl.of(cfg);

        autoMembershipTracker = new AutoMembershipTracker();
        autoMembershipTracker.start(new OsgiWhiteboard(bundleContext));
        config.user().setAutoMembershipConfig(autoMembershipTracker);
        config.group().setAutoMembershipConfig(autoMembershipTracker);
    }
    
    @Deactivate
    public void deactivate() {
        if (autoMembershipTracker != null) {
            autoMembershipTracker.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public String getName() {
        return config.getName();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public SyncContext createContext(@NotNull ExternalIdentityProvider idp, @NotNull UserManager userManager,
                                     @NotNull ValueFactory valueFactory) {
        if (config.user().getDynamicMembership()) {
            return new DynamicSyncContext(config, idp, userManager, valueFactory);
        } else {
            return new DefaultSyncContext(config, idp, userManager, valueFactory);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SyncedIdentity findIdentity(@NotNull UserManager userManager, @NotNull String id)
            throws RepositoryException {
        return DefaultSyncContext.createSyncedIdentity(userManager.getAuthorizable(id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requiresSync(@NotNull SyncedIdentity identity) {
        if (identity.getExternalIdRef() == null || identity.lastSynced() < 0) {
            return true;
        }
        final long now = System.currentTimeMillis();
        final long expirationTime = identity.isGroup()
                ? config.group().getExpirationTime()
                : config.user().getExpirationTime();
        return now - identity.lastSynced() > expirationTime;
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public Iterator<SyncedIdentity> listIdentities(@NotNull UserManager userManager) throws RepositoryException {
        final Iterator<Authorizable> iter = userManager.findAuthorizables(DefaultSyncContext.REP_EXTERNAL_ID, null);
        return new AbstractLazyIterator<SyncedIdentity>() {

            @Override
            protected @Nullable SyncedIdentity getNext() {
                while (iter.hasNext()) {
                    try {
                        SyncedIdentity id = DefaultSyncContext.createSyncedIdentity(iter.next());
                        if (id != null && id.getExternalIdRef() != null) {
                            return id;
                        }
                    } catch (RepositoryException e) {
                        log.error("Error while fetching authorizables", e);
                        break;
                    }
                }
                return null;
            }
        };
    }

    //----------------------------------------------------------------------------------------< AutoMembershipAware >---
    @Override
    public @NotNull AutoMembershipConfig getAutoMembershipConfig() {
        return (autoMembershipTracker == null || autoMembershipTracker.isEmpty()) ? AutoMembershipConfig.EMPTY : autoMembershipTracker;
    }

    /**
     * Internal tracker for {@link AutoMembershipConfig} services that might apply to this sync handler instance.
     * In addition it implements {@link AutoMembershipConfig} wrapping around the list of applicable.
     */
    private final class AutoMembershipTracker extends AbstractServiceTracker<AutoMembershipConfig> implements AutoMembershipConfig {
        
        public AutoMembershipTracker() {
            super(AutoMembershipConfig.class, Collections.singletonMap(AutoMembershipConfig.PARAM_SYNC_HANDLER_NAME, DefaultSyncHandler.this.getName()));
        }
        
        private boolean isEmpty() {
            return getServices().isEmpty();
        }

        @Override
        public String getName() {
            return DefaultSyncHandler.this.getName();
        }

        @Override
        public @NotNull Set<String> getAutoMembership(@NotNull Authorizable authorizable) {
            Set<String> groupIds = new HashSet<>();
            getServices().forEach(autoMembershipConfig -> groupIds.addAll(autoMembershipConfig.getAutoMembership(authorizable)));
            return groupIds;
        }
        
        @Override
        public Iterator<Authorizable> getAutoMembers(@NotNull UserManager userManager, @NotNull Group group) {
            return Iterators.concat(getServices().stream().map(autoMembershipConfig -> autoMembershipConfig.getAutoMembers(userManager, group)).iterator());
        }
    }
}
