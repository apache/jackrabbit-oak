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

import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DefaultSyncHandler implements SyncHandler {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncHandler.class);

    /**
     * internal configuration
     */
    private DefaultSyncConfig config;

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
    private void activate(Map<String, Object> properties) {
        ConfigurationParameters cfg = ConfigurationParameters.of(properties);
        config = DefaultSyncConfigImpl.of(cfg);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public String getName() {
        return config.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public SyncContext createContext(@Nonnull ExternalIdentityProvider idp, @Nonnull UserManager userManager,
                                     @Nonnull ValueFactory valueFactory) throws SyncException {
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
    public SyncedIdentity findIdentity(@Nonnull UserManager userManager, @Nonnull String id)
            throws RepositoryException {
        return DefaultSyncContext.createSyncedIdentity(userManager.getAuthorizable(id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requiresSync(@Nonnull SyncedIdentity identity) {
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
    @Nonnull
    @Override
    public Iterator<SyncedIdentity> listIdentities(@Nonnull UserManager userManager) throws RepositoryException {
        final Iterator<Authorizable> iter = userManager.findAuthorizables(DefaultSyncContext.REP_EXTERNAL_ID, null);
        return new AbstractLazyIterator<SyncedIdentity>() {

            @Override
            protected SyncedIdentity getNext() {
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
}