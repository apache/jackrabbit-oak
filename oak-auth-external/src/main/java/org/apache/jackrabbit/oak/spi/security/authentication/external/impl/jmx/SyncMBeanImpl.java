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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link SynchronizationMBean} interface.
 */
public class SyncMBeanImpl implements SynchronizationMBean {

    private static final Logger log = LoggerFactory.getLogger(SyncMBeanImpl.class);

    private final ContentRepository repository;

    private final SecurityProvider securityProvider;

    private final SyncManager syncManager;

    private final String syncName;

    private final ExternalIdentityProviderManager idpManager;

    private final String idpName;

    public SyncMBeanImpl(@Nonnull ContentRepository repository, @Nonnull SecurityProvider securityProvider,
                         @Nonnull SyncManager syncManager, @Nonnull String syncName,
                         @Nonnull ExternalIdentityProviderManager idpManager, @Nonnull String idpName) {
        this.repository = repository;
        this.securityProvider = securityProvider;
        this.syncManager = syncManager;
        this.syncName = syncName;
        this.idpManager = idpManager;
        this.idpName = idpName;
    }

    @Nonnull
    private Delegatee getDelegatee() {
        SyncHandler handler = syncManager.getSyncHandler(syncName);
        if (handler == null) {
            log.error("No sync manager available for name {}.", syncName);
            throw new IllegalArgumentException("No sync manager available for name " + syncName);
        }
        ExternalIdentityProvider idp = idpManager.getProvider(idpName);
        if (idp == null) {
            log.error("No idp available for name", idpName);
            throw new IllegalArgumentException("No idp manager available for name " + idpName);
        }
        return Delegatee.createInstance(repository, securityProvider, handler, idp);
    }

    //-----------------------------------------------< SynchronizationMBean >---
    @Nonnull
    @Override
    public String getSyncHandlerName() {
        return syncName;
    }

    @Nonnull
    @Override
    public String getIDPName() {
        return idpName;
    }

    @Nonnull
    @Override
    public String[] syncUsers(@Nonnull String[] userIds, boolean purge) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncUsers(userIds, purge);
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] syncAllUsers(boolean purge) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncAllUsers(purge);
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] syncExternalUsers(@Nonnull String[] externalIds) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncExternalUsers(externalIds);
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] syncAllExternalUsers() {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncAllExternalUsers();
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] listOrphanedUsers() {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.listOrphanedUsers();
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] purgeOrphanedUsers() {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.purgeOrphanedUsers();
        } finally {
            delegatee.close();
        }
    }
}