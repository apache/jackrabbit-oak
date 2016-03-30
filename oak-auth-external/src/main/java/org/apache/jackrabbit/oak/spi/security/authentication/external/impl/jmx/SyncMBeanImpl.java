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
import javax.jcr.Repository;

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

    private final Repository repository;

    private final SyncManager syncManager;

    private final String syncName;

    private final ExternalIdentityProviderManager idpManager;

    private final String idpName;

    public SyncMBeanImpl(Repository repository, SyncManager syncManager, String syncName,
                         ExternalIdentityProviderManager idpManager, String idpName) {
        this.repository = repository;
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
        return Delegatee.createInstance(repository, handler, idp);
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
        return syncUsers(userIds, purge, true);
    }

    @Nonnull
    @Override
    public String[] syncUsers(@Nonnull String[] userIds, boolean purge, boolean forceGroupSync) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncUsers(userIds, purge, forceGroupSync);
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] syncAllUsers(boolean purge) {
        return syncAllUsers(purge, true);
    }

    @Nonnull
    @Override
    public String[] syncAllUsers(boolean purge, boolean forceGroupSync) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncAllUsers(purge, forceGroupSync);
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] syncExternalUsers(@Nonnull String[] externalIds) {
        return syncExternalUsers(externalIds, true);
    }

    @Nonnull
    @Override
    public String[] syncExternalUsers(@Nonnull String[] externalIds, boolean forceGroupSync) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncExternalUsers(externalIds, forceGroupSync);
        } finally {
            delegatee.close();
        }
    }

    @Nonnull
    @Override
    public String[] syncAllExternalUsers() {
        return syncAllExternalUsers(true);
    }

    @Nonnull
    @Override
    public String[] syncAllExternalUsers(boolean forceGroupSync) {
        Delegatee delegatee = getDelegatee();
        try {
            return delegatee.syncAllExternalUsers(forceGroupSync);
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