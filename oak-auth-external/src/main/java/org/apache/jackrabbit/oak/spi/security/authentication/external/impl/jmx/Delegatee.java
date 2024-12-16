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

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncResultImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DynamicSyncContext;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

final class Delegatee {

    private static final Logger log = LoggerFactory.getLogger(Delegatee.class);

    private static final String ERROR_CREATE_DELEGATEE = "Unable to create delegatee";
    private static final String ERROR_SYNC_USER = "Error while syncing user {}";

    private static final int NO_BATCH_SIZE = 0;
    private static final int DEFAULT_BATCH_SIZE = 100;

    private final SyncHandler handler;
    private final ExternalIdentityProvider idp;
    private final UserManager userMgr;

    private final ContentSession systemSession;
    private final Root root;

    private final int batchSize;

    private SyncContext context;

    private Delegatee(@NotNull SyncHandler handler, @NotNull ExternalIdentityProvider idp,
                      @NotNull ContentSession systemSession, @NotNull SecurityProvider securityProvider, int batchSize) {
        this.handler = handler;
        this.idp = idp;

        this.systemSession = systemSession;
        this.batchSize = batchSize;

        root = systemSession.getLatestRoot();
        userMgr = securityProvider.getConfiguration(UserConfiguration.class).getUserManager(root, NamePathMapper.DEFAULT);
        context = handler.createContext(idp, userMgr, new ValueFactoryImpl(root, NamePathMapper.DEFAULT));

        log.info("Created delegatee for SyncMBean with session: {} {}", systemSession, systemSession.getAuthInfo().getUserID());
    }

    @NotNull
    static Delegatee createInstance(@NotNull ContentRepository repository, @NotNull SecurityProvider securityProvider,
                                    @NotNull SyncHandler handler, @NotNull ExternalIdentityProvider idp) {
        return createInstance(repository, securityProvider, handler, idp, DEFAULT_BATCH_SIZE);
    }

    @NotNull
    static Delegatee createInstance(@NotNull final ContentRepository repository,
                                    @NotNull SecurityProvider securityProvider,
                                    @NotNull SyncHandler handler,
                                    @NotNull ExternalIdentityProvider idp,
                                    int batchSize) {
        ContentSession systemSession;
        try {
            systemSession = Java23Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> repository.login(null, null));
        } catch (PrivilegedActionException e) {
            throw new SyncRuntimeException(ERROR_CREATE_DELEGATEE, e);
        }

        return new Delegatee(handler, idp, systemSession, securityProvider, batchSize);
    }

    private static void close(@NotNull ContentSession systemSession) {
        try {
            systemSession.close();
        } catch (IOException e) {
            log.error("Error while closing ContentSession {}", systemSession);
        }
    }

    void close() {
        if (context != null) {
            context.close();
            context = null;
        }
        close(systemSession);
    }

    /**
     * @see SynchronizationMBean#syncUsers(String[], boolean)
     */
    @NotNull
    String[] syncUsers(@NotNull String[] userIds, boolean purge) {
        context.setKeepMissing(!purge)
                .setForceGroupSync(true)
                .setForceUserSync(true);
        ResultMessages messages = new ResultMessages();

        List<SyncResult> results = new ArrayList<>(batchSize);
        for (String userId : userIds) {
            results = syncUser(userId, false, results, messages);
        }
        commit(messages, results, NO_BATCH_SIZE);
        return messages.getMessages();
    }

    /**
     * @see SynchronizationMBean#syncAllUsers(boolean)
     */
    @NotNull
    String[] syncAllUsers(boolean purge) {
        try {
            ResultMessages messages = new ResultMessages();
            context.setKeepMissing(!purge)
                    .setForceGroupSync(true)
                    .setForceUserSync(true);
            Iterator<SyncedIdentity> it = handler.listIdentities(userMgr);

            List<SyncResult> results = new ArrayList<>(batchSize);
            while (it.hasNext()) {
                SyncedIdentity id = it.next();
                if (isMyIDP(id)) {
                    results = syncUser(id.getId(), false, results, messages);
                }
            }
            commit(messages, results, NO_BATCH_SIZE);
            return messages.getMessages();
        } catch (RepositoryException e) {
            throw new IllegalStateException("Error retrieving users for syncing", e);
        }
    }

    /**
     * @see SynchronizationMBean#syncExternalUsers(String[])
     */
    @NotNull
    String[] syncExternalUsers(@NotNull String[] externalIds) {
        ResultMessages messages = new ResultMessages();
        context.setForceGroupSync(true).setForceUserSync(true);

        List<SyncResult> results = new ArrayList<>(batchSize);
        for (String externalId : externalIds) {
            ExternalIdentityRef ref = ExternalIdentityRef.fromString(externalId);
            if (!idp.getName().equals(ref.getProviderName())) {
                results.add(new DefaultSyncResultImpl(new DefaultSyncedIdentity(ref.getId(), ref, false, -1), SyncResult.Status.FOREIGN));
            } else {
                try {
                    ExternalIdentity id = idp.getIdentity(ref);
                    if (id != null) {
                        results = syncUser(id, results, messages);
                    } else {
                        results.add(new DefaultSyncResultImpl(
                                new DefaultSyncedIdentity("", ref, false, -1),
                                SyncResult.Status.NO_SUCH_IDENTITY
                        ));
                    }
                } catch (ExternalIdentityException e) {
                    log.warn("error while fetching the external identity {}", externalId, e);
                    results.add(new ErrorSyncResult(ref, e));
                }
            }
        }
        commit(messages, results, NO_BATCH_SIZE);
        return messages.getMessages();
    }

    /**
     * @see SynchronizationMBean#syncAllExternalUsers()
     */
    @NotNull
    String[] syncAllExternalUsers() {
        ResultMessages messages = new ResultMessages();
        context.setForceGroupSync(true).setForceUserSync(true);
        try {
            List<SyncResult> results = new ArrayList<>(batchSize);
            Iterator<ExternalUser> it = idp.listUsers();
            while (it.hasNext()) {
                ExternalUser user = it.next();
                results = syncUser(user, results, messages);
            }
            commit(messages, results, NO_BATCH_SIZE);
            return messages.getMessages();
        } catch (ExternalIdentityException e) {
            throw new SyncRuntimeException("Unable to retrieve external users", e);
        }
    }

    /**
     * @see SynchronizationMBean#listOrphanedUsers()
     */
    @NotNull
    String[] listOrphanedUsers() {
        return Iterators.toArray(internalListOrphanedIdentities(), String.class);
    }

    /**
     * @see SynchronizationMBean#purgeOrphanedUsers()
     */
    @NotNull
    String[] purgeOrphanedUsers() {
        context.setKeepMissing(false);

        ResultMessages messages = new ResultMessages();
        Iterator<String> orphanedIdentities = internalListOrphanedIdentities();

        List<SyncResult> results = new ArrayList<>(batchSize);
        while (orphanedIdentities.hasNext()) {
            String userId = orphanedIdentities.next();
            results = syncUser(userId, true, results, messages);
        }
        commit(messages, results, NO_BATCH_SIZE);
        return messages.getMessages();
    }

    /**
     * @see SynchronizationMBean#convertToDynamicMembership() ()
     */
    @NotNull
    String[] convertToDynamicMembership() {
        if (!(context instanceof DynamicSyncContext)) {
            log.debug("SyncHandler doesn't have dynamic-membership configured. Ignore request to convert external users.");
            return new String[0];
        }
        
        try {
            ResultMessages messages = new ResultMessages();
            Iterator<SyncedIdentity> it = handler.listIdentities(userMgr);
            List<SyncResult> results = new ArrayList<>();
            while (it.hasNext()) {
                SyncedIdentity id = it.next();
                if (isMyIDP(id) && !id.isGroup()) {
                    Authorizable a = userMgr.getAuthorizable(id.getId());
                    SyncResult.Status status;
                    if (a == null) {
                        status = SyncResult.Status.NO_SUCH_AUTHORIZABLE;
                    } else if (((DynamicSyncContext) context).convertToDynamicMembership(a)) {
                        status = SyncResult.Status.UPDATE;
                    } else {
                        status = SyncResult.Status.NOP;
                    }
                    results.add(new DefaultSyncResultImpl(new DefaultSyncedIdentity(id.getId(), id.getExternalIdRef(), id.isGroup(), id.lastSynced()), status));
                }
            }
            commit(messages, results, NO_BATCH_SIZE);
            return messages.getMessages();
        } catch (RepositoryException e) {
            throw new IllegalStateException("Error during conversion to dynamic membership", e);
        }
    }
    
    //------------------------------------------------------------< private >---

    private boolean isMyIDP(@NotNull SyncedIdentity id) {
        ExternalIdentityRef ref = id.getExternalIdRef();
        String providerName = (ref == null) ? null : ref.getProviderName();
        return providerName != null && providerName.equals(idp.getName());
    }

    @NotNull
    private List<SyncResult> syncUser(@NotNull ExternalIdentity id, @NotNull List<SyncResult> results, @NotNull ResultMessages messages) {
        try {
            SyncResult r = context.sync(id);
            if (r.getIdentity() == null) {
                r = new DefaultSyncResultImpl(
                        new DefaultSyncedIdentity(id.getId(), id.getExternalId(), false, -1),
                        SyncResult.Status.NO_SUCH_IDENTITY
                );
                log.warn("sync failed. {}", r.getIdentity());
            } else {
                log.info("synced {}", r.getIdentity());
            }
            results.add(r);
        } catch (SyncException e) {
            log.error(ERROR_SYNC_USER, id, e);
            results.add(new ErrorSyncResult(id.getExternalId(), e));
        }
        return commit(messages, results, batchSize);
    }

    @NotNull
    private List<SyncResult> syncUser(@NotNull String userId, boolean includeIdpName,
                                      @NotNull List<SyncResult> results, @NotNull ResultMessages messages) {
        try {
            results.add(context.sync(userId));
        } catch (SyncException e) {
            log.warn(ERROR_SYNC_USER, userId, e);
            results.add(new ErrorSyncResult(userId, ((includeIdpName) ? idp.getName() : null), e));
        }
        return commit(messages, results, batchSize);
    }

    @NotNull
    private List<SyncResult> commit(@NotNull ResultMessages messages, @NotNull List<SyncResult> resultList, int size) {
        if (resultList.isEmpty() || resultList.size() < size) {
            return resultList;
        } else {
            try {
                root.commit();
                messages.append(resultList);
            } catch (CommitFailedException e) {
                messages.append(resultList, e);
            } finally {
                // make sure there are not pending changes that would fail the next batches
                root.refresh();
            }
            return new ArrayList<>(size);
        }
    }

    @NotNull
    private Iterator<String> internalListOrphanedIdentities() {
        try {
            Iterator<SyncedIdentity> it = handler.listIdentities(userMgr);
            return Iterators.filter(Iterators.transform(it, syncedIdentity -> {
                if (syncedIdentity != null && isMyIDP(syncedIdentity)) {
                    try {
                        // nonNull-ExternalIdRef has already been asserted by 'isMyIDP'
                        ExternalIdentity extId = idp.getIdentity(requireNonNull(syncedIdentity.getExternalIdRef()));
                        if (extId == null) {
                            return syncedIdentity.getId();
                        }
                    } catch (ExternalIdentityException e) {
                        log.error("Error while fetching external identity {}", syncedIdentity, e);
                    }
                }
                return null;
            }), x -> x != null);
        } catch (RepositoryException e) {
            log.error("Error while listing orphaned users", e);
            return Collections.emptyIterator();
        }
    }
}
