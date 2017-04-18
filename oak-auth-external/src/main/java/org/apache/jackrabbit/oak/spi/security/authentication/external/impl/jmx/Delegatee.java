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

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.json.JsonUtil;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
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
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Delegatee(@Nonnull SyncHandler handler, @Nonnull ExternalIdentityProvider idp,
                      @Nonnull ContentSession systemSession, @Nonnull SecurityProvider securityProvider, int batchSize) throws SyncException {
        this.handler = handler;
        this.idp = idp;

        this.systemSession = systemSession;
        this.batchSize = batchSize;

        root = systemSession.getLatestRoot();
        userMgr = securityProvider.getConfiguration(UserConfiguration.class).getUserManager(root, NamePathMapper.DEFAULT);
        context = handler.createContext(idp, userMgr, new ValueFactoryImpl(root, NamePathMapper.DEFAULT));

        log.info("Created delegatee for SyncMBean with session: {} {}", systemSession, systemSession.getAuthInfo().getUserID());
    }

    static Delegatee createInstance(@Nonnull ContentRepository repository, @Nonnull SecurityProvider securityProvider,
                                    @Nonnull SyncHandler handler, @Nonnull ExternalIdentityProvider idp) {
        return createInstance(repository, securityProvider, handler, idp, DEFAULT_BATCH_SIZE);
    }

    static Delegatee createInstance(@Nonnull final ContentRepository repository,
                                    @Nonnull SecurityProvider securityProvider,
                                    @Nonnull SyncHandler handler,
                                    @Nonnull ExternalIdentityProvider idp,
                                    int batchSize) {
        ContentSession systemSession;
        try {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
                @Override
                public ContentSession run() throws NoSuchWorkspaceException, LoginException {
                    return repository.login(null, null);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new SyncRuntimeException(ERROR_CREATE_DELEGATEE, e);
        }

        try {
            return new Delegatee(handler, idp, systemSession, securityProvider, batchSize);
        } catch (SyncException e) {
            close(systemSession);
            throw new SyncRuntimeException(ERROR_CREATE_DELEGATEE, e);
        }
    }

    private static void close(@Nonnull ContentSession systemSession) {
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
    @Nonnull
    String[] syncUsers(@Nonnull String[] userIds, boolean purge) {
        context.setKeepMissing(!purge)
                .setForceGroupSync(true)
                .setForceUserSync(true);
        List<String> list = new ArrayList<String>();

        List<SyncResult> results = new ArrayList<SyncResult>(batchSize);
        for (String userId : userIds) {
            results = syncUser(userId, false, results, list);
        }
        commit(list, results, NO_BATCH_SIZE);
        return list.toArray(new String[list.size()]);
    }

    /**
     * @see SynchronizationMBean#syncAllUsers(boolean)
     */
    @Nonnull
    String[] syncAllUsers(boolean purge) {
        try {
            List<String> list = new ArrayList<String>();
            context.setKeepMissing(!purge)
                    .setForceGroupSync(true)
                    .setForceUserSync(true);
            Iterator<SyncedIdentity> it = handler.listIdentities(userMgr);

            List<SyncResult> results = new ArrayList<SyncResult>(batchSize);
            while (it.hasNext()) {
                SyncedIdentity id = it.next();
                if (isMyIDP(id)) {
                    results = syncUser(id.getId(), false, results, list);
                }
            }
            commit(list, results, NO_BATCH_SIZE);
            return list.toArray(new String[list.size()]);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Error retrieving users for syncing", e);
        }
    }

    /**
     * @see SynchronizationMBean#syncExternalUsers(String[])
     */
    @Nonnull
    String[] syncExternalUsers(@Nonnull String[] externalIds) {
        List<String> list = new ArrayList<String>();
        context.setForceGroupSync(true).setForceUserSync(true);

        List<SyncResult> results = new ArrayList<SyncResult>(batchSize);
        for (String externalId : externalIds) {
            ExternalIdentityRef ref = ExternalIdentityRef.fromString(externalId);
            if (!idp.getName().equals(ref.getProviderName())) {
                results.add(new DefaultSyncResultImpl(new DefaultSyncedIdentity(ref.getId(), ref, false, -1), SyncResult.Status.FOREIGN));
            } else {
                try {
                    ExternalIdentity id = idp.getIdentity(ref);
                    if (id != null) {
                        results = syncUser(id, results, list);
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
        commit(list, results, NO_BATCH_SIZE);
        return list.toArray(new String[list.size()]);
    }

    /**
     * @see SynchronizationMBean#syncAllExternalUsers()
     */
    @Nonnull
    String[] syncAllExternalUsers() {
        List<String> list = new ArrayList<String>();
        context.setForceGroupSync(true).setForceUserSync(true);
        try {
            List<SyncResult> results = new ArrayList<SyncResult>(batchSize);
            Iterator<ExternalUser> it = idp.listUsers();
            while (it.hasNext()) {
                ExternalUser user = it.next();
                results = syncUser(user, results, list);
            }
            commit(list, results, NO_BATCH_SIZE);
            return list.toArray(new String[list.size()]);
        } catch (ExternalIdentityException e) {
            throw new SyncRuntimeException("Unable to retrieve external users", e);
        }
    }

    /**
     * @see SynchronizationMBean#listOrphanedUsers()
     */
    @Nonnull
    String[] listOrphanedUsers() {
        return Iterators.toArray(internalListOrphanedIdentities(), String.class);
    }

    /**
     * @see SynchronizationMBean#purgeOrphanedUsers()
     */
    @Nonnull
    String[] purgeOrphanedUsers() {
        context.setKeepMissing(false);
        List<String> list = new ArrayList<String>();
        Iterator<String> orphanedIdentities = internalListOrphanedIdentities();

        List<SyncResult> results = new ArrayList<SyncResult>(batchSize);
        while (orphanedIdentities.hasNext()) {
            String userId = orphanedIdentities.next();
            results = syncUser(userId, true, results, list);
        }
        commit(list, results, NO_BATCH_SIZE);
        return list.toArray(new String[list.size()]);
    }

    //------------------------------------------------------------< private >---

    private boolean isMyIDP(@Nonnull SyncedIdentity id) {
        ExternalIdentityRef ref = id.getExternalIdRef();
        String providerName = (ref == null) ? null : ref.getProviderName();
        return providerName != null && (providerName.isEmpty() || providerName.equals(idp.getName()));
    }

    @Nonnull
    private List<SyncResult> syncUser(@Nonnull ExternalIdentity id, @Nonnull List<SyncResult> results, @Nonnull List<String> list) {
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
        return commit(list, results, batchSize);
    }

    private List<SyncResult> syncUser(@Nonnull String userId, boolean includeIdpName,
                                      @Nonnull List<SyncResult> results, @Nonnull List<String> list) {
        try {
            results.add(context.sync(userId));
        } catch (SyncException e) {
            log.warn(ERROR_SYNC_USER, userId, e);
            results.add(new ErrorSyncResult(userId, ((includeIdpName) ? idp.getName() : null), e));
        }
        return commit(list, results, batchSize);
    }

    private List<SyncResult> commit(@Nonnull List<String> list, @Nonnull List<SyncResult> resultList, int size) {
        if (resultList.isEmpty() || resultList.size() < size) {
            return resultList;
        } else {
            try {
                root.commit();
                append(list, resultList);
            } catch (CommitFailedException e) {
                append(list, resultList, e);
            } finally {
                // make sure there are not pending changes that would fail the next batches
                root.refresh();
            }
            return new ArrayList<SyncResult>(size);
        }
    }

    @Nonnull
    private Iterator<String> internalListOrphanedIdentities() {
        try {
            Iterator<SyncedIdentity> it = handler.listIdentities(userMgr);
            return Iterators.filter(Iterators.transform(it, new Function<SyncedIdentity, String>() {
                @Nullable
                @Override
                public String apply(@Nullable SyncedIdentity syncedIdentity) {
                    if (syncedIdentity != null && isMyIDP(syncedIdentity)) {
                        ExternalIdentityRef ref = syncedIdentity.getExternalIdRef();
                        try {
                            ExternalIdentity extId = (ref == null) ? null : idp.getIdentity(ref);
                            if (extId == null) {
                                return syncedIdentity.getId();
                            }
                        } catch (ExternalIdentityException e) {
                            log.error("Error while fetching external identity {}", syncedIdentity, e);
                        }
                    }
                    return null;
                }
            }), Predicates.notNull());
        } catch (RepositoryException e) {
            log.error("Error while listing orphaned users", e);
            return Iterators.emptyIterator();
        }
    }

    private static void append(@Nonnull List<String> list, @Nonnull SyncResult r) {
        if (r instanceof ErrorSyncResult) {
            ((ErrorSyncResult) r).append(list);
        } else {
            append(list, r.getIdentity(), getOperationFromStatus(r.getStatus()), null);
        }
    }

    private static void append(@Nonnull List<String> list, @CheckForNull SyncedIdentity syncedIdentity, @Nonnull Exception e) {
        append(list, syncedIdentity, "ERR", e.toString());
    }

    private static void append(@Nonnull List<String> list, @CheckForNull SyncedIdentity syncedIdentity, @Nonnull String op, @CheckForNull String msg) {
        String uid = JsonUtil.getJsonString((syncedIdentity == null ? null : syncedIdentity.getId()));
        ExternalIdentityRef externalIdentityRef = (syncedIdentity == null) ? null : syncedIdentity.getExternalIdRef();
        String eid = (externalIdentityRef == null) ? "\"\"" : JsonUtil.getJsonString(externalIdentityRef.getString());

        if (msg == null) {
            list.add(String.format("{op:\"%s\",uid:%s,eid:%s}", op, uid, eid));
        } else {
            list.add(String.format("{op:\"%s\",uid:%s,eid:%s,msg:%s}", op, uid, eid, JsonUtil.getJsonString(msg)));
        }
    }

    private static void append(@Nonnull List<String> list, @Nonnull List<SyncResult> results) {
        for (SyncResult result : results) {
            append(list, result);
        }
    }

    private static void append(@Nonnull List<String> list, @Nonnull List<SyncResult> results, @Nonnull Exception e) {
        for (SyncResult result : results) {
            if (result instanceof ErrorSyncResult) {
                ((ErrorSyncResult) result).append(list);
            } else {
                SyncResult.Status st = result.getStatus();
                switch (st) {
                    case ADD:
                    case DELETE:
                    case UPDATE:
                    case ENABLE:
                    case DISABLE:
                        append(list, result.getIdentity(), e);
                        break;
                    default:
                        append(list, result);
                }
            }
        }
    }

    private static String getOperationFromStatus(SyncResult.Status syncStatus) {
        String op;
        switch (syncStatus) {
            case NOP:
                op = "nop";
                break;
            case ADD:
                op = "add";
                break;
            case UPDATE:
                op = "upd";
                break;
            case DELETE:
                op = "del";
                break;
            case ENABLE:
                op = "ena";
                break;
            case DISABLE:
                op = "dis";
                break;
            case NO_SUCH_AUTHORIZABLE:
                op = "nsa";
                break;
            case NO_SUCH_IDENTITY:
                op = "nsi";
                break;
            case MISSING:
                op = "mis";
                break;
            case FOREIGN:
                op = "for";
                break;
            default:
                op = "";
        }
        return op;
    }

    private static final class ErrorSyncResult implements SyncResult {

        private final SyncedIdentity syncedIdentity;
        private final Exception error;

        private ErrorSyncResult(@Nonnull String userId, @CheckForNull String idpName, @Nonnull Exception error) {
            ExternalIdentityRef ref = (idpName != null) ? new ExternalIdentityRef(userId, idpName) : null;
            this.syncedIdentity = new DefaultSyncedIdentity(userId, ref, false, -1);
            this.error = error;
        }

        private ErrorSyncResult(@Nonnull ExternalIdentityRef ref, @Nonnull Exception error) {
            this.syncedIdentity = new DefaultSyncedIdentity(ref.getId(), ref, false, -1);
            this.error = error;
        }

        @Nonnull
        @Override
        public SyncedIdentity getIdentity() {
            return syncedIdentity;
        }

        @Nonnull
        @Override
        public Status getStatus() {
            return Status.NOP;
        }

        private void append(@Nonnull List<String> list) {
            Delegatee.append(list, syncedIdentity, error);
        }
    }
}
