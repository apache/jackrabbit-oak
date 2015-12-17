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

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.json.JsonUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Delegatee {

    private static final Logger log = LoggerFactory.getLogger(Delegatee.class);

    private static final String ERROR_CREATE_DELEGATEE = "Unable to create delegatee";
    private static final String ERROR_SYNC_USER = "Error while syncing user {}";

    private final SyncHandler handler;
    private final ExternalIdentityProvider idp;
    private final UserManager userMgr;
    private final Session systemSession;

    private SyncContext context;

    private Delegatee(@Nonnull SyncHandler handler, @Nonnull ExternalIdentityProvider idp,
                      @Nonnull JackrabbitSession systemSession) throws SyncException, RepositoryException {
        this.handler = handler;
        this.idp = idp;

        this.systemSession = systemSession;
        this.userMgr = systemSession.getUserManager();
        this.context = handler.createContext(idp, userMgr, systemSession.getValueFactory());

        log.info("Created delegatee for SyncMBean with session: {} {}", systemSession, systemSession.getUserID());
    }

    static Delegatee createInstance(final @Nonnull Repository repository, @Nonnull SyncHandler handler, @Nonnull ExternalIdentityProvider idp) {
        Session systemSession;
        try {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<Session>() {
                @Override
                public Session run() throws RepositoryException {
                    if (repository instanceof JackrabbitRepository) {
                        // this is to bypass GuestCredentials injection in the "AbstractSlingRepository2"
                        return ((JackrabbitRepository) repository).login(null, null, null);
                    } else {
                        return repository.login(null, null);
                    }
                }
            });
        } catch (PrivilegedActionException e) {
            throw new SyncRuntimeException(ERROR_CREATE_DELEGATEE, e);
        }

        if (!(systemSession instanceof JackrabbitSession)) {
            systemSession.logout();
            throw new SyncRuntimeException("Unable to create SyncContext: JackrabbitSession required.");
        }
        try {
            return new Delegatee(handler, idp, (JackrabbitSession) systemSession);
        } catch (RepositoryException e) {
            systemSession.logout();
            throw new SyncRuntimeException(ERROR_CREATE_DELEGATEE, e);
        } catch (SyncException e) {
            systemSession.logout();
            throw new SyncRuntimeException(ERROR_CREATE_DELEGATEE, e);
        }
    }

    void close() {
        if (context != null) {
            context.close();
            context = null;
        }
        if (systemSession.isLive()) {
            systemSession.logout();
        }
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
        for (String userId: userIds) {
            try {
                append(list, syncUser(userId));
            } catch (SyncException e) {
                log.warn(ERROR_SYNC_USER, userId, e);
            }
        }
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
            Iterator<SyncedIdentity> iter = handler.listIdentities(userMgr);
            while (iter.hasNext()) {
                SyncedIdentity id = iter.next();
                if (isMyIDP(id)) {
                    try {
                        append(list, syncUser(id.getId()));
                    } catch (SyncException e) {
                        log.error(ERROR_SYNC_USER, id, e);
                        append(list, id, e);
                    }
                }
            }
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
        for (String externalId : externalIds) {
            ExternalIdentityRef ref = ExternalIdentityRef.fromString(externalId);
            try {
                ExternalIdentity id = idp.getIdentity(ref);
                SyncResult r;
                if (id != null) {
                    r = syncUser(id);
                } else {
                    r = new DefaultSyncResultImpl(
                            new DefaultSyncedIdentity("", ref, false, -1),
                            SyncResult.Status.NO_SUCH_IDENTITY
                    );
                }
                append(list, r);
            } catch (ExternalIdentityException e) {
                log.warn("error while fetching the external identity {}", externalId, e);
                append(list, ref, e);
            } catch (SyncException e) {
                log.error(ERROR_SYNC_USER, ref, e);
                append(list, ref, e);
            }
        }
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
            Iterator<ExternalUser> iter = idp.listUsers();
            while (iter.hasNext()) {
                ExternalUser user = iter.next();
                try {
                    SyncResult r = syncUser(user);
                    if (r.getIdentity() == null) {
                        r = new DefaultSyncResultImpl(
                                new DefaultSyncedIdentity(user.getId(), user.getExternalId(), false, -1),
                                SyncResult.Status.NO_SUCH_IDENTITY
                        );
                        log.warn("sync failed. {}", r.getIdentity());
                    } else {
                        log.info("synced {}", r.getIdentity());
                    }
                    append(list, r);
                } catch (SyncException e) {
                    log.error(ERROR_SYNC_USER, user, e);
                    append(list, user.getExternalId(), e);
                }
            }
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
        List<String> list = new ArrayList<String>();
        try {
            Iterator<SyncedIdentity> iter = handler.listIdentities(userMgr);
            while (iter.hasNext()) {
                SyncedIdentity id = iter.next();
                if (isMyIDP(id)) {
                    try {
                        ExternalIdentityRef ref = id.getExternalIdRef();
                        ExternalIdentity extId = (ref == null) ? null : idp.getIdentity(ref);
                        if (extId == null) {
                            list.add(id.getId());
                        }
                    } catch (ExternalIdentityException e) {
                        log.error("Error while fetching external identity {}", id, e);
                    }
                }
            }
        } catch (RepositoryException e) {
            log.error("Error while listing orphaned users", e);
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * @see SynchronizationMBean#purgeOrphanedUsers()
     */
    @Nonnull
    String[] purgeOrphanedUsers() {
        context.setKeepMissing(false);
        List<String> list = new ArrayList<String>();
        for (String userId : listOrphanedUsers()) {
            try {
                append(list, syncUser(userId));
            } catch (SyncException e) {
                log.warn(ERROR_SYNC_USER, userId, e);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    //------------------------------------------------------------< private >---

    private boolean isMyIDP(@Nonnull SyncedIdentity id) {
        ExternalIdentityRef ref = id.getExternalIdRef();
        String providerName = (ref == null) ? null : ref.getProviderName();
        return providerName != null && (providerName.isEmpty() || providerName.equals(idp.getName()));
    }


    @Nonnull
    private SyncResult syncUser(@Nonnull ExternalIdentity id) throws SyncException {
        try {
            SyncResult r = context.sync(id);
            systemSession.save();
            return r;
        } catch (RepositoryException e) {
            throw new SyncException(e);
        }
    }

    @Nonnull
    private SyncResult syncUser(@Nonnull String userId) throws SyncException {
        try {
            SyncResult r = context.sync(userId);
            systemSession.save();
            return r;
        } catch (RepositoryException e) {
            throw new SyncException(e);
        }
    }

    private static void append(@Nonnull List<String> list, @Nonnull SyncResult r) {
        SyncedIdentity syncedIdentity = r.getIdentity();
        String uid = JsonUtil.getJsonString((syncedIdentity == null ? null : syncedIdentity.getId()));
        ExternalIdentityRef externalIdentityRef = (syncedIdentity == null) ? null : syncedIdentity.getExternalIdRef();
        String eid = (externalIdentityRef == null) ? "\"\"" : JsonUtil.getJsonString(externalIdentityRef.getString());
        String jsonStr = String.format("{op:\"%s\",uid:%s,eid:%s}", getOperationFromStatus(r.getStatus()), uid, eid);
        list.add(jsonStr);
    }

    private static void append(@Nonnull List<String> list, @Nonnull ExternalIdentityRef idRef, @Nonnull Exception e) {
        String eid = JsonUtil.getJsonString(idRef.getString());
        String msg = JsonUtil.getJsonString(e.toString());
        String jsonStr = String.format("{op:\"ERR\",uid:\"\",eid:%s,msg:%s}", eid, msg);
        list.add(jsonStr);
    }

    private static void append(@Nonnull List<String> list, @Nonnull SyncedIdentity id, @Nonnull Exception e) {
        String uid = JsonUtil.getJsonString(id.getId());
        ExternalIdentityRef ref = id.getExternalIdRef();
        String eid = (ref == null) ? "\"\"" : JsonUtil.getJsonString(ref.getString());
        String msg = JsonUtil.getJsonString(e.toString());
        list.add(String.format("{op:\"ERR\",uid:%s,eid:%s,msg:%s}", uid, eid, msg));
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
}
