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
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.json.JsonUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncResultImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncedIdentityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code SyncMBeanImpl}...
 */
public class SyncMBeanImpl implements SynchronizationMBean {

    /**
     * default logger
     */
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
        try {
            return new Delegatee(handler, idp);
        } catch (SyncException e) {
            throw new IllegalArgumentException("Unable to create delegatee", e);
        }
    }

    private class Delegatee {

        private SyncHandler handler;

        private ExternalIdentityProvider idp;

        private SyncContext context;

        private UserManager userMgr;

        private Session systemSession;

        private Delegatee(SyncHandler handler, ExternalIdentityProvider idp) throws SyncException {
            this.handler = handler;
            this.idp = idp;
            try {
                systemSession = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<Session>() {
                    @Override
                    public Session run() throws LoginException, RepositoryException {
                        if (repository instanceof JackrabbitRepository) {
                            // this is to bypass GuestCredentials injection in the "AbstractSlingRepository2"
                            return ((JackrabbitRepository) repository).login(null, null, null);
                        } else {
                            return repository.login(null, null);
                        }
                    }
                });
            } catch (PrivilegedActionException e) {
                throw new SyncException(e);
            }
            try {
                context = handler.createContext(idp, userMgr = ((JackrabbitSession) systemSession).getUserManager(), systemSession.getValueFactory());
            } catch (Exception e) {
                systemSession.logout();
                throw new SyncException(e);
            }

            log.info("Created delegatee for SyncMBean with session: {} {}", systemSession, systemSession.getUserID());
        }

        private void close() {
            if (context != null) {
                context.close();
                context = null;
            }
            if (systemSession != null) {
                systemSession.logout();
            }
        }

        /**
         * @see SynchronizationMBean#syncUsers(String[], boolean)
         */
        @Nonnull
        public String[] syncUsers(@Nonnull String[] userIds, boolean purge) {
            context.setKeepMissing(!purge)
                    .setForceGroupSync(true)
                    .setForceUserSync(true);
            List<String> result = new ArrayList<String>();
            for (String userId: userIds) {
                try {
                    SyncResult r = context.sync(userId);
                    systemSession.save();
                    result.add(getJSONString(r));
                } catch (SyncException e) {
                    log.warn("Error while syncing user {}", userId, e);
                } catch (RepositoryException e) {
                    log.warn("Error while syncing user {}", userId, e);
                }
            }
            return result.toArray(new String[result.size()]);
        }

        /**
         * @see SynchronizationMBean#syncAllUsers(boolean)
         */
        @Nonnull
        public String[] syncAllUsers(boolean purge) {
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
                            SyncResult r = context.sync(id.getId());
                            systemSession.save();
                            list.add(getJSONString(r));
                        } catch (SyncException e) {
                            list.add(getJSONString(id, e));
                        } catch (RepositoryException e) {
                            list.add(getJSONString(id, e));
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
        public String[] syncExternalUsers(@Nonnull String[] externalIds) {
            List<String> list = new ArrayList<String>();
            context.setForceGroupSync(true).setForceUserSync(true);
            for (String externalId: externalIds) {
                ExternalIdentityRef ref = ExternalIdentityRef.fromString(externalId);
                try {
                    ExternalIdentity id = idp.getIdentity(ref);
                    if (id != null) {
                        SyncResult r = context.sync(id);
                        systemSession.save();
                        list.add(getJSONString(r));
                    } else {
                        SyncResult r = new SyncResultImpl(
                                new SyncedIdentityImpl("", ref, false, -1),
                                SyncResult.Status.NO_SUCH_IDENTITY
                        );
                        list.add(getJSONString(r));
                    }
                } catch (ExternalIdentityException e) {
                    log.warn("error while fetching the external identity {}", externalId, e);
                    list.add(getJSONString(ref, e));
                } catch (SyncException e) {
                    list.add(getJSONString(ref, e));
                } catch (RepositoryException e) {
                    list.add(getJSONString(ref, e));
                }
            }
            return list.toArray(new String[list.size()]);
        }

        /**
         * @see SynchronizationMBean#syncAllExternalUsers()
         */
        @Nonnull
        public String[] syncAllExternalUsers() {
            List<String> list = new ArrayList<String>();
            context.setForceGroupSync(true).setForceUserSync(true);
            try {
                Iterator<ExternalUser> iter = idp.listUsers();
                while (iter.hasNext()) {
                    ExternalUser user = iter.next();
                    try {
                        SyncResult r = context.sync(user);
                        systemSession.save();
                        list.add(getJSONString(r));
                    } catch (SyncException e) {
                        list.add(getJSONString(user.getExternalId(), e));
                    } catch (RepositoryException e) {
                        list.add(getJSONString(user.getExternalId(), e));
                    }
                }
                return list.toArray(new String[list.size()]);
            } catch (ExternalIdentityException e) {
                throw new IllegalArgumentException("Unable to retrieve external users", e);
            }
        }

        /**
         * @see SynchronizationMBean#listOrphanedUsers()
         */
        @Nonnull
        public String[] listOrphanedUsers() {
            List<String> list = new ArrayList<String>();
            try {
                Iterator<SyncedIdentity> iter = handler.listIdentities(userMgr);
                while (iter.hasNext()) {
                    SyncedIdentity id = iter.next();
                    if (isMyIDP(id)) {
                        ExternalIdentity extId = idp.getIdentity(id.getExternalIdRef());
                        if (extId == null) {
                            list.add(id.getId());
                        }
                    }
                }
            } catch (RepositoryException e) {
                log.error("Error while listing orphaned users", e);
            } catch (ExternalIdentityException e) {
                log.error("Error while fetching external identity", e);
            }
            return list.toArray(new String[list.size()]);
        }

        /**
         * @see SynchronizationMBean#purgeOrphanedUsers()
         */
        @Nonnull
        public String[] purgeOrphanedUsers() {
            context.setKeepMissing(false);
            List<String> result = new ArrayList<String>();
            for (String userId: listOrphanedUsers()) {
                try {
                    SyncResult r = context.sync(userId);
                    systemSession.save();
                    result.add(getJSONString(r));
                } catch (SyncException e) {
                    log.warn("Error while syncing user {}", userId, e);
                } catch (RepositoryException e) {
                    log.warn("Error while syncing user {}", userId, e);
                }
            }
            return result.toArray(new String[result.size()]);
        }

        private boolean isMyIDP(SyncedIdentity id) {
            String idpName = id.getExternalIdRef() == null
                    ? null
                    : id.getExternalIdRef().getProviderName();
            return (idpName == null || idpName.length() ==0 || idpName.equals(idp.getName()));
        }
    }

    private static String getJSONString(SyncResult r) {
        String op = "";
        switch (r.getStatus()) {
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
        }
        String uid = JsonUtil.getJsonString(r.getIdentity().getId());
        String eid = r.getIdentity().getExternalIdRef() == null
                ? "\"\""
                : JsonUtil.getJsonString(r.getIdentity().getExternalIdRef().getString());
        return String.format("{op:\"%s\",uid:%s,eid:%s}", op, uid, eid);
    }

    private static String getJSONString(SyncedIdentity id, Exception e) {
        String uid = JsonUtil.getJsonString(id.getId());
        String eid = id.getExternalIdRef() == null
                ? "\"\""
                : JsonUtil.getJsonString(id.getExternalIdRef().getString());
        String msg = JsonUtil.getJsonString(e.toString());
        return String.format("{op:\"ERR\",uid:%s,eid:%s,msg:%s}", uid, eid, msg);
    }

    private static String getJSONString(ExternalIdentityRef ref, Exception e) {
        String eid = JsonUtil.getJsonString(ref.getString());
        String msg = JsonUtil.getJsonString(e.toString());
        return String.format("{op:\"ERR\",uid:\"\",eid:%s,msg:%s}", eid, msg);
    }

    //---------------------------------------------------------------------------------------< SynchronizationMBean >---
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