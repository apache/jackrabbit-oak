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

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExternalLoginModule implements a LoginModule that uses and external identity provider for login.
 */
public class ExternalLoginModule extends AbstractLoginModule {

    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModule.class);

    /**
     * Name of the parameter that configures the name of the external identity provider.
     */
    public static final String PARAM_IDP_NAME = "idp.name";

    /**
     * Name of the parameter that configures the name of the synchronization handler.
     */
    public static final String PARAM_SYNC_HANDLER_NAME = "sync.handlerName";

    /**
     * internal configuration when invoked from a factory rather than jaas
     */
    private ConfigurationParameters osgiConfig;

    /**
     * The external identity provider as specified by the {@link #PARAM_IDP_NAME}
     */
    private ExternalIdentityProvider idp;

    /**
     * The configured sync handler as specified by the {@link #PARAM_SYNC_HANDLER_NAME}
     */
    private SyncHandler syncHandler;

    /**
     * The external user as resolved in the login call.
     */
    private ExternalUser externalUser;

    /**
     * Login credentials
     */
    private Credentials credentials;

    /**
     * Default constructor for the OSGIi LoginModuleFactory case and the default non-OSGi JAAS case.
     */
    @SuppressWarnings("UnusedDeclaration")
    public ExternalLoginModule() {
    }

    /**
     * Creates a new ExternalLoginModule with the given OSGi config.
     * @param osgiConfig the config
     */
    public ExternalLoginModule(ConfigurationParameters osgiConfig) {
        this.osgiConfig = osgiConfig;
    }

    //--------------------------------------------------------< LoginModule >---
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> ss, Map<String, ?> opts) {
        super.initialize(subject, callbackHandler, ss, opts);

        // merge options with osgi options if needed
        if (osgiConfig != null) {
            options = ConfigurationParameters.of(osgiConfig, options);
        }

        Whiteboard whiteboard = getWhiteboard();
        if (whiteboard == null) {
            log.error("External login module needs whiteboard. Will not be used for login.");
            return;
        }

        String idpName = options.getConfigValue(PARAM_IDP_NAME, "");
        if (idpName.length() == 0) {
            log.error("External login module needs IPD name. Will not be used for login.");
        } else {
            ExternalIdentityProviderManager idpMgr = WhiteboardUtils.getService(whiteboard, ExternalIdentityProviderManager.class);
            if (idpMgr == null) {
                log.error("External login module needs IDPManager. Will not be used for login.");
            } else {
                idp = idpMgr.getProvider(idpName);
                if (idp == null) {
                    log.error("No IDP found with name {}. Will not be used for login.", idpName);
                }
            }
        }

        String syncHandlerName = options.getConfigValue(PARAM_SYNC_HANDLER_NAME, "");
        if (syncHandlerName.length() == 0) {
            log.error("External login module needs SyncHandler name. Will not be used for login.");
        } else {
            SyncManager syncMgr = WhiteboardUtils.getService(whiteboard, SyncManager.class);
            if (syncMgr == null) {
                log.error("External login module needs SyncManager. Will not be used for login.");
            } else {
                syncHandler = syncMgr.getSyncHandler(syncHandlerName);
                if (syncHandler == null) {
                    log.error("No SyncHandler found with name {}. Will not be used for login.", syncHandlerName);
                }
            }
        }
    }

    @Override
    public boolean login() throws LoginException {
        if (idp == null || syncHandler == null) {
            return false;
        }

        credentials = getCredentials();
        if (credentials == null) {
            log.debug("No credentials found for external login module. ignoring.");
            return false;
        }

        // remember userID as we need this so often
        final String userId = credentials instanceof SimpleCredentials ? ((SimpleCredentials) credentials).getUserID() : null;
        try {
            SyncedIdentity sId = null;
            if (userId != null) {
                sId = syncHandler.findIdentity(getUserManager(), userId);
                // if there exists an authorizable with the given userid but is not an external one or if it belongs to
                // another IDP, we just ignore it.
                if (sId != null) {
                    if (sId.getExternalIdRef() == null) {
                        log.debug("ignoring local user: {}", sId.getId());
                        return false;
                    }
                    if (!sId.getExternalIdRef().getProviderName().equals(idp.getName())) {
                        if (log.isDebugEnabled()) {
                            log.debug("ignoring foreign identity: {} (idp={})", sId.getExternalIdRef().getString(), idp.getName());
                        }
                        return false;
                    }
                }
            }

            externalUser = idp.authenticate(credentials);
            if (externalUser != null) {
                log.debug("IDP {} returned valid user {}", idp.getName(), externalUser);

                //noinspection unchecked
                sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

                //noinspection unchecked
                sharedState.put(SHARED_KEY_LOGIN_NAME, externalUser.getId());

                syncUser(externalUser);

                return true;
            } else {
                if (log.isDebugEnabled()) {
                    if (userId != null) {
                        log.debug("IDP {} returned null for simple creds of {}", idp.getName(), userId);
                    } else {
                        log.debug("IDP {} returned null for {}", idp.getName(), credentials);
                    }
                }

                if (sId != null) {
                    // invalidate the user if it exists as synced variant
                    log.debug("local user exists for '{}'. re-validating.", sId.getId());
                    validateUser(sId.getId());
                }
                return false;
            }
        } catch (ExternalIdentityException e) {
            log.error("Error while authenticating '{}' with {}",
                    userId == null ? credentials : userId, idp.getName(), e);
            return false;
        } catch (LoginException e) {
            log.debug("IDP {} throws login exception for '{}': {}",
                    idp.getName(), userId == null ? credentials : userId, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.debug("SyncHandler {} throws sync exception for '{}'",
                    syncHandler.getName(), userId == null ? credentials : userId, e);
            LoginException le = new LoginException("Error while syncing user.");
            le.initCause(e);
            throw le;
        }
    }

    @Override
    public boolean commit() throws LoginException {
        if (externalUser == null) {
            return false;
        }
        Set<? extends Principal> principals = getPrincipals(externalUser.getId());
        if (!principals.isEmpty()) {
            if (!subject.isReadOnly()) {
                subject.getPrincipals().addAll(principals);
                subject.getPublicCredentials().add(credentials);
                setAuthInfo(new AuthInfoImpl(externalUser.getId(), null, principals), subject);
            } else {
                log.debug("Could not add information to read only subject {}", subject);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean abort() throws LoginException {
        clearState();
        // do we need to remove the user again, in case we created it during login() ?
        return true;
    }

    /**
     * Initiates synchronization of the external user.
     * @param user the external user
     * @throws SyncException if an error occurs
     */
    private void syncUser(@Nonnull ExternalUser user) throws SyncException {
        SyncContext context = null;
        try {
            Root root = getRoot();
            if (root == null) {
                throw new SyncException("Cannot synchronize user. root == null");
            }
            UserManager userManager = getUserManager();
            if (userManager == null) {
                throw new SyncException("Cannot synchronize user. userManager == null");
            }
            DebugTimer timer = new DebugTimer();
            context = syncHandler.createContext(idp, userManager, root);
            context.sync(user);
            timer.mark("sync");
            root.commit();
            timer.mark("commit");
            if (log.isDebugEnabled()) {
                log.debug("syncUser({}) {}", user.getId(), timer.getString());
            }
        } catch (CommitFailedException e) {
            throw new SyncException("User synchronization failed during commit.", e);
        } finally {
            if (context != null) {
                context.close();
            }
        }
    }

    /**
     * Initiates synchronization of a possible remove user
     * @param id the user id
     */
    private void validateUser(@Nonnull String id) throws SyncException {
        SyncContext context = null;
        try {
            Root root = getRoot();
            if (root == null) {
                throw new SyncException("Cannot synchronize user. root == null");
            }
            UserManager userManager = getUserManager();
            if (userManager == null) {
                throw new SyncException("Cannot synchronize user. userManager == null");
            }
            DebugTimer timer = new DebugTimer();
            context = syncHandler.createContext(idp, userManager, root);
            context.sync(id);
            timer.mark("sync");
            root.commit();
            timer.mark("commit");
            if (log.isDebugEnabled()) {
                log.debug("validateUser({}) {}", id, timer.getString());
            }
        } catch (CommitFailedException e) {
            throw new SyncException("User synchronization failed during commit.", e);
        } finally {
            if (context != null) {
                context.close();
            }
        }

    }
    //------------------------------------------------< AbstractLoginModule >---

    @Override
    protected void clearState() {
        super.clearState();
        externalUser = null;
        credentials = null;
    }

    /**
     * @return An immutable set containing only the {@link SimpleCredentials} class.
     */
    @Override
    protected Set<Class> getSupportedCredentials() {
        // TODO: maybe delegate getSupportedCredentials to IDP
        Class scClass = SimpleCredentials.class;
        return Collections.singleton(scClass);
    }
}