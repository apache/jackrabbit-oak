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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.DebugTimer;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.SimpleCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ExternalLoginModule} implements a {@code LoginModule} that uses an
 * {@link ExternalIdentityProvider} for authentication.
 */
public class ExternalLoginModule extends AbstractLoginModule {

    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModule.class);

    private static final int MAX_SYNC_ATTEMPTS = 50;

    /**
     * Name of the parameter that configures the name of the external identity provider.
     */
    public static final String PARAM_IDP_NAME = SyncHandlerMapping.PARAM_IDP_NAME;

    /**
     * Name of the parameter that configures the name of the synchronization handler.
     */
    public static final String PARAM_SYNC_HANDLER_NAME = SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;

    private ExternalIdentityProviderManager idpManager;

    private SyncManager syncManager;

    private CredentialsSupport credentialsSupport = SimpleCredentialsSupport.getInstance();

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
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> opts) {
        super.initialize(subject, callbackHandler, sharedState, opts);

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
        if (idpName.isEmpty()) {
            log.error("External login module needs IPD name. Will not be used for login.");
        } else {
            if (idpManager == null) {
                idpManager = WhiteboardUtils.getService(whiteboard, ExternalIdentityProviderManager.class);
            }
            if (idpManager == null) {
                log.error("External login module needs IDPManager. Will not be used for login.");
            } else {
                idp = idpManager.getProvider(idpName);
                if (idp == null) {
                    log.error("No IDP found with name {}. Will not be used for login.", idpName);
                }
            }
        }

        String syncHandlerName = options.getConfigValue(PARAM_SYNC_HANDLER_NAME, "");
        if (syncHandlerName.isEmpty()) {
            log.error("External login module needs SyncHandler name. Will not be used for login.");
        } else {
            if (syncManager == null) {
                syncManager = WhiteboardUtils.getService(whiteboard, SyncManager.class);
            }
            if (syncManager == null) {
                log.error("External login module needs SyncManager. Will not be used for login.");
            } else {
                syncHandler = syncManager.getSyncHandler(syncHandlerName);
                if (syncHandler == null) {
                    log.error("No SyncHandler found with name {}. Will not be used for login.", syncHandlerName);
                }
            }
        }

        if (idp instanceof CredentialsSupport) {
            credentialsSupport = (CredentialsSupport) idp;
        } else {
            log.debug("No 'SupportedCredentials' configured. Using default implementation supporting 'SimpleCredentials'.");
        }
    }

    @Override
    public boolean login() throws LoginException {
        if (idp == null || syncHandler == null) {
            return false;
        }
        credentials = getCredentials();

        // check if we have a pre authenticated login from a previous login module
        final PreAuthenticatedLogin preAuthLogin = getSharedPreAuthLogin();
        final String userId = getUserId(preAuthLogin, credentials);

        if (userId == null && credentials == null) {
            log.debug("No credentials|userId found for external login module. ignoring.");
            return false;
        }

        // remember identification for log-output
        Object logId = (userId != null) ? userId : credentials;
        try {
            // check if there exists a user with the given ID that has been synchronized
            // before into the repository.
            SyncedIdentity sId = getSyncedIdentity(userId);

            // if there exists an authorizable with the given userid (syncedIdentity != null),
            // ignore it if any of the following conditions is met:
            // - identity is local (i.e. not an external identity)
            // - identity belongs to another IDP
            // - identity is valid but we have a preAuthLogin and the user doesn't need an updating sync (OAK-3508)
            if (ignore(sId, preAuthLogin)) {
                return false;
            }

            if (preAuthLogin != null) {
                externalUser = idp.getUser(preAuthLogin.getUserId());
            } else {
                externalUser = idp.authenticate(credentials);
            }

            if (externalUser != null) {
                log.debug("IDP {} returned valid user {}", idp.getName(), externalUser);

                if (credentials != null) {
                    //noinspection unchecked
                    sharedState.put(SHARED_KEY_CREDENTIALS, credentials);
                }

                //noinspection unchecked
                sharedState.put(SHARED_KEY_LOGIN_NAME, externalUser.getId());

                syncUser(externalUser);

                return true;
            } else {
                debug("IDP {} returned null for {}", idp.getName(), logId.toString());

                if (sId != null) {
                    // invalidate the user if it exists as synced variant
                    log.debug("local user exists for '{}'. re-validating.", sId.getId());
                    validateUser(sId.getId());
                }
                return false;
            }
        } catch (ExternalIdentityException e) {
            log.error("Error while authenticating '{}' with {}", logId, idp.getName(), e);
            return false;
        } catch (LoginException e) {
            log.debug("IDP {} throws login exception for '{}': {}", idp.getName(), logId, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.debug("SyncHandler {} throws sync exception for '{}'", syncHandler.getName(), logId, e);
            LoginException le = new LoginException("Error while syncing user.");
            le.initCause(e);
            throw le;
        }
    }

    @Override
    public boolean commit() throws LoginException {
        if (externalUser == null) {
            // login attempt in this login module was not successful
            clearState();
            return false;
        }
        Set<? extends Principal> principals = getPrincipals(externalUser.getId());
        if (!principals.isEmpty()) {
            if (!subject.isReadOnly()) {
                subject.getPrincipals().addAll(principals);
                if (credentials != null) {
                    subject.getPublicCredentials().add(credentials);
                }
                setAuthInfo(createAuthInfo(externalUser.getId(), principals), subject);
            } else {
                log.debug("Could not add information to read only subject {}", subject);
            }
            return true;
        }
        clearState();
        return false;
    }

    @Override
    public boolean abort() throws LoginException {
        clearState();
        // do we need to remove the user again, in case we created it during login() ?
        return true;
    }

    //------------------------------------------------------------< private >---

    @CheckForNull
    private String getUserId(@CheckForNull PreAuthenticatedLogin preAuthLogin, @CheckForNull Credentials credentials) {
        if (preAuthLogin != null) {
            return preAuthLogin.getUserId();
        } else if (credentials != null){
            return credentialsSupport.getUserId(credentials);
        } else {
            return null;
        }
    }

    @CheckForNull
    private SyncedIdentity getSyncedIdentity(@CheckForNull String userId) throws RepositoryException {
        UserManager userMgr = getUserManager();
        if (userId != null && userMgr != null) {
            return syncHandler.findIdentity(userMgr, userId);
        } else {
            return null;
        }
    }

    private boolean ignore(@CheckForNull SyncedIdentity syncedIdentity, @CheckForNull PreAuthenticatedLogin preAuthLogin) {
        if (syncedIdentity != null) {
            ExternalIdentityRef externalIdRef = syncedIdentity.getExternalIdRef();
            if (externalIdRef == null) {
                debug("ignoring local user: {}", syncedIdentity.getId());
                return true;
            } else if (!idp.getName().equals(externalIdRef.getProviderName())) {
                debug("ignoring foreign identity: {} (idp={})", externalIdRef.getString(), idp.getName());
                return true;
            }

            if (preAuthLogin != null && !syncHandler.requiresSync(syncedIdentity)) {
                debug("pre-authenticated external user {} does not require syncing.", syncedIdentity.toString());
                return true;
            }
        }
        return false;
    }

    /**
     * Initiates synchronization of the external user.
     * @param user the external user
     * @throws SyncException if an error occurs
     */
    private void syncUser(@Nonnull ExternalUser user) throws SyncException {
        Root root = getRoot();
        if (root == null) {
            throw new SyncException("Cannot synchronize user. root == null");
        }
        UserManager userManager = getUserManager();
        if (userManager == null) {
            throw new SyncException("Cannot synchronize user. userManager == null");
        }

        int numAttempt = 0;
        while (numAttempt++ < MAX_SYNC_ATTEMPTS) {
            SyncContext context = null;
            try {
                DebugTimer timer = new DebugTimer();
                context = syncHandler.createContext(idp, userManager, new ValueFactoryImpl(root, NamePathMapper.DEFAULT));
                SyncResult syncResult =  context.sync(user);
                timer.mark("sync");
                if (root.hasPendingChanges()) {
                    root.commit();
                    timer.mark("commit");
                }
                debug("syncUser({}) {}, status: {}", user.getId(), timer.getString(), syncResult.getStatus().toString());
                return;
            } catch (CommitFailedException e) {
                log.warn("User synchronization failed during commit: {}. (attempt {}/{})", e.toString(), numAttempt, MAX_SYNC_ATTEMPTS);
                root.refresh();
            } finally {
                if (context != null) {
                    context.close();
                }
            }
        }
        throw new SyncException("User synchronization failed during commit after " + MAX_SYNC_ATTEMPTS + " attempts");
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
            context = syncHandler.createContext(idp, userManager, new ValueFactoryImpl(root, NamePathMapper.DEFAULT));
            context.sync(id);
            timer.mark("sync");
            root.commit();
            timer.mark("commit");
            debug("validateUser({}) {}", id, timer.getString());
        } catch (CommitFailedException e) {
            throw new SyncException("User synchronization failed during commit.", e);
        } finally {
            if (context != null) {
                context.close();
            }
        }

    }

    @Nonnull
    private AuthInfo createAuthInfo(@Nonnull String userId, @Nonnull Set<? extends Principal> principals) {
        Credentials creds;
        if (credentials instanceof ImpersonationCredentials) {
            creds = ((ImpersonationCredentials) credentials).getBaseCredentials();
        } else {
            creds = credentials;
        }
        Map<String, Object> attributes = new HashMap<String, Object>();
        Object shared = sharedState.get(SHARED_KEY_ATTRIBUTES);
        if (shared instanceof Map) {
            for (Map.Entry entry : ((Map<?,?>) shared).entrySet()) {
                attributes.put(entry.getKey().toString(), entry.getValue());
            }
        } else if (creds != null) {
            attributes.putAll(credentialsSupport.getAttributes(creds));
        }
        return new AuthInfoImpl(userId, attributes, principals);
    }

    private static void debug(@Nonnull String msg, String... args) {
        if (log.isDebugEnabled()) {
            log.debug(msg, args);
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
     * @return the set of credentials classes as exposed by the configured
     * {@link CredentialsSupport} implementation.
     */
    @Nonnull
    @Override
    protected Set<Class> getSupportedCredentials() {
        return credentialsSupport.getCredentialClasses();
    }

    //----------------------------------------------< public setters (JAAS) >---

    public void setSyncManager(@Nonnull SyncManager syncManager) {
        this.syncManager = syncManager;
    }

    public void setIdpManager(@Nonnull ExternalIdentityProviderManager idpManager) {
        this.idpManager = idpManager;
    }
}