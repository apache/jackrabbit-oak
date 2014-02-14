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

        try {
            externalUser = idp.authenticate(credentials);
            if (externalUser != null) {
                if (log.isDebugEnabled()) {
                    log.debug("IDP {} returned valid user {}", idp.getName(), externalUser);
                }

                //noinspection unchecked
                sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

                //noinspection unchecked
                sharedState.put(SHARED_KEY_LOGIN_NAME, externalUser.getId());

                return true;
            } else {
                if (log.isDebugEnabled()) {
                    if (credentials instanceof SimpleCredentials) {
                        log.debug("IDP {} returned null for simple creds of {}", idp.getName(), ((SimpleCredentials) credentials).getUserID());
                    } else {
                        log.debug("IDP {} returned null for {}", idp.getName(), credentials);
                    }
                }
            }
        } catch (ExternalIdentityException e) {
            log.error("Error while authenticating credentials {} with {}: {}", new Object[]{
                    credentials, idp.getName(), e.toString()});
            return false;
        } catch (LoginException e) {
            if (log.isDebugEnabled()) {
                log.debug("IDP {} throws login exception for {}", idp.getName(), credentials);
            }
            throw e;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return An immutable set containing only the {@link SimpleCredentials} class.
     */
    @Override
    protected Set<Class> getSupportedCredentials() {
        // todo: maybe delegate getSupportedCredentials to IDP
        Class scClass = SimpleCredentials.class;
        return Collections.singleton(scClass);
    }

    @Override
    public boolean commit() throws LoginException {
        if (externalUser == null || syncHandler == null) {
            return false;
        }

        SyncContext context = null;
        try {
            Root root = getRoot();
            UserManager userManager = getUserManager();
            if (root == null || userManager == null) {
                throw new LoginException("Cannot synchronize user.");
            }
            context = syncHandler.createContext(idp, userManager, root);
            context.sync(externalUser);
            root.commit();

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
        } catch (SyncException e) {
            throw new LoginException("User synchronization failed: " + e);
        } catch (CommitFailedException e) {
            throw new LoginException("User synchronization failed: " + e);
        } finally {
            if (context != null) {
                context.close();
            }
        }
    }

    @Override
    protected void clearState() {
        super.clearState();
        externalUser = null;
        credentials = null;
    }
}