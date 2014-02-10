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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExternalLoginModule implements a LoginModule that uses and external identity provider for login.
 *
 * Note: This is an attempt to combine a login module implementation with a LoginModuleFactory for the Felix JAAS
 * support. This allows to configure the login module normally via jaas config, but also via OSGi config.
 */
@Component(metatype = true, policy = ConfigurationPolicy.REQUIRE, configurationFactory = true)
@Service
public class ExternalLoginModule extends AbstractLoginModule implements LoginModuleFactory {

    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModule.class);

    public static final SyncMode DEFAULT_SYNC_MODE = SyncMode.DEFAULT_SYNC;

    private static final String PARAM_SYNC_HANDLER = "syncHandler";
    private static final String DEFAULT_SYNC_HANDLER = DefaultSyncHandler.class.getName();

    @Property(
            intValue = 900,
            label = "JAAS Ranking",
            description = "Specifying the ranking (i.e. sort order) of this login module entry. The entries are sorted " +
                    "in a descending order (i.e. higher value ranked configurations come first)."
    )
    public static final String JAAS_RANKING = LoginModuleFactory.JAAS_RANKING;

    @Property(
            value = "SUFFICIENT",
            label = "JAAS Control Flag",
            description = "Property specifying whether or not a LoginModule is REQUIRED, REQUISITE, SUFFICIENT or " +
                    "OPTIONAL.Refer to the JAAS configuration documentation for more details around the meaning of " +
                    "these flags."
    )
    public static final String JAAS_CONTROL_FLAG = LoginModuleFactory.JAAS_CONTROL_FLAG;

    @Property(
            label = "JAAS Realm",
            description = "The realm name (or application name) against which the LoginModule  is be registered. If no " +
                    "realm name is provided then LoginModule is registered with a default realm as configured in " +
                    "the Felix JAAS configuration."
    )
    public static final String JAAS_REALM_NAME = LoginModuleFactory.JAAS_REALM_NAME;

    /**
     * Name of the parameter that configures the name of the external identity provider.
     */
    @Property(
            label = "Identity Provider Name",
            description = "Name of the identity provider (for example: 'ldap')."
    )
    public static final String PARAM_IDP_NAME = "idp.name";

    /**
     * Name of the parameter that configures the name of the synchronization handler.
     */
    @Property(
            value = "default",
            label = "Sync Handler Name",
            description = "Name of the sync handler."
    )
    public static final String PARAM_SYNC_HANDLER_NAME = "sync.handlerName";

    /**
     * Name of the parameter that configures the synchronization mode.
     */
    @Property(
            value = "default",
            label = "Sync Mode",
            // TODO!!!
            description = "a comma separated list of sync modes. allowed values: 'user','group','default','update'"
    )
    public static final String PARAM_SYNC_MODE = "sync.mode";

    private ConfigurationParameters osgiConfig;

    /**
     * The external identity provider as specified by the {@link #PARAM_IDP_NAME}
     */
    private ExternalIdentityProvider idp;

    /**
     * The external user as resolved in the login call.
     */
    private ExternalUser externalUser;

    /**
     * Default constructor for the OSGIi LoginModuleFactory case and the default non-OSGi JAAS case.
     */
    public ExternalLoginModule() {
    }

    /**
     * Activates the LoginModuleFactory service (not the LoginModule)
     * @param properties the OSGi config
     */
    @Activate
    protected void activate(Map<String, Object> properties) {
        // only remember the properties here. the real initialization happens later
        osgiConfig = ConfigurationParameters.of(properties);
    }

    @Override
    public LoginModule createLoginModule() {
        log.error("*** creating new login module. config: {}", osgiConfig);
        return new ExternalLoginModule(osgiConfig);
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

        String idpName = options.getConfigValue(PARAM_IDP_NAME, "");
        if (idpName.length() == 0) {
            log.error("External login module needs IPD name. Will not be used for login.");
        } else {
            ExternalIdentityProviderManager idpMgr = getSecurityProvider().getConfiguration(ExternalIdentityProviderManager.class);
            idp = idpMgr.getProvider(idpName);
            if (idp == null) {
                log.error("No IDP found with name {}. Will not be used for login.", idpName);
            }
        }
    }

    @Override
    public boolean login() throws LoginException {
        if (idp == null) {
            return false;
        }

        Credentials credentials = getCredentials();
        if (credentials == null) {
            log.info("No credentials found for external login module. ignoring.");
            return false;
        }

        try {
            ExternalUser externalUser = idp.authenticate(credentials);
            if (externalUser != null) {
                log.debug("Adding Credentials to shared state.");
                //noinspection unchecked
                sharedState.put(SHARED_KEY_CREDENTIALS, credentials);

                log.debug("Adding login name to shared state.");
                //noinspection unchecked
                sharedState.put(SHARED_KEY_LOGIN_NAME, externalUser.getId());

                return true;
            }
        } catch (ExternalIdentityException e) {
            log.error("Error while authenticating credentials {} with {}: {}", new Object[]{
                    credentials, idp.getName(), e.toString()});
            return false;
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
        if (externalUser == null) {
            return false;
        }

        try {
            SyncHandler handler = getSyncHandler();
            Root root = getRoot();
            UserManager userManager = getUserManager();
            if (root == null || userManager == null) {
                throw new LoginException("Cannot synchronize user.");
            }
            Object smValue = options.getConfigValue(PARAM_SYNC_MODE, null, null);
            SyncMode syncMode;
            if (smValue == null) {
                syncMode = DEFAULT_SYNC_MODE;
            } else {
                syncMode = SyncMode.fromObject(smValue);
            }
            if (externalUser != null && handler.initialize(userManager, root, syncMode, options)) {
                handler.sync(externalUser);
                root.commit();
                return true;
            } else {
                log.warn("Failed to initialize sync handler.");
                return false;
            }
        } catch (SyncException e) {
            throw new LoginException("User synchronization failed: " + e);
        } catch (CommitFailedException e) {
            throw new LoginException("User synchronization failed: " + e);
        }
    }

    @Nonnull
    protected SyncHandler getSyncHandler() throws SyncException {
        Object syncHandler = options.getConfigValue(PARAM_SYNC_HANDLER, null, null);
        if (syncHandler == null) {
            return new DefaultSyncHandler();
        } else if (syncHandler instanceof SyncHandler) {
            return (SyncHandler) syncHandler;
        } else {
            try {
                Object sh = Class.forName(syncHandler.toString()).newInstance();
                if (sh instanceof SyncHandler) {
                    return (SyncHandler) sh;
                } else {
                    throw new SyncException("Invalid SyncHandler configuration: " + sh);
                }
            } catch (Exception e) {
                throw new SyncException("Error while getting SyncHandler:", e);
            }
        }
    }

    @Override
    protected void clearState() {
        super.clearState();
        idp = null;
        externalUser = null;
    }
}