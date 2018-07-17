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

import java.util.Hashtable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.security.auth.spi.LoginModule;

import com.google.common.collect.ImmutableMap;
import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SyncMBeanImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SynchronizationMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a LoginModuleFactory that creates {@link ExternalLoginModule}s and allows to configure login modules
 * via OSGi config.
 */
@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE
)
@Designate(ocd=ExternalLoginModuleFactory.LoginModuleConfig.class, factory=true)
public class ExternalLoginModuleFactory implements LoginModuleFactory, SyncHandlerMapping {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak External Login Module"
            )
    @interface LoginModuleConfig {
        @AttributeDefinition(
                name = "JAAS Ranking",
                description = "Specifying the ranking (i.e. sort order) of this login module entry. The entries are sorted " +
                        "in a descending order (i.e. higher value ranked configurations come first)."
                )
        String jaasRanking() default "50";

        @AttributeDefinition(
                name = "JAAS Control Flag",
                description = "Property specifying whether or not a LoginModule is REQUIRED, REQUISITE, SUFFICIENT or " +
                        "OPTIONAL. Refer to the JAAS configuration documentation for more details around the meaning of " +
                        "these flags."
                )
        String jaasControlFlag() default "SUFFICIENT";

        @AttributeDefinition(
                name = "JAAS Realm",
                description = "The realm name (or application name) against which the LoginModule  is be registered. If no " +
                        "realm name is provided then LoginModule is registered with a default realm as configured in " +
                        "the Felix JAAS configuration."
                )
        String jaasRealmName();

        @AttributeDefinition(
                name = "Identity Provider Name",
                description = "Name of the identity provider (for example: 'ldap')."
                )
        String idpName();

        @AttributeDefinition(
                name = "Sync Handler Name",
                description = "Name of the sync handler."
                )
        String syncHandlerName() default "default";
    }

    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModuleFactory.class);

    @Reference(cardinality=ReferenceCardinality.OPTIONAL, policyOption=ReferencePolicyOption.GREEDY)
    volatile private SecurityProvider securityProvider;

    @Reference(cardinality=ReferenceCardinality.OPTIONAL, policyOption=ReferencePolicyOption.GREEDY)
    volatile private ContentRepository contentRepository;

    @Reference
    private SyncManager syncManager;

    @Reference
    private ExternalIdentityProviderManager idpManager;

    /**
     * default configuration for the login modules
     */
    private ConfigurationParameters osgiConfig = ConfigurationParameters.EMPTY;

    private BundleContext bundleContext;

    /**
     * whiteboard registration handle of the manager mbean
     */
    private Registration mbeanRegistration;

    //----------------------------------------------------< SCR integration >---
    /**
     * Activates the LoginModuleFactory service
     * @param context the component context
     */
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(ComponentContext context) {
        //noinspection unchecked
        osgiConfig = ConfigurationParameters.of(context.getProperties());
        bundleContext = context.getBundleContext();

        mayRegisterSyncMBean();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        unregisterSyncMBean();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void bindContentRepository(ContentRepository contentRepository) {
        this.contentRepository = contentRepository;
        mayRegisterSyncMBean();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void unbindContentRepository(ContentRepository contentRepository) {
        this.contentRepository = null;
        unregisterSyncMBean();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void bindSecurityProvider(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
        mayRegisterSyncMBean();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void unbindSecurityProvider(SecurityProvider securityProvider)  {
        this.securityProvider = null;
        unregisterSyncMBean();
    }

    private void mayRegisterSyncMBean() {
        log.debug("Trying to register SynchronizationMBean");

        if (mbeanRegistration != null) {
            log.debug("SynchronizationMBean already registered");
            return;
        }
        if (bundleContext == null) {
            log.debug("Cannot register SynchronizationMBean; not yet activated.");
            return;
        }
        if (contentRepository == null || securityProvider == null) {
            log.debug("Cannot register SynchronizationMBean; waiting for references to ContentRepository|SecurityProvider.");
            return;
        }

        Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        try {
            log.debug("Registering SynchronizationMBean");

            String idpName = osgiConfig.getConfigValue(PARAM_IDP_NAME, "");
            String sncName = osgiConfig.getConfigValue(PARAM_SYNC_HANDLER_NAME, "");

            SyncMBeanImpl bean = new SyncMBeanImpl(contentRepository, securityProvider, syncManager, sncName, idpManager, idpName);
            Hashtable<String, String> table = new Hashtable();
            table.put("type", "UserManagement");
            table.put("name", "External Identity Synchronization Management");
            table.put("handler", ObjectName.quote(sncName));
            table.put("idp", ObjectName.quote(idpName));
            mbeanRegistration = whiteboard.register(SynchronizationMBean.class, bean, ImmutableMap.of(
                            "jmx.objectname",
                            new ObjectName("org.apache.jackrabbit.oak", table))
            );
            log.debug("Registration of SynchronizationMBean completed");
        } catch (MalformedObjectNameException e) {
            log.error("Unable to register SynchronizationMBean", e);
        }
    }

    private void unregisterSyncMBean() {
        if (mbeanRegistration != null) {
            log.debug("Unregistering SynchronizationMBean");

            mbeanRegistration.unregister();
            mbeanRegistration = null;
            log.debug("Unregister SynchronizationMBean: completed");
        } else {
            log.debug("Unable to unregister SynchronizationMBean; missing registration.");
        }
    }

    //-------------------------------------------------< LoginModuleFactory >---
    /**
     * {@inheritDoc}
     *
     * @return a new {@link ExternalLoginModule} instance.
     */
    @Override
    public LoginModule createLoginModule() {
        ExternalLoginModule lm = new ExternalLoginModule(osgiConfig);
        lm.setIdpManager(idpManager);
        lm.setSyncManager(syncManager);
        return lm;
    }
}