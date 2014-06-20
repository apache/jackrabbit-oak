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

import javax.jcr.Repository;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.security.auth.spi.LoginModule;

import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SyncMBeanImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx.SynchronizationMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Implements a LoginModuleFactory that creates {@link ExternalLoginModule}s and allows to configure login modules
 * via OSGi config.
 */
@Component(
        label = "Apache Jackrabbit Oak External Login Module",
        metatype = true,
        policy = ConfigurationPolicy.REQUIRE,
        configurationFactory = true
)
@Service
public class ExternalLoginModuleFactory implements LoginModuleFactory {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(ExternalLoginModuleFactory.class);

    @Property(
            intValue = 50,
            label = "JAAS Ranking",
            description = "Specifying the ranking (i.e. sort order) of this login module entry. The entries are sorted " +
                    "in a descending order (i.e. higher value ranked configurations come first)."
    )
    public static final String JAAS_RANKING = LoginModuleFactory.JAAS_RANKING;

    @Property(
            value = "SUFFICIENT",
            label = "JAAS Control Flag",
            description = "Property specifying whether or not a LoginModule is REQUIRED, REQUISITE, SUFFICIENT or " +
                    "OPTIONAL. Refer to the JAAS configuration documentation for more details around the meaning of " +
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

    @Property(
            label = "Identity Provider Name",
            description = "Name of the identity provider (for example: 'ldap')."
    )
    public static final String PARAM_IDP_NAME = ExternalLoginModule.PARAM_IDP_NAME;

    @Property(
            value = "default",
            label = "Sync Handler Name",
            description = "Name of the sync handler."
    )
    public static final String PARAM_SYNC_HANDLER_NAME = ExternalLoginModule.PARAM_SYNC_HANDLER_NAME;

    @Reference
    private SyncManager syncManager;

    @Reference
    private ExternalIdentityProviderManager idpManager;

    @Reference
    private Repository repository;

    /**
     * default configuration for the login modules
     */
    private ConfigurationParameters osgiConfig;

    /**
     * whiteboard registration handle of the manager mbean
     */
    private Registration mbeanRegistration;

    /**
     * Activates the LoginModuleFactory service
     * @param context the component context
     */
    @Activate
    private void activate(ComponentContext context) {
        //noinspection unchecked
        osgiConfig = ConfigurationParameters.of(context.getProperties());
        String idpName = osgiConfig.getConfigValue(PARAM_IDP_NAME, "");
        String sncName = osgiConfig.getConfigValue(PARAM_SYNC_HANDLER_NAME, "");

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        try {
            SyncMBeanImpl bean = new SyncMBeanImpl(repository, syncManager, sncName, idpManager, idpName);
            Hashtable<String, String> table = new Hashtable<String, String>();
            table.put("type", "UserManagement");
            table.put("name", "External Identity Synchronization Management");
            table.put("handler", ObjectName.quote(sncName));
            table.put("idp", ObjectName.quote(idpName));
            mbeanRegistration = whiteboard.register(SynchronizationMBean.class, bean, ImmutableMap.of(
                    "jmx.objectname",
                    new ObjectName("org.apache.jackrabbit.oak", table))
            );
        } catch (MalformedObjectNameException e) {
            log.error("Unable to register SynchronizationMBean.", e);
        }
    }

    @Deactivate
    private void deactivate() {
        if (mbeanRegistration != null) {
            mbeanRegistration.unregister();
            mbeanRegistration = null;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @return a new {@link ExternalLoginModule} instance.
     */
    @Override
    public LoginModule createLoginModule() {
        return new ExternalLoginModule(osgiConfig);
    }

}