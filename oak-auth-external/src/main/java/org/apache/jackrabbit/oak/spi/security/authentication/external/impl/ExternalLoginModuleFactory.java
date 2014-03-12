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

import java.util.Map;

import javax.security.auth.spi.LoginModule;

import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;

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

    /**
     * default configuration for the login modules
     */
    private ConfigurationParameters osgiConfig;

    /**
     * Activates the LoginModuleFactory service
     * @param properties the OSGi config
     */
    @Activate
    protected void activate(Map<String, Object> properties) {
        osgiConfig = ConfigurationParameters.of(properties);
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