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
package org.apache.jackrabbit.oak.exercise.security.authentication;

import javax.security.auth.spi.LoginModule;

import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a LoginModuleFactory that creates {@link CustomLoginModule}s
 * and allows to configure login modules via OSGi config.
 */
@Component(
        label = "Custom Test Login Module (Oak Exercise Module)",
        metatype = true,
        policy = ConfigurationPolicy.REQUIRE,
        configurationFactory = true
)
@Service
public class CustomLoginModuleFactory implements LoginModuleFactory {

    private static final Logger log = LoggerFactory.getLogger(CustomLoginModuleFactory.class);

    @SuppressWarnings("UnusedDeclaration")
    @Property(
            intValue = 500,
            label = "JAAS Ranking",
            description = "Specifying the ranking (i.e. sort order) of this login module entry. The entries are sorted " +
                    "in a descending order (i.e. higher value ranked configurations come first)."
    )
    public static final String JAAS_RANKING = LoginModuleFactory.JAAS_RANKING;

    @SuppressWarnings("UnusedDeclaration")
    @Property(
            value = "OPTIONAL",
            label = "JAAS Control Flag",
            description = "Property specifying whether or not a LoginModule is REQUIRED, REQUISITE, SUFFICIENT or " +
                    "OPTIONAL. Refer to the JAAS configuration documentation for more details around the meaning of " +
                    "these flags."
    )
    public static final String JAAS_CONTROL_FLAG = LoginModuleFactory.JAAS_CONTROL_FLAG;

    @SuppressWarnings("UnusedDeclaration")
    @Property(
            label = "JAAS Realm",
            description = "The realm name (or application name) against which the LoginModule  is be registered. If no " +
                    "realm name is provided then LoginModule is registered with a default realm as configured in " +
                    "the Felix JAAS configuration."
    )
    public static final String JAAS_REALM_NAME = LoginModuleFactory.JAAS_REALM_NAME;

    // configuration parameters for the login module instances
    private ConfigurationParameters osgiConfig;

    /**
     * Activates the LoginModuleFactory service
     * @param componentContext the component context
     */
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(ComponentContext componentContext) {
        osgiConfig = ConfigurationParameters.of(componentContext.getProperties());
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate() {
        // nop
    }

    /**
     * {@inheritDoc}
     *
     * @return a new {@link ExternalLoginModule} instance.
     */
    @Override
    public LoginModule createLoginModule() {
        return new CustomLoginModule(osgiConfig);
    }
}