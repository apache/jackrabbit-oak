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

import org.apache.felix.jaas.LoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import javax.security.auth.spi.LoginModule;

/**
 * Implements a LoginModuleFactory that creates {@link CustomLoginModule}s
 * and allows to configure login modules via OSGi config.
 */
@Component(service = LoginModuleFactory.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = CustomLoginModuleFactory.Configuration.class, factory=true)
public class CustomLoginModuleFactory implements LoginModuleFactory {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak Custom Test Login Module (Oak Exercises)")
    @interface Configuration {

        @AttributeDefinition(
                name = "JAAS Ranking",
                description = "Specifying the ranking (i.e. sort order) of this login module entry. The entries are sorted " +
                        "in a descending order (i.e. higher value ranked configurations come first)."
        )
        int jaas_ranking() default 500;

        @AttributeDefinition(
                name = "JAAS Control Flag",
                description = "Property specifying whether or not a LoginModule is REQUIRED, REQUISITE, SUFFICIENT or " +
                        "OPTIONAL. Refer to the JAAS configuration documentation for more details around the meaning of " +
                        "these flags."
        )
        String jaas_controlFlag() default "OPTIONAL";

        @AttributeDefinition(
                name = "JAAS Realm",
                description = "The realm name (or application name) against which the LoginModule  is be registered. If no " +
                        "realm name is provided then LoginModule is registered with a default realm as configured in " +
                        "the Felix JAAS configuration."
        )
        String jaas_realmName();
    }

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
     * @return a new {@link CustomLoginModule} instance.
     */
    @Override
    public LoginModule createLoginModule() {
        return new CustomLoginModule(osgiConfig);
    }
}