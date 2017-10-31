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
package org.apache.jackrabbit.oak.spi.security.user.action;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link AuthorizableActionProvider} interface
 * that allows to config all actions provided by the OAK.
 */
@Component(
        service = AuthorizableActionProvider.class,
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider")
@Designate(ocd = DefaultAuthorizableActionProvider.Configuration.class)
public class DefaultAuthorizableActionProvider implements AuthorizableActionProvider {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak AuthorizableActionProvider")
    @interface Configuration {
        @AttributeDefinition(
                name = "Authorizable Actions",
                description = "The set of actions that is supported by this provider implementation.",
                cardinality = 4,
                options = {
                        @Option(label = "AccessControlAction", value = "org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction"),
                        @Option(label = "PasswordValidationAction", value = "org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction"),
                        @Option(label = "PasswordChangeAction", value = "org.apache.jackrabbit.oak.spi.security.user.action.PasswordChangeAction"),
                        @Option(label = "ClearMembershipAction", value = "org.apache.jackrabbit.oak.spi.security.user.action.ClearMembershipAction")
                })
        String[] enabledActions();

        @AttributeDefinition(
                name = "Configure AccessControlAction: User Privileges",
                description = "The name of the privileges that should be granted to a given user on it's home.")
        String[] userPrivilegeNames();

        @AttributeDefinition(
                name = "Configure AccessControlAction: Group Privileges",
                description = "The name of the privileges that should be granted to a given group on it's home.")
        String[] groupPrivilegeNames();

        @AttributeDefinition(
                name = "Configure PasswordValidationAction: Password Constraint",
                description = "A regular expression specifying the pattern that must be matched by a user's password.")
        String constraint();
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultAuthorizableActionProvider.class);

    private static final Map<String, Class<? extends AuthorizableAction>> SUPPORTED_ACTIONS = ImmutableMap.<String, Class<? extends AuthorizableAction>>of(
            AccessControlAction.class.getName(), AccessControlAction.class,
            PasswordValidationAction.class.getName(), PasswordValidationAction.class,
            PasswordChangeAction.class.getName(), PasswordChangeAction.class,
            ClearMembershipAction.class.getName(), ClearMembershipAction.class
    );

    private static final String[] DEFAULT_ACTIONS = new String[] {AccessControlAction.class.getName()};

    static final String ENABLED_ACTIONS = "enabledActions";

    private String[] enabledActions = DEFAULT_ACTIONS;

    private ConfigurationParameters config = ConfigurationParameters.EMPTY;

    @SuppressWarnings("UnusedDeclaration")
    public DefaultAuthorizableActionProvider() {}

    public DefaultAuthorizableActionProvider(ConfigurationParameters config) {
        if (config != null) {
            activate(config);
        }
    }

    //-----------------------------------------< AuthorizableActionProvider >---
    @Nonnull
    @Override
    public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
        List<AuthorizableAction> actions = Lists.newArrayListWithExpectedSize(enabledActions.length);
        for (String className : enabledActions) {
            try {
                Class<? extends AuthorizableAction> cl = SUPPORTED_ACTIONS.get(className);
                if (cl != null) {
                    AuthorizableAction action = cl.newInstance();
                    action.init(securityProvider, config);
                    actions.add(action);
                }
            } catch (Exception e) {
                log.debug("Unable to create authorizable action", e);
            }
        }
        return actions;
    }

    //----------------------------------------------------< SCR Integration >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        config = ConfigurationParameters.of(properties);
        enabledActions = config.getConfigValue(ENABLED_ACTIONS, DEFAULT_ACTIONS);
    }
}