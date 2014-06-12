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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link AuthorizableActionProvider} interface
 * that allows to config all actions provided by the OAK.
 */
@Component(metatype = true, label = "Apache Jackrabbit Oak AuthorizableActionProvider")
@Service(AuthorizableActionProvider.class)
@Properties({
        @Property(name = DefaultAuthorizableActionProvider.ENABLED_ACTIONS,
                label = "Authorizable Actions",
                description = "The set of actions that is supported by this provider implementation.",
                cardinality = 4,
                options = {
                        @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction", value = "AccessControlAction"),
                        @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction", value = "PasswordValidationAction"),
                        @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.PasswordChangeAction", value = "PasswordChangeAction"),
                        @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.ClearMembershipAction", value = "ClearMembershipAction")
                }),
        @Property(name = AccessControlAction.USER_PRIVILEGE_NAMES,
                label = "Configure AccessControlAction: User Privileges",
                description = "The name of the privileges that should be granted to a given user on it's home.",
                cardinality = Integer.MAX_VALUE),
        @Property(name = AccessControlAction.GROUP_PRIVILEGE_NAMES,
                label = "Configure AccessControlAction: Group Privileges",
                description = "The name of the privileges that should be granted to a given group on it's home.",
                cardinality = Integer.MAX_VALUE),
        @Property(name = PasswordValidationAction.CONSTRAINT,
                label = "Configure PasswordValidationAction: Password Constraint",
                description = "A regular expression specifying the pattern that must be matched by a user's password.")
})
public class DefaultAuthorizableActionProvider implements AuthorizableActionProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultAuthorizableActionProvider.class);

    static final String ENABLED_ACTIONS = "enabledActions";

    private String[] enabledActions = new String[] {AccessControlAction.class.getName()};

    private ConfigurationParameters config = ConfigurationParameters.EMPTY;

    public DefaultAuthorizableActionProvider() {}

    public DefaultAuthorizableActionProvider(ConfigurationParameters config) {
        this.config = config;
    }

    //-----------------------------------------< AuthorizableActionProvider >---
    @Override
    public List<? extends AuthorizableAction> getAuthorizableActions(SecurityProvider securityProvider) {
        List<AuthorizableAction> actions = Lists.newArrayList();
        for (String className : enabledActions) {
            try {
                Object o = Class.forName(className).newInstance();
                if (o instanceof AuthorizableAction) {
                    actions.add((AuthorizableAction) o);
                }
            } catch (Exception e) {
                log.debug("Unable to create authorizable action", e);
            }
        }

        // merge configurations that may contain action configuration information
        for (AuthorizableAction action : actions) {
            action.init(securityProvider, config);
        }
        return actions;
    }

    //----------------------------------------------------< SCR Integration >---
    @Activate
    private void activate(Map<String, Object> properties) {
        config = ConfigurationParameters.of(properties);
        enabledActions = PropertiesUtil.toStringArray(properties.get(ENABLED_ACTIONS), new String[0]);
    }
}