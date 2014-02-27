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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Default implementation of the {@link AuthorizableActionProvider} interface
 * that allows to config all actions provided by the OAK.
 */
@Component()
@Service(AuthorizableActionProvider.class)
public class DefaultAuthorizableActionProvider implements AuthorizableActionProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultAuthorizableActionProvider.class);

    private static final String ENABLED_ACTIONS = "enabledActions";

    @Property(name = ENABLED_ACTIONS,
            options = {
                    @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction", value = "AccessControlAction"),
                    @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction", value = "PasswordValidationAction"),
                    @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.PasswordChangeAction", value = "PasswordChangeAction"),
                    @PropertyOption(name = "org.apache.jackrabbit.oak.spi.security.user.action.ClearMembershipAction", value = "ClearMembershipAction")
            })
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
    }
}