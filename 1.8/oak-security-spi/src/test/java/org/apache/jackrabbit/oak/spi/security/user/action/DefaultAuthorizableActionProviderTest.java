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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class DefaultAuthorizableActionProviderTest {

    private SecurityProvider getSecurityProvider() {
        AuthorizationConfiguration ac = Mockito.mock(AuthorizationConfiguration.class);
        when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);

        SecurityProvider sp = Mockito.mock(SecurityProvider.class);
        when(sp.getConfiguration(AuthorizationConfiguration.class)).thenReturn(ac);

        return sp;
    }

    @Test
    public void testNoConfig() {
        AuthorizableActionProvider[] providers = new AuthorizableActionProvider[] {
                new DefaultAuthorizableActionProvider(),
                new DefaultAuthorizableActionProvider(null)
        };

        for (AuthorizableActionProvider actionProvider : providers) {
            List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(getSecurityProvider());
            assertNotNull(actions);
            assertEquals(1, actions.size());
            assertTrue(actions.get(0) instanceof AccessControlAction);
        }
    }

    @Test
    public void testEmptyConfig() {
        AuthorizableActionProvider actionProvider = new DefaultAuthorizableActionProvider(ConfigurationParameters.EMPTY);

        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(getSecurityProvider());
        assertEquals(1, actions.size());
        assertTrue(actions.get(0) instanceof AccessControlAction);
    }

    @Test
    public void testNullActionConfig() {
        Map<String, String[]> m = new HashMap();
        m.put(DefaultAuthorizableActionProvider.ENABLED_ACTIONS, null);

        AuthorizableActionProvider actionProvider = new DefaultAuthorizableActionProvider(ConfigurationParameters.of(m));
        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(getSecurityProvider());
        assertEquals(1, actions.size());
        assertTrue(actions.get(0) instanceof AccessControlAction);
    }

    @Test
    public void testEmtpyActionConfig() {
        AuthorizableActionProvider actionProvider = new DefaultAuthorizableActionProvider(
                ConfigurationParameters.of(DefaultAuthorizableActionProvider.ENABLED_ACTIONS, new String[0]));
        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(getSecurityProvider());
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }

    @Test
    public void testNonExistingClassName() {
        String[] classNames = new String[] {
                "org.apache.jackrabbit.oak.spi.security.user.action.NonExistingAction",
                ""
        };
        AuthorizableActionProvider actionProvider = new DefaultAuthorizableActionProvider(
                ConfigurationParameters.of(DefaultAuthorizableActionProvider.ENABLED_ACTIONS, classNames));

        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(getSecurityProvider());
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }

    @Test
    public void testValidConfig() {
        String[] classNames = new String[] {
                PasswordChangeAction.class.getName(),
                PasswordValidationAction.class.getName()
        };
        AuthorizableActionProvider actionProvider = new DefaultAuthorizableActionProvider(
                ConfigurationParameters.of(DefaultAuthorizableActionProvider.ENABLED_ACTIONS, classNames));

        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(getSecurityProvider());
        assertNotNull(actions);
        assertEquals(2, actions.size());
        assertTrue(actions.get(0) instanceof PasswordChangeAction);
        assertTrue(actions.get(1) instanceof PasswordValidationAction);
    }
}