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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Iterator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * A utility interface that generates mocked entities for Impersonation tests
 */
public interface ImpersonationTestUtil {
    /**
     * Generates a mocked {@link ConfigurationParameters} object with a specified impersonator group ID.

     * @param configs The configuration object to be spied. If null, a new mock object will be created.
     * @param impersonatorGroupId The impersonator group ID to be returned when searching in the config for
     *                            <code>PARAM_IMPERSONATOR_GROUPS_ID</code>.
     * @return A mocked configuration object with the specified impersonator group ID.
     */
    static ConfigurationParameters getMockedConfigs(ConfigurationParameters configs, String impersonatorGroupId) {
        ConfigurationParameters configMock;
        if (configs != null) {
            configMock = spy(configs);
        } else {
            configMock = mock(ConfigurationParameters.class);
        }
        doReturn(new String[]{impersonatorGroupId})
                .when(configMock).getConfigValue(eq(UserConstants.PARAM_IMPERSONATOR_PRINCIPAL_NAMES), any());

        return configMock;
    }

    static ConfigurationParameters getMockedConfigs(String impersonatorGroupId) {
        return getMockedConfigs(null, impersonatorGroupId);
    }

    /**

     * Attaches to a given {@link UserImpl} a {@link ConfigurationParameters}. The config
     * will have the impersonator principals key set to a given group name.

     * @param impersonatorGroupName the name of the impersonator group
     * @param user the existing user object to attach a config to
     * @return a mock user object with mocked configuration parameters
     */
    static UserImpl getUserWithMockedConfigs(String impersonatorGroupName, UserImpl user) {
        ConfigurationParameters configMock = getMockedConfigs(impersonatorGroupName);

        UserManagerImpl userManagerMock = spy(user.getUserManager());
        when(userManagerMock.getConfig()).thenReturn(configMock);

        UserImpl userMock = spy(user);
        when(userMock.getUserManager()).thenReturn(userManagerMock);

        return userMock;
    }

    static Authorizable getAuthorizableWithPrincipal(Principal principal) throws RepositoryException {
        Authorizable authorizable = mock(Authorizable.class);
        when(authorizable.getPrincipal()).thenReturn(principal);

        return authorizable;
    }

    static PrincipalManager getMockedPrincipalManager(String impersonatorName, Principal principal) {
        PrincipalManager principalManagerMocked = mock(PrincipalManager.class);
        when(principalManagerMocked.getPrincipal(impersonatorName)).thenReturn(principal);

        return principalManagerMocked;
    }
}
