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
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import java.security.Principal;

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
     *                            {@code PARAM_IMPERSONATOR_GROUPS_ID}.
     * @return A mocked configuration object with the specified impersonator group ID.
     */
    @NotNull static ConfigurationParameters getMockedConfigs(@Nullable ConfigurationParameters configs, @NotNull String impersonatorGroupId) {
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

    @NotNull static ConfigurationParameters getMockedConfigs(@NotNull String impersonatorGroupId) {
        return getMockedConfigs(null, impersonatorGroupId);
    }

    /**

     * Attaches to a given {@link UserImpl} a {@link ConfigurationParameters}. The config
     * will have the impersonator principals key set to a given group name.

     * @param impersonatorGroupName the name of the impersonator group
     * @param user the existing user object to attach a config to
     * @return a mock user object with mocked configuration parameters
     */
    @NotNull static UserImpl getUserWithMockedConfigs(@NotNull String impersonatorGroupName, @NotNull UserImpl user) {
        ConfigurationParameters configMock = getMockedConfigs(impersonatorGroupName);

        UserManagerImpl userManagerMock = spy(user.getUserManager());
        when(userManagerMock.getConfig()).thenReturn(configMock);

        UserImpl userMock = spy(user);
        when(userMock.getUserManager()).thenReturn(userManagerMock);

        return userMock;
    }

    @NotNull static User getUserWithPrincipal(@NotNull Principal principal) throws RepositoryException {
        User user = mock(User.class);
        when(user.getPrincipal()).thenReturn(principal);

        return user;
    }

    @NotNull static PrincipalManager getMockedPrincipalManager(@NotNull String impersonatorName, @NotNull Principal principal) {
        PrincipalManager principalManagerMocked = mock(PrincipalManager.class);
        when(principalManagerMocked.getPrincipal(impersonatorName)).thenReturn(principal);

        return principalManagerMocked;
    }
}
