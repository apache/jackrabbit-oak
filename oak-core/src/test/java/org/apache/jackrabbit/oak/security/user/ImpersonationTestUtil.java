package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;

import javax.jcr.RepositoryException;
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
                .when(configMock).getConfigValue(eq(UserConstants.PARAM_IMPERSONATOR_GROUPS_ID), any());

        return configMock;
    }

    static ConfigurationParameters getMockedConfigs(String impersonatorGroupId) {
        return getMockedConfigs(null, impersonatorGroupId);
    }

    static UserImpl getUserWithMockedConfigs(String impersonatorGroupId, UserImpl user) {
        ConfigurationParameters configMock = getMockedConfigs(impersonatorGroupId);

        UserManagerImpl userManagerMock = spy(user.getUserManager());
        when(userManagerMock.getConfig()).thenReturn(configMock);

        UserImpl userMock = spy(user);
        when(userMock.getUserManager()).thenReturn(userManagerMock);

        return userMock;
    }

    /**

     * Generates an {@link Authorizable} mocked object which is member of a specified group.

     * @param authorizable The authorizable object to be spied. If null, a new mock object will be created.
     * @param groupId The group ID for the authorizable object to be member of.
     * @return An authorizable object which is member to the specified group.
     * @throws RepositoryException If an error occurs while mocking the authorizable object.
     */
    static Authorizable getAuthorizablewithGroup(Authorizable authorizable, String groupId) throws RepositoryException {
        Authorizable authorizableMock;
        if (authorizable != null) {
            authorizableMock = spy(authorizable);
        } else {
            authorizableMock = mock(Authorizable.class);
        }
        Group group = mock(Group.class);
        when(group.getID()).thenReturn(groupId);
        when(authorizableMock.memberOf()).thenReturn(new Iterator<>() {
            private boolean hasNext = true;
            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Group next() {
                hasNext = false;
                return group;
            }
        });

        return authorizableMock;
    }

    static Authorizable getAuthorizablewithGroup(String groupId) throws RepositoryException {
        return getAuthorizablewithGroup(null, groupId);
    }
}
