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
package org.apache.jackrabbit.oak.spi.security.user.util;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * Utility methods for user management.
 */
public final class UserUtil implements UserConstants {

    private UserUtil() {
    }

    public static boolean isAdmin(@Nonnull ConfigurationParameters parameters, @Nonnull String userId) {
        return getAdminId(parameters).equals(userId);
    }

    @Nonnull
    public static String getAdminId(@Nonnull ConfigurationParameters parameters) {
        return parameters.getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
    }

    @Nonnull
    public static String getAnonymousId(@Nonnull ConfigurationParameters parameters) {
        return parameters.getConfigValue(PARAM_ANONYMOUS_ID, DEFAULT_ANONYMOUS_ID);
    }

    public static boolean isType(@Nullable Tree authorizableTree, @Nonnull AuthorizableType type) {
        if (authorizableTree != null) {
            String ntName = TreeUtil.getPrimaryTypeName(authorizableTree);
            switch (type) {
                case GROUP:
                    return NT_REP_GROUP.equals(ntName);
                case USER:
                    return NT_REP_USER.equals(ntName) || NT_REP_SYSTEM_USER.equals(ntName);
                default:
                    return NT_REP_USER.equals(ntName) || NT_REP_GROUP.equals(ntName) || NT_REP_SYSTEM_USER.equals(ntName);
            }
        }
        return false;
    }

    @CheckForNull
    public static AuthorizableType getType(@Nonnull Tree authorizableNode) {
        String ntName = TreeUtil.getPrimaryTypeName(authorizableNode);
        if (ntName != null) {
            if (NT_REP_GROUP.equals(ntName)) {
                return AuthorizableType.GROUP;
            } else if (NT_REP_USER.equals(ntName)) {
                return AuthorizableType.USER;
            } else if (NT_REP_SYSTEM_USER.equals(ntName)) {
                return AuthorizableType.USER;
            }
        }
        return null;
    }

    public static boolean isSystemUser(@Nullable Tree authorizableTree) {
        return authorizableTree != null && NT_REP_SYSTEM_USER.equals(TreeUtil.getPrimaryTypeName(authorizableTree));
    }

    @CheckForNull
    public static String getAuthorizableRootPath(ConfigurationParameters parameters, AuthorizableType type) {
        String path = null;
        if (type != null) {
            switch (type) {
                case USER:
                    path = parameters.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
                    break;
                case GROUP:
                    path = parameters.getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH);
                    break;
                default:
                    path = parameters.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
                    String groupRoot = parameters.getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH);
                    while (!Text.isDescendant(path, groupRoot)) {
                        path = Text.getRelativeParent(path, 1);
                    }
            }
        }
        return path;
    }

    @CheckForNull
    public static String getAuthorizableId(@Nonnull Tree authorizableTree) {
        checkNotNull(authorizableTree);
        if (UserUtil.isType(authorizableTree, AuthorizableType.AUTHORIZABLE)) {
            PropertyState idProp = authorizableTree.getProperty(UserConstants.REP_AUTHORIZABLE_ID);
            if (idProp != null) {
                return idProp.getValue(STRING);
            } else {
                return Text.unescapeIllegalJcrChars(authorizableTree.getName());
            }
        }
        return null;
    }
}