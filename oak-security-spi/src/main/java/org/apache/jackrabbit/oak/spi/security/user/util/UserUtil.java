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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableTypeException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkArgument;
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
        return getType(ntName);
    }

    @CheckForNull
    public static AuthorizableType getType(@CheckForNull String primaryTypeName) {
        if (primaryTypeName != null) {
            if (NT_REP_GROUP.equals(primaryTypeName)) {
                return AuthorizableType.GROUP;
            } else if (NT_REP_USER.equals(primaryTypeName)) {
                return AuthorizableType.USER;
            } else if (NT_REP_SYSTEM_USER.equals(primaryTypeName)) {
                return AuthorizableType.USER;
            }
        }
        return null;
    }

    public static boolean isSystemUser(@Nullable Tree authorizableTree) {
        return authorizableTree != null && NT_REP_SYSTEM_USER.equals(TreeUtil.getPrimaryTypeName(authorizableTree));
    }

    @CheckForNull
    public static String getAuthorizableRootPath(@Nonnull ConfigurationParameters parameters,
                                                 @Nullable AuthorizableType type) {
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
                    while (!Text.isDescendantOrEqual(path, groupRoot)) {
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

    /**
     * Retrieve the id from the given {@code authorizableTree}, which must have
     * been verified for being a valid authorizable of the specified type upfront.
     *
     * @param authorizableTree The authorizable tree which must be of the given {@code type}/
     * @param type The type of the authorizable tree.
     * @return The id retrieved from the specified {@code AuthorizableTree}.
     */
    @Nonnull
    public static String getAuthorizableId(@Nonnull Tree authorizableTree, @Nonnull AuthorizableType type) {
        checkArgument(UserUtil.isType(authorizableTree, type));
        PropertyState idProp = authorizableTree.getProperty(UserConstants.REP_AUTHORIZABLE_ID);
        if (idProp != null) {
            return idProp.getValue(STRING);
        } else {
            return Text.unescapeIllegalJcrChars(authorizableTree.getName());
        }
    }

    @CheckForNull
    public static <T extends Authorizable> T castAuthorizable(@Nullable Authorizable authorizable, Class<T> authorizableClass) throws AuthorizableTypeException {
        if (authorizable == null) {
            return null;
        }

        if (authorizableClass != null && authorizableClass.isInstance(authorizable)) {
            return authorizableClass.cast(authorizable);
        } else {
            throw new AuthorizableTypeException("Invalid authorizable type '" + ((authorizableClass == null) ? "null" : authorizableClass) + '\'');
        }
    }

    /**
     * Return the configured {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
     * for the given {@code config}. The default behavior in case
     * {@link org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter#PARAM_IMPORT_BEHAVIOR}
     * is not contained in the {@code config} object is
     * {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior#IGNORE}
     *
     * @param config The configuration parameters.
     * @return The import behavior as defined by {@link org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter#PARAM_IMPORT_BEHAVIOR}
     * or {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior#IGNORE} if this
     * config parameter is missing.
     */
    public static int getImportBehavior(@Nonnull ConfigurationParameters config) {
        String importBehaviorStr = config.getConfigValue(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_IGNORE);
        return ImportBehavior.valueFromString(importBehaviorStr);
    }
}