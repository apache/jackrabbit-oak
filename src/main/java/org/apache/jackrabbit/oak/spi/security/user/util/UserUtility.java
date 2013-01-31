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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

/**
 * UserUtils... TODO
 */
public final class UserUtility implements UserConstants {

    private UserUtility() {
    }

    @Nonnull
    public static String getAdminId(ConfigurationParameters parameters) {
        return parameters.getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
    }

    @Nonnull
    public static String getAnonymousId(ConfigurationParameters parameters) {
        return parameters.getConfigValue(PARAM_ANONYMOUS_ID, DEFAULT_ANONYMOUS_ID);
    }

    public static boolean isType(Tree authorizableTree, AuthorizableType type) {
        // FIXME: check for node type according to the specified type constraint
        if (authorizableTree != null) {
            String ntName = TreeUtil.getPrimaryTypeName(authorizableTree);
            switch (type) {
                case GROUP:
                    return NT_REP_GROUP.equals(ntName);
                case USER:
                    return NT_REP_USER.equals(ntName);
                default:
                    return NT_REP_USER.equals(ntName) || NT_REP_GROUP.equals(ntName);
            }
        }
        return false;
    }

    @CheckForNull
    public static AuthorizableType getType(@Nonnull Tree authorizableNode) {
        String ntName = TreeUtil.getPrimaryTypeName(authorizableNode);
        if (ntName != null) {
            if (UserConstants.NT_REP_GROUP.equals(ntName)) {
                return AuthorizableType.GROUP;
            } else if (UserConstants.NT_REP_USER.equals(ntName)) {
                return AuthorizableType.USER;
            }
        }
        return null;
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
}