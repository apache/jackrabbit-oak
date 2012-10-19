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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;

import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * UserUtils... TODO
 */
public final class UserUtility implements UserConstants{

    @Nonnull
    public static String getAdminId(ConfigurationParameters parameters) {
        return parameters.getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
    }

    @Nonnull
    public static String getAnonymousId(ConfigurationParameters parameters) {
        return parameters.getConfigValue(PARAM_ANONYMOUS_ID, DEFAULT_ANONYMOUS_ID);
    }

    public static boolean isAuthorizableTree(Tree authorizableTree) {
        return isType(authorizableTree, AuthorizableType.AUTHORIZABLE);
    }

    public static boolean isType(Tree authorizableTree, AuthorizableType type) {
        // FIXME: check for node type according to the specified type constraint
        if (authorizableTree != null && authorizableTree.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            String ntName = authorizableTree.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue(STRING);
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
}