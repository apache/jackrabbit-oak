/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.security.user;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;

/**
 * The different authorizable types.
 */
public enum AuthorizableType {

    USER(UserManager.SEARCH_TYPE_USER),
    GROUP(UserManager.SEARCH_TYPE_GROUP),
    AUTHORIZABLE(UserManager.SEARCH_TYPE_AUTHORIZABLE);

    private final int userType;

    private AuthorizableType(int jcrUserType) {
        this.userType = jcrUserType;
    }

    @Nonnull
    public static AuthorizableType getType(int jcrUserType) {
        switch (jcrUserType) {
            case UserManager.SEARCH_TYPE_AUTHORIZABLE:
                return AUTHORIZABLE;
            case UserManager.SEARCH_TYPE_GROUP:
                return GROUP;
            case UserManager.SEARCH_TYPE_USER:
                return USER;
            default:
                throw new IllegalArgumentException("Invalid authorizable type "+jcrUserType);
        }
    }

    public boolean isType(Authorizable authorizable) {
        if (authorizable == null) {
            return false;
        }
        switch (userType) {
            case UserManager.SEARCH_TYPE_GROUP:
                return authorizable.isGroup();
            case UserManager.SEARCH_TYPE_USER:
                return !authorizable.isGroup();
            default:
                // TYPE_AUTHORIZABLE:
                return true;
        }
    }

    public Class<? extends Authorizable> getAuthorizableClass() {
        switch (userType) {
            case UserManager.SEARCH_TYPE_GROUP:
                return Group.class;
            case UserManager.SEARCH_TYPE_USER:
                return User.class;
            default:
                // TYPE_AUTHORIZABLE:
                return Authorizable.class;
        }
    }
}
