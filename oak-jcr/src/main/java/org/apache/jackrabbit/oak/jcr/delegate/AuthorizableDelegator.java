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

package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;

/**
 * Utility class for creating the correct delegator instance (either
 * {@link GroupDelegator} or {@link UserDelegator} for a given
 * {@link Authorizable}.
 */
public final class AuthorizableDelegator {
    private AuthorizableDelegator() {}

    public static Authorizable wrap(SessionDelegate sessionDelegate, Authorizable authorizable) {
        if (authorizable.isGroup()) {
            return new GroupDelegator(sessionDelegate, (Group) authorizable);
        } else {
            return new UserDelegator(sessionDelegate, (User) authorizable);
        }
    }

}
