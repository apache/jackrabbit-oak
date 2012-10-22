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
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import javax.security.auth.callback.Callback;

import org.apache.jackrabbit.api.security.user.UserManager;

/**
 * Callback implementation used to pass a {@link UserManager} to the
 * login module.
 */
public class UserManagerCallback implements Callback {

    private UserManager userManager;

    /**
     * Returns the user provider as set using
     * {@link #setUserManager(org.apache.jackrabbit.api.security.user.UserManager)}
     * or {@code null}.
     *
     * @return an instance of {@code UserManager} or {@code null} if no
     * provider has been set before.
     */
    public UserManager getUserManager() {
        return userManager;
    }

    /**
     * Sets the {@code UserManager} that is being used during the
     * authentication process.
     *
     * @param userManager The user provider to use during the
     * authentication process.
     */
    public void setUserManager(UserManager userManager) {
        this.userManager = userManager;
    }
}