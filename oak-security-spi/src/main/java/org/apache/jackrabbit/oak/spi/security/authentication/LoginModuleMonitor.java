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
package org.apache.jackrabbit.oak.spi.security.authentication;

import org.apache.jackrabbit.oak.stats.Monitor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

import javax.jcr.Credentials;
import javax.security.auth.login.LoginException;
import java.util.Collections;
import java.util.Map;

@ProviderType
public interface LoginModuleMonitor extends Monitor<LoginModuleMonitor> {

    LoginModuleMonitor NOOP = () -> { };

    /**
     * Event to be called in the case there is an error in the login chain. This
     * is not covering failed logins, but actual operational errors that
     * probably need to be investigated. Any triggered event should have a
     * corresponding error logged to make this investigation possible.
     * 
     * @see #loginFailed(LoginException, Credentials)
     */
    void loginError();

    /**
     * Marks a failed login attempt for the given {@link Credentials} that resulted in the given {@link javax.jcr.LoginException}.
     * 
     * @param loginException The {@link LoginException} raised by the failed login attempt.
     * @param credentials The credentials used for login.
     */
    default void loginFailed(@NotNull LoginException loginException, @Nullable Credentials credentials) {}

    /**
     * Record the time taken to collect the given number of principals during the commit phase of a given {@code LoginModule}.
     *
     * @param timeTakenNanos The time in nanoseconds
     * @param numberOfPrincipals The number of principals that were collected.
     */
    default void principalsCollected(long timeTakenNanos, int numberOfPrincipals) {}

    @Override
    default @NotNull Class<LoginModuleMonitor> getMonitorClass() {
        return LoginModuleMonitor.class;
    }

    @Override
    default @NotNull Map<Object, Object> getMonitorProperties() {
        return Collections.emptyMap();
    }
}
