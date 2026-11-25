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
package org.apache.jackrabbit.oak.security.user.monitor;

import org.apache.jackrabbit.oak.stats.Monitor;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

import java.util.Collections;
import java.util.Map;

@ProviderType
public interface UserMonitor extends Monitor<UserMonitor> {

    UserMonitor NOOP = new UserMonitor() {};

    /**
     * Called to record the time it takes to compute the members of a group.
     *
     * @param timeTakenNanos The time in nanoseconds
     * @param declaredOnly {@code true} if only declared members were retrieved; {@code false} if declared and
     * inherited members were retrieved.
     */
    default void doneGetMembers(long timeTakenNanos, boolean declaredOnly) {}

    /**
     * Called to record the time it takes to compute the group membership of a given user or group.
     *
     * @param timeTakenNanos The time in nanoseconds
     * @param declaredOnly {@code true} if only declared membership was retrieved; {@code false} otherwise.
     */
    default void doneMemberOf(long timeTakenNanos, boolean declaredOnly) {}

    /**
     * Called to record changes to members of a group.
     *
     * @param timeTakenNanos The time in nanoseconds
     * @param totalProcessed The total number of processed members
     * @param failed The number of unsuccessful updates (either members added or removed)
     * @param isRemove {@code true} if members were being removed; {@code false} if added.
     */
    default void doneUpdateMembers(long timeTakenNanos, long totalProcessed, long failed, boolean isRemove) {}

    /**
     * Called to record a password based login attempt of an existing user, including attempts of disabled users
     * (which always fail).
     *
     * @param success {@code true} if the credentials were accepted by the user authentication; {@code false}
     *                otherwise. This does not reflect the outcome of the overall login (e.g. other login modules).
     * @param isAdmin {@code true} if the user is the admin user as identified by the configured admin id (not any
     *                principal with administrative privileges).
     */
    default void login(boolean success, boolean isAdmin) {}

    /**
     * Called to record an attempt to impersonate an existing user, including attempts of disabled users (which
     * always fail).
     *
     * @param success {@code true} if the impersonation was accepted by the user authentication; {@code false}
     *                otherwise. This does not reflect the outcome of the overall login (e.g. other login modules).
     * @param isAdmin {@code true} if the impersonated user is the admin user as identified by the configured admin
     *                id (not any principal with administrative privileges).
     */
    default void impersonated(boolean success, boolean isAdmin) {}

    /**
     * Called to record a change of the password of an existing user observed during commit validation. This is
     * best-effort monitoring: the commit may still fail afterwards, and retried commits may be recorded more than once.
     *
     * @param added {@code true} if the user did not have a password before; {@code false} if an existing
     *              password was changed.
     * @param isAdmin {@code true} if the user is the admin user as identified by the configured admin id (not any
     *                principal with administrative privileges).
     */
    default void passwordChanged(boolean added, boolean isAdmin) {}

    /**
     * Called to record changes to the set of principals allowed to impersonate an existing user observed during
     * commit validation. This is best-effort monitoring: the commit may still fail afterwards, and retried commits
     * may be recorded more than once.
     *
     * @param granted The number of principals granted impersonation.
     * @param revoked The number of principals revoked impersonation.
     * @param isAdmin {@code true} if the user is the admin user as identified by the configured admin id (not any
     *                principal with administrative privileges).
     */
    default void impersonatorsChanged(long granted, long revoked, boolean isAdmin) {}

    default @NotNull Class<UserMonitor> getMonitorClass() {
        return UserMonitor.class;
    }

    @Override
    default @NotNull Map<Object, Object> getMonitorProperties() {
        return Collections.emptyMap();
    }

}