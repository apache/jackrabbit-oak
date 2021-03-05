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

    default void doneGetMembers(long timeTakenNanos, boolean declaredOnly) {}

    default void doneMemberOf(long timeTakenNanos, boolean declaredOnly) {}

    default void doneUpdateMembers(long timeTakenNanos, long totalProcessed, long failed, boolean isRemove) {}

    default @NotNull Class<UserMonitor> getMonitorClass() {
        return UserMonitor.class;
    }

    @Override
    default @NotNull Map<Object, Object> getMonitorProperties() {
        return Collections.emptyMap();
    }

}