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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

public interface ExternalIdentityMonitor extends Monitor<ExternalIdentityMonitor> {

    ExternalIdentityMonitor NOOP = new ExternalIdentityMonitor() {};

    /**
     * Mark the successful completion of {@link org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext#sync(ExternalIdentity)}.
     *
     * @param timeTakenNanos Time in nanoseconds spend to complete {@link org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext#sync(ExternalIdentity)}
     * @param result The result of the sync operation.
     * @param retryCount The number of retries needed to complete the sync.
     */
    default void doneSyncExternalIdentity(long timeTakenNanos, @NotNull SyncResult result, int retryCount) {};

    /**
     * Mark the successful completion of {@link org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext#sync(String)}.
     *
     * @param timeTakenNanos Time in nanoseconds spend to complete {@link org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext#sync(String)}
     * @param result The result of the sync operation.
     */
    default void doneSyncId(long timeTakenNanos, @NotNull SyncResult result) {};

    /**
     * Mark the failure of a sync operation that resulted in the given {@link SyncException}.
     *
     * @param syncException The sync exception.
     */
    default void syncFailed(@NotNull SyncException syncException) {};

    @Override
    @NotNull
    default Class<ExternalIdentityMonitor> getMonitorClass() {
        return ExternalIdentityMonitor.class;
    }

    @Override
    @NotNull
    default Map<Object, Object> getMonitorProperties() {
        return Collections.emptyMap();
    }
}