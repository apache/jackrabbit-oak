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

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExternalIdentityMonitorImpl implements ExternalIdentityMonitor {

    private final TimerStats syncTimer;
    private final MeterStats syncRetries;
    private final TimerStats syncIdTimer;
    private final MeterStats syncFailed;

    public ExternalIdentityMonitorImpl(@NotNull StatisticsProvider statisticsProvider) {
        syncTimer = statisticsProvider.getTimer("security.authentication.external.sync_external_identity.timer", StatsOptions.METRICS_ONLY);
        syncRetries = statisticsProvider.getMeter("security.authentication.external.sync_external_identity.retries", StatsOptions.DEFAULT);
        syncIdTimer = statisticsProvider.getTimer("security.authentication.external.sync_id.timer", StatsOptions.METRICS_ONLY);
        syncFailed = statisticsProvider.getMeter("security.authentication.external.sync.failed", StatsOptions.DEFAULT);
    }

    @Override
    public void doneSyncExternalIdentity(long timeTakenNanos, @NotNull SyncResult result, int retryCount) {
        // note: currently the sync-result is ignored. further improvements might choose to exclude certain
        // results or refine the stats gather by result type.
        syncTimer.update(timeTakenNanos, NANOSECONDS);
        if (retryCount > 0){
            syncRetries.mark(retryCount);
        }
    }

    @Override
    public void doneSyncId(long timeTakenNanos, @NotNull SyncResult result) {
        // note: currently the sync-result is ignored. further improvements might choose to exclude certain
        // results or refine the stats gather by result type.
        syncIdTimer.update(timeTakenNanos, NANOSECONDS);
    }

    @Override
    public void syncFailed(@NotNull SyncException syncException) {
        syncFailed.mark();
    }
}
