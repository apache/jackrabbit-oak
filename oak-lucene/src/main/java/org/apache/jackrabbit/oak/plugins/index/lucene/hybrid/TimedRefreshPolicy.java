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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.stats.Clock;

public class TimedRefreshPolicy implements ReaderRefreshPolicy, IndexUpdateListener {
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final Clock clock;
    private final long refreshDelta;
    private volatile long lastRefreshTime;

    public TimedRefreshPolicy(Clock clock, TimeUnit unit, long refreshDelta) {
        this.clock = clock;
        this.refreshDelta = unit.toMillis(refreshDelta);
    }

    @Override
    public void refreshOnReadIfRequired(Runnable refreshCallback) {
        refreshIfRequired(refreshCallback);
    }

    @Override
    public void refreshOnWriteIfRequired(Runnable refreshCallback) {
        refreshIfRequired(refreshCallback);
    }

    @Override
    public void updated() {
        dirty.set(true);
    }

    private void refreshIfRequired(Runnable refreshCallback) {
        if (dirty.get()){
            long currentTime = clock.getTime();
            if (currentTime - lastRefreshTime > refreshDelta
                    && dirty.compareAndSet(true, false)){
                lastRefreshTime = currentTime;
                refreshCallback.run();
            }
        }
    }
}
