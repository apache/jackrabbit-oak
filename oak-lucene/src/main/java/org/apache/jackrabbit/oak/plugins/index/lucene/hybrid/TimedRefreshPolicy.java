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

public class TimedRefreshPolicy implements ReaderRefreshPolicy {
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final boolean syncIndexingMode;
    private final Clock clock;
    private final long refreshDelta;
    private volatile long lastRefreshTime;

    public TimedRefreshPolicy(boolean syncIndexingMode, Clock clock, TimeUnit unit, long refreshDelta) {
        this.syncIndexingMode = syncIndexingMode;
        this.clock = clock;
        this.refreshDelta = unit.toMillis(refreshDelta);
    }

    @Override
    public void refreshOnReadIfRequired(Runnable refreshCallback) {
        if (syncIndexingMode) {
            //As writer itself refreshes the index. No refresh done
            //on read
            return;
        }
        refreshIfRequired(refreshCallback);
    }

    @Override
    public void refreshOnWriteIfRequired(Runnable refreshCallback) {
        if (syncIndexingMode) {
            //For sync indexing mode we refresh the reader immediately
            //on the writer thread. So that any read call later sees upto date index

            //Another possibility is to refresh the readers upon first query post index update
            //but that would mean that if multiple queries get invoked simultaneously then
            //others would get blocked. So here we take hit on write side. If that proves to
            //be problematic query side refresh can be looked into
            if (dirty.get()) {
                refreshCallback.run();
                dirty.set(false);
            }
        } else {
            refreshIfRequired(refreshCallback);
        }
    }

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
