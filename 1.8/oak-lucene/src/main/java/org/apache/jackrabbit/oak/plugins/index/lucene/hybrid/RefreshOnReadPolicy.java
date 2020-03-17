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

/**
 * This policy ensures that any writes that have been done to index are made visible
 * *before* any read is performed. Its meant as an alternative to {@link RefreshOnWritePolicy}
 * and for "sync" indexes. For "nrt" indexes {@link TimedRefreshPolicy} should be preferred
 *
 * <p>The readers are not refreshed immediately upon write. Instead they would be refreshed if
 *
 * <ul>
 *     <li>Upon write if refreshDelta time has elapsed then readers would be refreshed</li>
 *     <li>Upon read if index is found to be updated then again readers would be refreshed</li>
 * </ul>
 *
 * <p>This policy can result in some contention if index is being frequently updated and
 * queried.
 *
 * *This is an experimental policy. Currently it causes high contention*
 */
public class RefreshOnReadPolicy implements ReaderRefreshPolicy, IndexUpdateListener {
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final Object lock = new Object();
    private final Clock clock;
    private final long refreshDelta;
    private volatile long lastRefreshTime;

    public RefreshOnReadPolicy(Clock clock, TimeUnit unit, long refreshDelta) {
        this.clock = clock;
        this.refreshDelta = unit.toMillis(refreshDelta);
    }

    @Override
    public void refreshOnReadIfRequired(Runnable refreshCallback) {
        if (dirty.get()){
            refreshWithLock(refreshCallback, false);
        }
    }

    @Override
    public void refreshOnWriteIfRequired(Runnable refreshCallback) {
        long currentTime = clock.getTime();
        if (currentTime - lastRefreshTime > refreshDelta) {
            //Do not set dirty instead directly refresh
            refreshWithLock(refreshCallback, true);
        } else {
            synchronized (lock){
                //Needs to be done in a lock otherwise
                //refreshWithLock would override this
                dirty.set(true);
            }
        }
    }

    @Override
    public void updated() {
        //Detect dirty based on call from refreshOnWriteIfRequired
        //as that would *always* be called if the index has been updated
        //And ensures that it gets calls after all changes for that index
        //for that transaction got committed
    }

    private void refreshWithLock(Runnable refreshCallback, boolean forceRefresh) {
        synchronized (lock){
            if (dirty.get() || forceRefresh) {
                refreshCallback.run();
                dirty.set(false);
                lastRefreshTime = clock.getTime();
            }
        }
    }
}
