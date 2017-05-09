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
package org.apache.jackrabbit.oak.spi.whiteboard;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * {@code AbstractServiceTracker} is a base class for composite components
 * that dynamically look up the available component services from the
 * whiteboard.
 */
public abstract class AbstractServiceTracker<T> {

    /**
     * Sentinel object used as the {@link #tracker} value of an already
     * stopped instance.
     */
    private final Tracker<T> stopped = new Tracker<T>() {
        @Override
        public List<T> getServices() {
            return emptyList();
        }
        @Override
        public void stop() {
            // do nothing
        }
    };

    /**
     * The type of services tracked by this instance.
     */
    private final Class<T> type;

    /**
     * The underlying {@link Tracker}, or the {@link #stopped} sentinel
     * sentinel object when this instance is not active. This variable
     * is {@code volatile} so that the {@link #getServices()} method will
     * always see the latest state without having to be synchronized.
     */
    private volatile Tracker<T> tracker = stopped;

    protected AbstractServiceTracker(@Nonnull Class<T> type) {
        this.type = checkNotNull(type);
    }

    public synchronized void start(Whiteboard whiteboard) {
        checkState(tracker == stopped);
        tracker = whiteboard.track(type);
    }

    public synchronized void stop() {
        if (tracker != stopped) {
            Tracker<T> t = tracker;
            tracker = stopped;
            t.stop();
        }
    }

    /**
     * Returns all services of type {@code T} that are currently available.
     * This method is intentionally not synchronized to prevent lock
     * contention when accessed frequently in highly concurrent code.
     *
     * @return currently available services
     */
    protected List<T> getServices() {
        return tracker.getServices();
    }

}
