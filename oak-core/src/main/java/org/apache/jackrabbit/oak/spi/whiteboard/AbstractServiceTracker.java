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

import java.util.List;

import javax.annotation.Nonnull;

/**
 * {@code AbstractServiceTracker} is a base class for composite components
 * that dynamically look up the available component services from the
 * whiteboard.
 */
public abstract class AbstractServiceTracker<T> {

    private final Class<T> type;

    private Tracker<T> tracker = null;

    public AbstractServiceTracker(@Nonnull Class<T> type) {
        this.type = checkNotNull(type);
    }

    public synchronized void start(Whiteboard whiteboard) {
        checkState(tracker == null);
        tracker = whiteboard.track(type);
    }

    public synchronized void stop() {
        checkState(tracker != null);
        tracker.stop();
        tracker = null;
    }

    /**
     * Returns all services of type {@code T} currently available.
     *
     * @return services currently available.
     */
    protected synchronized List<T> getServices() {
        checkState(tracker != null);
        return tracker.getServices();
    }

}
