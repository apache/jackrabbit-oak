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

package org.apache.jackrabbit.oak.spi.gc;

import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * This {@link GCMonitor} implementation tracks {@code GCMonitor} instances registered
 * to the {@link Whiteboard} delegating all calls to to those.
 */
public class GCMonitorTracker extends AbstractServiceTracker<GCMonitor> implements GCMonitor {
    public GCMonitorTracker() {
        super(GCMonitor.class);
    }

    @Override
    public void info(String message, Object... arguments) {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.info(message, arguments);
        }
    }

    @Override
    public void warn(String message, Object... arguments) {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.warn(message, arguments);
        }
    }

    @Override
    public void error(String message, Exception e) {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.error(message, e);
        }
    }

    @Override
    public void skipped(String message, Object... arguments) {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.skipped(message, arguments);
        }
    }

    @Override
    public void compacted() {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.compacted();
        }
    }

    @Override
    public void cleaned(long reclaimedSize, long currentSize) {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.cleaned(reclaimedSize, currentSize);
        }
    }

    @Override
    public void updateStatus(String status) {
        for (GCMonitor gcMonitor : getServices()) {
            gcMonitor.updateStatus(status);
        }
    }
}
