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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newConcurrentHashSet;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;

/**
 * This {@link GCMonitor} implementation simply delegates all its call
 * to registered monitors.
 */
public class DelegatingGCMonitor implements GCMonitor {
    private final Set<GCMonitor> gcMonitors;

    /**
     * New instance with an initial set of delegates (which cannot be unregistered).
     * @param gcMonitors
     */
    public DelegatingGCMonitor(@Nonnull Collection<? extends GCMonitor> gcMonitors) {
        this.gcMonitors = newConcurrentHashSet();
        this.gcMonitors.addAll(gcMonitors);
    }

    /**
     * New instance without any delegate.
     */
    public DelegatingGCMonitor() {
        this(Sets.<GCMonitor>newConcurrentHashSet());
    }

    /**
     * Register a {@link GCMonitor}.
     * @param gcMonitor
     * @return  a {@link Registration} instance, which removes the registered
     *          {@code GCMonitor} instance when called.
     */
    public Registration registerGCMonitor(@Nonnull final GCMonitor gcMonitor) {
        gcMonitors.add(checkNotNull(gcMonitor));
        return new Registration() {
            @Override
            public void unregister() {
                gcMonitors.remove(gcMonitor);
            }
        };
    }

    @Override
    public void info(String message, Object... arguments) {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.info(message, arguments);
        }
    }

    @Override
    public void warn(String message, Object... arguments) {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.warn(message, arguments);
        }
    }

    @Override
    public void error(String message, Exception exception) {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.error(message, exception);
        }
    }

    @Override
    public void skipped(String reason, Object... arguments) {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.skipped(reason, arguments);
        }
    }

    @Override
    public void compacted() {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.compacted();
        }
    }

    @Override
    public void cleaned(long reclaimedSize, long currentSize) {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.cleaned(reclaimedSize, currentSize);
        }
    }
    
    @Override
    public void updateStatus(String status) {
        for (GCMonitor gcMonitor : gcMonitors) {
            gcMonitor.updateStatus(status);
        }
    }

}
