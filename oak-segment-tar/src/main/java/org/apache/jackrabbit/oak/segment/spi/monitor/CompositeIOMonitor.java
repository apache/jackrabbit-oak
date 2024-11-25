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
 *
 */

package org.apache.jackrabbit.oak.segment.spi.monitor;

import static java.util.Objects.requireNonNull;
import static java.util.Collections.emptySet;

import java.io.File;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.jetbrains.annotations.NotNull;

/**
 * This {@link IOMonitor} instance delegates all calls to all
 * {@code IOMonitor} instances registered.
 */
public class CompositeIOMonitor implements IOMonitor {
    private final Set<IOMonitor> ioMonitors;

    /**
     * Create a new {@code CompositeIOMonitor} instance delegating the passed {@code ioMonitors}
     * @param ioMonitors  {@link IOMonitor} instances to delegate to
     */
    public CompositeIOMonitor(@NotNull Iterable<? extends IOMonitor> ioMonitors) {
        this.ioMonitors = CollectionUtils.newConcurrentHashSet(requireNonNull(ioMonitors));
    }

    /**
     * Create a new empty {@code CompositeIOMonitor} instance.
     */
    public CompositeIOMonitor() {
        this(emptySet());
    }

    /**
     * Register a {@code IOMonitor} instance to which this {@code CompositeIOMonitor}
     * will delegate all its calls until {@link Registration#unregister()} is called
     * on the return {@code Registration}.
     *
     * @param ioMonitor  {@code IOMonitor} to delegate to
     * @return  a {@code Registration} for {@code ioMonitor}.
     */
    @NotNull
    public Registration registerIOMonitor(@NotNull IOMonitor ioMonitor) {
        ioMonitors.add(requireNonNull(ioMonitor));
        return () -> ioMonitors.remove(ioMonitor);
    }

    @Override
    public void beforeSegmentRead(File file, long msb, long lsb, int length) {
        ioMonitors.forEach(ioMonitor ->
           ioMonitor.beforeSegmentRead(file, msb, lsb, length));
    }

    @Override
    public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
        ioMonitors.forEach(ioMonitor ->
           ioMonitor.afterSegmentRead(file, msb, lsb, length, elapsed));
    }

    @Override
    public void beforeSegmentWrite(File file, long msb, long lsb, int length) {
        ioMonitors.forEach(ioMonitor ->
           ioMonitor.beforeSegmentWrite(file, msb, lsb, length));
    }

    @Override
    public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        ioMonitors.forEach(ioMonitor ->
            ioMonitor.afterSegmentWrite(file, msb, lsb, length, elapsed));
    }
}
