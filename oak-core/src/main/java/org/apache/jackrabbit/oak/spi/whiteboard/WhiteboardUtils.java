/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.whiteboard;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.spi.commit.Observer;

public class WhiteboardUtils {

    private static final AtomicLong COUNTER = new AtomicLong();

    public static Registration scheduleWithFixedDelay(
            Whiteboard whiteboard, Runnable runnable, long delayInSeconds) {
        return scheduleWithFixedDelay(whiteboard, runnable, delayInSeconds, false);
    }

    public static Registration scheduleWithFixedDelay(
            Whiteboard whiteboard, Runnable runnable, long delayInSeconds, boolean runOnSingleClusterNode) {
        ImmutableMap.Builder<String,Object> builder = ImmutableMap.<String,Object>builder()
                .put("scheduler.period", delayInSeconds)
                .put("scheduler.concurrent", false);
        if (runOnSingleClusterNode) {
            //Make use of feature while running in Sling SLING-2979
            builder.put("scheduler.runOn", "SINGLE");
        }
        return whiteboard.register(
                Runnable.class, runnable, builder.build());
    }

    public static <T> Registration registerMBean(
            Whiteboard whiteboard,
            Class<T> iface, T bean, String type, String name) {
        try {
            Hashtable<String, String> table = new Hashtable<String, String>();
            table.put("type", ObjectName.quote(type));
            table.put("name", ObjectName.quote(name));
            table.put("id", String.valueOf(COUNTER.incrementAndGet()));
            return whiteboard.register(iface, bean, ImmutableMap.of(
                    "jmx.objectname",
                    new ObjectName("org.apache.jackrabbit.oak", table)));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Registration registerObserver(
            @Nonnull Whiteboard whiteboard,
            @Nonnull Observer observer) {
        return checkNotNull(whiteboard)
                .register(Observer.class, checkNotNull(observer), emptyMap());
    }

    /**
     * Returns the currently available services from the whiteboard of the tracked type.
     *
     * Note that the underlying tracker is closed automatically.
     *
     * @param wb the whiteboard
     * @param type the service type
     * @return a list of services
     */
    @Nonnull
    public static <T> List<T> getServices(@Nonnull Whiteboard wb, @Nonnull Class<T> type) {
        return getServices(wb, type, null);
    }

    /**
     * Returns the one of the currently available services from the whiteboard of the tracked type.
     *
     * Note that the underlying tracker is closed automatically.
     *
     * @return one service or {@code null}
     */
    @CheckForNull
    public static <T> T getService(@Nonnull Whiteboard wb, @Nonnull Class<T> type) {
        return getService(wb, type, null);
    }

    /**
     * Returns the currently available services from the whiteboard of the tracked type. If {@code predicate} is
     * not {@code null} the returned list is limited to the ones that match the predicate.
     *
     * Note that the underlying tracker is stopped automatically after the services are returned.
     *
     * @param wb the whiteboard
     * @param type the service type
     * @param predicate filtering predicate or {@code null}
     * @return a list of services
     */
    @Nonnull
    public static <T> List<T> getServices(@Nonnull Whiteboard wb, @Nonnull Class<T> type, @Nullable Predicate<T> predicate) {
        Tracker<T> tracker = wb.track(type);
        try {
            if (predicate == null) {
                return tracker.getServices();
            } else {
                return ImmutableList.copyOf(Iterables.filter(tracker.getServices(), predicate));
            }
        } finally {
            tracker.stop();
        }
    }

    /**
     * Returns the one of the currently available services from the whiteboard of the tracked type. If {@code predicate} is
     * not {@code null} only a service that match the predicate is returned.
     *
     * Note that the underlying tracker is closed automatically.
     *
     * @param wb the whiteboard
     * @param type the service type
     * @param predicate filtering predicate or {@code null}
     * @return one service or {@code null}
     */
    @CheckForNull
    public static <T> T getService(@Nonnull Whiteboard wb, @Nonnull Class<T> type, @Nullable Predicate<T> predicate) {
        Tracker<T> tracker = wb.track(type);
        try {
            for (T service : tracker.getServices()) {
                if (predicate == null || predicate.apply(service)) {
                    return service;
                }
            }
            return null;
        } finally {
            tracker.stop();
        }

    }


}
