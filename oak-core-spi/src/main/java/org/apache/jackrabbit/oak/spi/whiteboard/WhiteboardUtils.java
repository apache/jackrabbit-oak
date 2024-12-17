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

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.jackrabbit.oak.commons.jmx.JmxUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.DEFAULT;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.RUN_ON_LEADER;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.RUN_ON_SINGLE;

public class WhiteboardUtils {

    /**
     * JMX Domain name under which Oak related JMX MBeans are registered
     */
    public static final String JMX_OAK_DOMAIN = "org.apache.jackrabbit.oak";

    public enum ScheduleExecutionInstanceTypes {
        DEFAULT,
        RUN_ON_SINGLE,
        RUN_ON_LEADER
    }

    public static Registration scheduleWithFixedDelay(
            Whiteboard whiteboard, Runnable runnable, long delayInSeconds) {
        return scheduleWithFixedDelay(whiteboard, runnable, delayInSeconds, false, false);
    }

    public static Registration scheduleWithFixedDelay(
            Whiteboard whiteboard, Runnable runnable, long delayInSeconds, boolean runOnSingleClusterNode,
            boolean useDedicatedPool) {
        return scheduleWithFixedDelay(whiteboard, runnable, Collections.<String, Object>emptyMap(),
                delayInSeconds, runOnSingleClusterNode, useDedicatedPool);
    }

    public static Registration scheduleWithFixedDelay(
            Whiteboard whiteboard, Runnable runnable, Map<String, Object> extraProps, long delayInSeconds, boolean runOnSingleClusterNode,
            boolean useDedicatedPool) {
        return scheduleWithFixedDelay(whiteboard, runnable, extraProps, delayInSeconds,
                runOnSingleClusterNode ? RUN_ON_SINGLE : DEFAULT,
                useDedicatedPool);
    }

    public static Registration scheduleWithFixedDelay(
            Whiteboard whiteboard, Runnable runnable, Map<String, Object> extraProps, long delayInSeconds,
            ScheduleExecutionInstanceTypes scheduleExecutionInstanceTypes, boolean useDedicatedPool) {

        Map<String, Object> builder = new HashMap<>();
        builder.putAll(extraProps);
        builder.put("scheduler.period", delayInSeconds);
        builder.put("scheduler.concurrent", false);
        if (scheduleExecutionInstanceTypes == RUN_ON_SINGLE) {
            //Make use of feature while running in Sling SLING-5387
            builder.put("scheduler.runOn", "SINGLE");
        } else if (scheduleExecutionInstanceTypes == RUN_ON_LEADER) {
            //Make use of feature while running in Sling SLING-2979
            builder.put("scheduler.runOn", "LEADER");
        }
        if (useDedicatedPool) {
            //Make use of dedicated threadpool SLING-5831
            builder.put("scheduler.threadPool", "oak");
        }
        return whiteboard.register(
                Runnable.class, runnable, Collections.unmodifiableMap(builder));
    }

    public static <T> Registration registerMBean(
            Whiteboard whiteboard,
            Class<T> iface, T bean, String type, String name) {
        return registerMBean(whiteboard, iface, bean, type, name, Collections.<String, String>emptyMap());
    }

    public static <T> Registration registerMBean(
            Whiteboard whiteboard,
            Class<T> iface, T bean, String type, String name, Map<String, String> attrs) {
        try {

            Hashtable<String, String> table = new Hashtable<String, String>(attrs);
            table.put("type", JmxUtil.quoteValueIfRequired(type));
            table.put("name", JmxUtil.quoteValueIfRequired(name));

            // ensure the MBean is tracked by the Aries JMX Whiteboard
            // https://aries.apache.org/documentation/modules/jmx.html#_whiteboard_support
            @SuppressWarnings("rawtypes")
            Map properties = new HashMap();
            properties.put("jmx.objectname", new ObjectName(JMX_OAK_DOMAIN, table));
            properties.putAll(attrs);

            return whiteboard.register(iface, bean, Collections.unmodifiableMap(properties));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
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
    @NotNull
    public static <T> List<T> getServices(@NotNull Whiteboard wb, @NotNull Class<T> type) {
        return getServices(wb, type, (java.util.function.Predicate<T>) null);
    }

    /**
     * Returns the one of the currently available services from the whiteboard of the tracked type.
     *
     * Note that the underlying tracker is closed automatically.
     *
     * @return one service or {@code null}
     */
    @Nullable
    public static <T> T getService(@NotNull Whiteboard wb, @NotNull Class<T> type) {
        return getService(wb, type, (java.util.function.Predicate<T>) null);
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
    @NotNull
    public static <T> List<T> getServices(@NotNull Whiteboard wb, @NotNull Class<T> type, @Nullable Predicate<T> predicate) {
        Tracker<T> tracker = wb.track(type);
        try {
            if (predicate == null) {
                return tracker.getServices();
            } else {
                return ImmutableList.copyOf(Iterables.filter(tracker.getServices(), (input) -> predicate.test(input)));
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
    @Nullable
    public static <T> T getService(@NotNull Whiteboard wb, @NotNull Class<T> type, @Nullable Predicate<T> predicate) {
        Tracker<T> tracker = wb.track(type);
        try {
            for (T service : tracker.getServices()) {
                if (predicate == null || predicate.test(service)) {
                    return service;
                }
            }
            return null;
        } finally {
            tracker.stop();
        }

    }
}
