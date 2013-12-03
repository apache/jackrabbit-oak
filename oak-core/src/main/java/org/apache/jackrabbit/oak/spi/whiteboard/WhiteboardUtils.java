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

import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableMap;

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

}
