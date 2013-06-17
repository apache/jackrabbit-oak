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
            Whiteboard whiteboard, Runnable runnable, long delay) {
        return whiteboard.register(
                Runnable.class, runnable, ImmutableMap.builder()
                    .put("scheduler.period", delay)
                    .put("scheduler.concurrent", false)
                    .build());
    }

    public static <T> Registration registerMBean(
            Whiteboard whiteboard,
            Class<T> iface, T bean, String type, String name) {
        try {
            Hashtable<String, String> table = new Hashtable<String, String>();
            table.put("type", type);
            table.put("name", name);
            table.put("id", String.valueOf(COUNTER.incrementAndGet()));
            return whiteboard.register(iface, bean, ImmutableMap.of(
                    "jmx.objectname",
                    new ObjectName("org.apache.jackrabbit.oak", table)));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
