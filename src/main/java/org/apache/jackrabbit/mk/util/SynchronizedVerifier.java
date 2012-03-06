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
package org.apache.jackrabbit.mk.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A utility class that allows to verify access to a resource is synchronized.
 */
public class SynchronizedVerifier {

    private static volatile boolean enabled;
    private static final Map<Class<?>, AtomicBoolean> DETECT =
        Collections.synchronizedMap(new HashMap<Class<?>, AtomicBoolean>());
    private static final Map<Object, Boolean> CURRENT =
        Collections.synchronizedMap(new IdentityHashMap<Object, Boolean>());

    /**
     * Enable or disable detection for a given class.
     *
     * @param clazz the class
     * @param value the new value (true means detection is enabled)
     */
    public static void setDetect(Class<?> clazz, boolean value) {
        if (value) {
            DETECT.put(clazz, new AtomicBoolean());
        } else {
            AtomicBoolean b = DETECT.remove(clazz);
            if (b == null) {
                throw new AssertionError("Detection was not enabled");
            } else if (!b.get()) {
                throw new AssertionError("No object of this class was tested");
            }
        }
        enabled = DETECT.size() > 0;
    }

    /**
     * Verify the object is not accessed concurrently.
     *
     * @param o the object
     * @param write if the object is modified
     */
    public static void check(Object o, boolean write) {
        if (enabled) {
            detectConcurrentAccess(o, write);
        }
    }

    private static void detectConcurrentAccess(Object o, boolean write) {
        AtomicBoolean value = DETECT.get(o.getClass());
        if (value != null) {
            value.set(true);
            Boolean old = CURRENT.put(o, write);
            if (old != null) {
                if (write || old) {
                    throw new AssertionError("Concurrent write access");
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
            old = CURRENT.remove(o);
            if (old == null && write) {
                throw new AssertionError("Concurrent write access");
            }
        }
    }

}
