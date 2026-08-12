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
 */
package org.apache.jackrabbit.oak.cache.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CacheMaintenanceExecutor}.
 */
public class CacheMaintenanceExecutorTest {

    @Test
    public void threadFactoryDoesNotInheritCallerContextClassLoader() {
        ClassLoader markerClassLoader = new ClassLoader() {
        };
        Thread currentThread = Thread.currentThread();
        ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(markerClassLoader);
        try {
            Thread worker = CacheMaintenanceExecutor.newThreadFactory(new AtomicInteger()).newThread(() -> {
            });

            Assert.assertSame(CacheMaintenanceExecutor.class.getClassLoader(),
                    worker.getContextClassLoader());
            Assert.assertNotSame(markerClassLoader, worker.getContextClassLoader());
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }
}
