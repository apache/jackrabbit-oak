/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.jackrabbit.oak.run.osgi;

import static org.junit.Assert.assertTrue;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.felix.connect.launch.PojoServiceRegistryFactory;
import org.junit.After;
import org.junit.Test;
import org.osgi.framework.BundleException;

public class TrackerSupportTest {

    private final PojoServiceRegistry reg = createServiceRegistry(new LinkedHashMap<>());

    @After
    public void tearDown() throws BundleException {
        reg.getBundleContext().getBundle().stop();
    }

    @Test
    public void runnableTest() throws Exception {
        try (RunnableJobTracker runnableJobTracker = new RunnableJobTracker(reg.getBundleContext())) {

            final CountDownLatch latch = new CountDownLatch(1);
            Runnable runnable = latch::countDown;
            reg.registerService(Runnable.class.getName(), runnable, new Hashtable<>(Map.of("scheduler.period", 1)));

            //Wait for latch to get executed otherwise fail with timeout
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
    }

    private PojoServiceRegistry createServiceRegistry(Map<String, Object> config) {
        try {
            ServiceLoader<PojoServiceRegistryFactory> loader = ServiceLoader.load(PojoServiceRegistryFactory.class);
            return loader.iterator().next().newPojoServiceRegistry(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
