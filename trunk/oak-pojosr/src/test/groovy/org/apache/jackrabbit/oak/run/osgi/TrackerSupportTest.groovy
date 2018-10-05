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

package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.felix.connect.launch.PojoServiceRegistryFactory
import org.junit.After
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class TrackerSupportTest {
    PojoServiceRegistry reg = createServiceRegistry([:])

    @After
    public void tearDown(){
        reg.getBundleContext().getBundle().stop()
    }

    @Test
    public void runnableTest() throws Exception{
        new RunnableJobTracker(reg.bundleContext)

        CountDownLatch latch = new CountDownLatch(1)
        reg.registerService(Runnable.class.name,{latch.countDown()} as Runnable,
                ['scheduler.period':1] as Hashtable)

        //Wait for latch to get executed otherwise fail with timeout
        latch.await(5, TimeUnit.SECONDS)
    }

    private PojoServiceRegistry createServiceRegistry(Map<String, Object> config) {
        ServiceLoader<PojoServiceRegistryFactory> loader = ServiceLoader.load(PojoServiceRegistryFactory.class);
        return loader.iterator().next().newPojoServiceRegistry(config);
    }
}
