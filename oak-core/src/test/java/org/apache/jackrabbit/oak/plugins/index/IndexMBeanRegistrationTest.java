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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexMBeanRegistrationTest {

    @Test
    public void jobName() throws Exception {
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                if (service instanceof AsyncIndexUpdate) {
                    props.set(properties);
                }
                return super.register(type, service, properties);
            }
        };
        long schedulingDelayInSecs = 7; // some number which is hard to default else-where

        AsyncIndexUpdate update = new AsyncIndexUpdate("async",
                new MemoryNodeStore(), new CompositeIndexEditorProvider());
        IndexMBeanRegistration reg = new IndexMBeanRegistration(wb);
        reg.registerAsyncIndexer(update, schedulingDelayInSecs);
        try {
            Map<?, ?> map = props.get();
            assertNotNull(map);
            assertEquals(AsyncIndexUpdate.class.getName() + "-async",
                    map.get("scheduler.name"));
            assertEquals(schedulingDelayInSecs,
                    map.get("scheduler.period"));
            assertEquals(false,
                    map.get("scheduler.concurrent"));
            assertEquals("LEADER",
                    map.get("scheduler.runOn"));
            assertEquals("oak",
                    map.get("scheduler.threadPool"));
        } finally {
            reg.unregister();
        }
    }
}
