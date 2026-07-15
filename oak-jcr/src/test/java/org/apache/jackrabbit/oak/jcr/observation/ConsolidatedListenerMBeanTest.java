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
package org.apache.jackrabbit.oak.jcr.observation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.junit.Test;

public class ConsolidatedListenerMBeanTest {

    @Test
    public void testListenerStatsData() throws MalformedObjectNameException {
        ConsolidatedListenerMBeanImpl consolidatedListener = new ConsolidatedListenerMBeanImpl();

        EventListener listener = new Listener(new AtomicInteger());
        EventListenerMBean mbean = new ListenerTracker(listener, 0, "/", false, null, null, false).getListenerMBean();

        Map<String, ObjectName> config = new HashMap<>();
        config.put("jmx.objectname", new ObjectName("*:*"));
        consolidatedListener.bindListenerMBean(mbean, config);
        TabularData data = consolidatedListener.getListenerStats();
        assertNotNull(data);
    }

    @Test(expected = NullPointerException.class)
    public void testGetConfigNameNoValue() {
        final Map<String, Object> properties = Collections.emptyMap();
        ConsolidatedListenerMBeanImpl.getObjectName(properties);
    }

    @Test
    public void testGetConfigNameObjectNameValue() throws MalformedObjectNameException {
        final ObjectName name = new ObjectName("*:*");
        final Map<String, Object> properties = Collections.singletonMap("jmx.objectname", name);
        assertSame(name, ConsolidatedListenerMBeanImpl.getObjectName(properties));
    }

    @Test
    public void testGetConfigNameStringValue() throws MalformedObjectNameException {
        final String name = "*:*";
        final Map<String, Object> properties = Collections.singletonMap("jmx.objectname", name);
        assertEquals(new ObjectName(name), ConsolidatedListenerMBeanImpl.getObjectName(properties));
    }

    @Test(expected = NullPointerException.class)
    public void testGetConfigNameInvalidValue() {
        final Map<String, Object> properties = Collections.singletonMap("jmx.objectname", Boolean.TRUE);
        ConsolidatedListenerMBeanImpl.getObjectName(properties);
    }

    private static class Listener implements EventListener {
        private final AtomicInteger eventCount;

        Listener(AtomicInteger eventCount) {
            this.eventCount = eventCount;
        }

        @Override
        public void onEvent(EventIterator events) {
            for (; events.hasNext(); events.nextEvent()) {
                eventCount.incrementAndGet();
            }
        }
    }
}
