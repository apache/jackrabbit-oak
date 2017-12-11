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

package org.apache.jackrabbit.oak.spi.whiteboard;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.junit.After;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.DEFAULT;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.RUN_ON_LEADER;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.ScheduleExecutionInstanceTypes.RUN_ON_SINGLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class WhiteboardUtilsTest {
    private List<Registration> regs = Lists.newArrayList();

    @After
    public void unregisterRegs(){
        new CompositeRegistration(regs).unregister();
    }

    @Test
    public void jmxBeanRegistration() throws Exception{
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Oak oak = new Oak().with(server);
        Whiteboard wb = oak.getWhiteboard();
        Hello hello = new Hello();
        regs.add(WhiteboardUtils.registerMBean(wb, HelloMBean.class, hello, "test", "hello"));
        assertNotNull(server.getObjectInstance(new ObjectName("org.apache.jackrabbit.oak:type=test,name=hello")));
    }

    @Test
    public void jmxBeanRegistrationDuplicate() throws Exception{
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Oak oak = new Oak().with(server);
        Whiteboard wb = oak.getWhiteboard();
        Hello hello = new Hello();
        regs.add(WhiteboardUtils.registerMBean(wb, HelloMBean.class, hello, "test", "hello"));

        //Second one would trigger a warning log but no affect on caller
        regs.add(WhiteboardUtils.registerMBean(wb, HelloMBean.class, hello, "test", "hello"));
        assertNotNull(server.getObjectInstance(new ObjectName("org.apache.jackrabbit.oak:type=test,name=hello")));
    }

    @Test
    public void stdMBean() throws Exception{
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Oak oak = new Oak().with(server);
        Whiteboard wb = oak.getWhiteboard();
        Hello hello = new HelloTest();
        regs.add(WhiteboardUtils.registerMBean(wb, HelloMBean.class, hello, "test", "hello"));
        assertNotNull(server.getObjectInstance(new ObjectName("org.apache.jackrabbit.oak:type=test,name=hello")));
    }

    @Test
    public void queryMBean() throws Exception{
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Oak oak = new Oak().with(server);
        Whiteboard wb = oak.getWhiteboard();
        QueryEngineSettings settings = new QueryEngineSettings();
        regs.add(WhiteboardUtils.registerMBean(wb, QueryEngineSettingsMBean.class, settings, "query", "settings"));
        assertNotNull(server.getObjectInstance(new ObjectName("org.apache.jackrabbit.oak:type=query,name=settings")));
    }

    @Test
    public void scheduledJobWithPoolName() throws Exception{
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), 1, false, true);
        assertNotNull(props.get().get("scheduler.threadPool"));

        props.set(null);
        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), 1, true, false);
        assertNull(props.get().get("scheduler.threadPool"));
        assertEquals("SINGLE", props.get().get("scheduler.runOn"));
    }

    @Test
    public void scheduledJobWithExtraProps() throws Exception{
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };

        Map<String, Object> config = ImmutableMap.<String, Object>of("foo", "bar");
        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), config, 1, false, true);
        assertNotNull(props.get().get("scheduler.threadPool"));
        assertEquals("bar", props.get().get("foo"));
    }

    @Test
    public void scheduledJobDefaultExecutionInstanceType() {
        Map<String, Object> config = Collections.emptyMap();
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), 1);
        assertNull(props.get().get("scheduler.runOn"));

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), 1, false, false);
        assertNull(props.get().get("scheduler.runOn"));

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), config,1, false, false);
        assertNull(props.get().get("scheduler.runOn"));

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), config, 1, DEFAULT, false);
        assertNull(props.get().get("scheduler.runOn"));
    }

    @Test
    public void scheduledJobOnSingle() {
        Map<String, Object> config = Collections.emptyMap();
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), 1, true, false);
        assertEquals("SINGLE", props.get().get("scheduler.runOn"));

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), config, 1, true, false);
        assertEquals("SINGLE", props.get().get("scheduler.runOn"));

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), config, 1, RUN_ON_SINGLE, false);
        assertEquals("SINGLE", props.get().get("scheduler.runOn"));
    }

    @Test
    public void scheduledJobOnLeader() {
        Map<String, Object> config = Collections.emptyMap();
        final AtomicReference<Map<?, ?>> props = new AtomicReference<Map<?, ?>>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                props.set(properties);
                return super.register(type, service, properties);
            }
        };

        WhiteboardUtils.scheduleWithFixedDelay(wb, new TestRunnable(), config, 1, RUN_ON_LEADER, false);
        assertEquals("LEADER", props.get().get("scheduler.runOn"));
    }

    public interface HelloMBean {
        boolean isRunning();
        int getCount();
    }

    private static class Hello implements HelloMBean {
        int count;
        boolean running;

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int getCount() {
            return count;
        }
    }

    private static class HelloTest extends Hello {

    }

    private static class TestRunnable implements Runnable {
        @Override
        public void run() {

        }
    }
}
