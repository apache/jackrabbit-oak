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
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.junit.After;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;

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
    public void quotation() throws Exception{
        assertEquals("text", WhiteboardUtils.quoteIfRequired("text"));
        assertEquals("", WhiteboardUtils.quoteIfRequired(""));
        assertTrue(WhiteboardUtils.quoteIfRequired("text*with?chars").startsWith("\""));
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
}
