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

package org.apache.jackrabbit.oak.segment.standby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MBeanTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    public void testServerEmptyConfig() throws Exception {
        final StandbyServerSync serverSync = new StandbyServerSync(TestBase.port, this.storeS);
        serverSync.start();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(1, instances.size());
            status = instances.toArray(new ObjectName[0])[0];
            assertEquals(new ObjectName(serverSync.getMBeanName()), status);
            assertTrue(jmxServer.isRegistered(status));

            assertEquals("primary", jmxServer.getAttribute(status, "Mode"));
            String m = jmxServer.getAttribute(status, "Status").toString();
            if (!m.equals(StandbyStatusMBean.STATUS_RUNNING) && !m.equals("channel unregistered"))
                fail("unexpected Status " + m);

            assertEquals(StandbyStatusMBean.STATUS_RUNNING, jmxServer.getAttribute(status, "Status"));
            assertEquals(true, jmxServer.getAttribute(status, "Running"));
            jmxServer.invoke(status, "stop", null, null);
            assertEquals(false, jmxServer.getAttribute(status, "Running"));
            assertEquals(StandbyStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(status, "Status"));
            jmxServer.invoke(status, "start", null, null);

            assertEquals(true, jmxServer.getAttribute(status, "Running"));
            assertEquals(StandbyStatusMBean.STATUS_RUNNING, jmxServer.getAttribute(status, "Status"));
        } finally {
            serverSync.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientEmptyConfigNoServer() throws Exception {
        final StandbyClientSync clientSync = newStandbyClientSync(storeC);
        clientSync.start();
        clientSync.run();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(1, instances.size());
            status = instances.toArray(new ObjectName[0])[0];
            assertEquals(new ObjectName(clientSync.getMBeanName()), status);
            assertTrue(jmxServer.isRegistered(status));

            String m = jmxServer.getAttribute(status, "Mode").toString();
            if (!m.startsWith("client: ")) fail("unexpected mode " + m);

            assertEquals("1", jmxServer.getAttribute(status, "FailedRequests").toString());
            assertEquals("-1", jmxServer.getAttribute(status, "SecondsSinceLastSuccess").toString());

            assertEquals(StandbyStatusMBean.STATUS_RUNNING, jmxServer.getAttribute(status, "Status"));

            assertEquals(true, jmxServer.getAttribute(status, "Running"));
            jmxServer.invoke(status, "stop", null, null);
            assertEquals(false, jmxServer.getAttribute(status, "Running"));
            assertEquals(StandbyStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(status, "Status"));
            jmxServer.invoke(status, "start", null, null);
            assertEquals(true, jmxServer.getAttribute(status, "Running"));
            assertEquals(StandbyStatusMBean.STATUS_RUNNING, jmxServer.getAttribute(status, "Status"));
        } finally {
            clientSync.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientNoServer() throws Exception {
        System.setProperty(StandbyClientSync.CLIENT_ID_PROPERTY_NAME, "Foo");
        final StandbyClientSync clientSync = newStandbyClientSync(storeC);
        clientSync.start();
        clientSync.run();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(clientSync.getMBeanName());
        try {
            assertTrue(jmxServer.isRegistered(status));
            assertEquals("client: Foo", jmxServer.getAttribute(status, "Mode"));

            assertEquals("1", jmxServer.getAttribute(status, "FailedRequests").toString());
            assertEquals("-1", jmxServer.getAttribute(status, "SecondsSinceLastSuccess").toString());

            assertEquals("1", jmxServer.invoke(status, "calcFailedRequests", null, null).toString());
            assertEquals("-1", jmxServer.invoke(status, "calcSecondsSinceLastSuccess", null, null).toString());
        } finally {
            clientSync.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientAndServerEmptyConfig() throws Exception {
        final StandbyServerSync serverSync = new StandbyServerSync(port, this.storeS);
        serverSync.start();

        System.setProperty(StandbyClientSync.CLIENT_ID_PROPERTY_NAME, "Bar");
        final StandbyClientSync clientSync = newStandbyClientSync(storeC);
        clientSync.start();
        clientSync.run();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        ObjectName clientStatus = new ObjectName(clientSync.getMBeanName());
        ObjectName serverStatus = new ObjectName(serverSync.getMBeanName());
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(3, instances.size());

            ObjectName connectionStatus = null;
            for (ObjectName s : instances) {
                if (!s.equals(clientStatus) && !s.equals(serverStatus)) connectionStatus = s;
            }
            assertNotNull(connectionStatus);

            assertTrue(jmxServer.isRegistered(clientStatus));
            assertTrue(jmxServer.isRegistered(serverStatus));
            assertTrue(jmxServer.isRegistered(connectionStatus));

            String m = jmxServer.getAttribute(clientStatus, "Mode").toString();
            if (!m.startsWith("client: ")) fail("unexpected mode " + m);

            assertEquals("primary", jmxServer.getAttribute(serverStatus, "Mode"));

            assertEquals(true, jmxServer.getAttribute(serverStatus, "Running"));
            assertEquals(true, jmxServer.getAttribute(clientStatus, "Running"));

            assertEquals("0", jmxServer.getAttribute(clientStatus, "FailedRequests").toString());
            assertEquals("0", jmxServer.getAttribute(clientStatus, "SecondsSinceLastSuccess").toString());
            assertEquals("0", jmxServer.invoke(clientStatus, "calcFailedRequests", null, null).toString());
            assertEquals("0", jmxServer.invoke(clientStatus, "calcSecondsSinceLastSuccess", null, null).toString());

            Thread.sleep(1000);

            assertEquals("0", jmxServer.getAttribute(clientStatus, "FailedRequests").toString());
            assertEquals("1", jmxServer.getAttribute(clientStatus, "SecondsSinceLastSuccess").toString());
            assertEquals("0", jmxServer.invoke(clientStatus, "calcFailedRequests", null, null).toString());
            assertEquals("1", jmxServer.invoke(clientStatus, "calcSecondsSinceLastSuccess", null, null).toString());

            assertEquals(1L, jmxServer.getAttribute(connectionStatus, "TransferredSegments"));

            // stop the master
            jmxServer.invoke(serverStatus, "stop", null, null);
            assertEquals(false, jmxServer.getAttribute(serverStatus, "Running"));
            m = jmxServer.getAttribute(serverStatus, "Status").toString();
            if (!m.equals(StandbyStatusMBean.STATUS_STOPPED) && !m.equals("channel unregistered"))
                fail("unexpected Status" + m);

            // restart the master
            jmxServer.invoke(serverStatus, "start", null, null);
            assertEquals(true, jmxServer.getAttribute(serverStatus, "Running"));
            assertEquals(true, jmxServer.getAttribute(clientStatus, "Running"));
            m = jmxServer.getAttribute(serverStatus, "Status").toString();
            if (!m.equals(StandbyStatusMBean.STATUS_RUNNING) && !m.equals("channel unregistered"))
                fail("unexpected Status" + m);

            // stop the slave
            jmxServer.invoke(clientStatus, "stop", null, null);
            assertEquals(true, jmxServer.getAttribute(serverStatus, "Running"));
            assertEquals(false, jmxServer.getAttribute(clientStatus, "Running"));
            assertEquals(StandbyStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(clientStatus, "Status"));

            // restart the slave
            jmxServer.invoke(clientStatus, "start", null, null);
            assertEquals(true, jmxServer.getAttribute(clientStatus, "Running"));
            assertEquals(StandbyStatusMBean.STATUS_RUNNING, jmxServer.getAttribute(clientStatus, "Status"));

        } finally {
            clientSync.close();
            serverSync.close();
        }

        assertTrue(!jmxServer.isRegistered(clientStatus));
        assertTrue(!jmxServer.isRegistered(serverStatus));
    }
}
