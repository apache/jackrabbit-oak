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

import org.apache.jackrabbit.oak.segment.standby.client.StandbySync;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServer;
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
        final StandbyServer server = new StandbyServer(TestBase.port, this.storeS);
        server.start();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(1, instances.size());
            status = instances.toArray(new ObjectName[0])[0];
            assertEquals(new ObjectName(server.getMBeanName()), status);
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
            server.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientEmptyConfigNoServer() throws Exception {
        final StandbySync client = newStandbySync(storeC);
        client.start();
        client.run();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(1, instances.size());
            status = instances.toArray(new ObjectName[0])[0];
            assertEquals(new ObjectName(client.getMBeanName()), status);
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
            client.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientNoServer() throws Exception {
        System.setProperty(StandbySync.CLIENT_ID_PROPERTY_NAME, "Foo");
        final StandbySync client = newStandbySync(storeC);
        client.start();
        client.run();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(client.getMBeanName());
        try {
            assertTrue(jmxServer.isRegistered(status));
            assertEquals("client: Foo", jmxServer.getAttribute(status, "Mode"));

            assertEquals("1", jmxServer.getAttribute(status, "FailedRequests").toString());
            assertEquals("-1", jmxServer.getAttribute(status, "SecondsSinceLastSuccess").toString());

            assertEquals("1", jmxServer.invoke(status, "calcFailedRequests", null, null).toString());
            assertEquals("-1", jmxServer.invoke(status, "calcSecondsSinceLastSuccess", null, null).toString());
        } finally {
            client.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientAndServerEmptyConfig() throws Exception {
        final StandbyServer server = new StandbyServer(port, this.storeS);
        server.start();

        System.setProperty(StandbySync.CLIENT_ID_PROPERTY_NAME, "Bar");
        final StandbySync client = newStandbySync(storeC);
        client.start();
        client.run();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        ObjectName clientStatus = new ObjectName(client.getMBeanName());
        ObjectName serverStatus = new ObjectName(server.getMBeanName());
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

            assertEquals(new Long(1), jmxServer.getAttribute(connectionStatus, "TransferredSegments"));
            assertEquals(new Long(208), jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes"));
            
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
            client.close();
            server.close();
        }

        assertTrue(!jmxServer.isRegistered(clientStatus));
        assertTrue(!jmxServer.isRegistered(serverStatus));
    }
}
