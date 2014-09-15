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
package org.apache.jackrabbit.oak.plugins.segment.failover;

import org.apache.jackrabbit.oak.plugins.segment.failover.client.FailoverClient;
import org.apache.jackrabbit.oak.plugins.segment.failover.jmx.FailoverStatusMBean;
import org.apache.jackrabbit.oak.plugins.segment.failover.server.FailoverServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;

import static junit.framework.Assert.*;

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
        final FailoverServer server = new FailoverServer(this.port, this.storeS);
        server.start();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(FailoverStatusMBean.JMX_NAME + ",id=*");
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(1, instances.size());
            status = instances.toArray(new ObjectName[0])[0];
            assertEquals(new ObjectName(server.getMBeanName()), status);
            assertTrue(jmxServer.isRegistered(status));

            assertEquals("master", jmxServer.getAttribute(status, "Mode"));
            String m = jmxServer.getAttribute(status, "Status").toString();
            if (!m.equals(FailoverStatusMBean.STATUS_STARTING) && !m.equals("channel unregistered"))
                fail("unexpected Status" + m);

            assertEquals(FailoverStatusMBean.STATUS_STARTING, jmxServer.getAttribute(status, "Status"));
            assertEquals(true, jmxServer.getAttribute(status, "Running"));
            jmxServer.invoke(status, "stop", null, null);
            assertEquals(false, jmxServer.getAttribute(status, "Running"));
            assertEquals(FailoverStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(status, "Status"));
            jmxServer.invoke(status, "start", null, null);

            assertEquals(true, jmxServer.getAttribute(status, "Running"));
            assertEquals(FailoverStatusMBean.STATUS_STARTING, jmxServer.getAttribute(status, "Status"));
        } finally {
            server.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientEmptyConfigNoServer() throws Exception {
        final FailoverClient client = new FailoverClient("127.0.0.1", this.port, this.storeC);
        client.start();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(FailoverStatusMBean.JMX_NAME + ",id=*");
        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(1, instances.size());
            status = instances.toArray(new ObjectName[0])[0];
            assertEquals(new ObjectName(client.getMBeanName()), status);
            assertTrue(jmxServer.isRegistered(status));

            String m = jmxServer.getAttribute(status, "Mode").toString();
            if (!m.startsWith("client: ")) fail("unexpected mode " + m);

            assertEquals(FailoverStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(status, "Status"));

            assertEquals(false, jmxServer.getAttribute(status, "Running"));
            jmxServer.invoke(status, "stop", null, null);
            assertEquals(false, jmxServer.getAttribute(status, "Running"));
            assertEquals(FailoverStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(status, "Status"));
            jmxServer.invoke(status, "start", null, null);
            assertEquals(false, jmxServer.getAttribute(status, "Running"));
            assertEquals(FailoverStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(status, "Status"));
        } finally {
            client.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    public void testClientNoServer() throws Exception {
        System.setProperty(FailoverClient.CLIENT_ID_PROPERTY_NAME, "Foo");
        final FailoverClient client = new FailoverClient("127.0.0.1", this.port, this.storeC);
        client.start();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(client.getMBeanName());
        try {
            assertTrue(jmxServer.isRegistered(status));
            assertEquals("client: Foo", jmxServer.getAttribute(status, "Mode"));
        } finally {
            client.close();
        }

        assertTrue(!jmxServer.isRegistered(status));
    }

    @Test
    @Ignore("OAK-2086")
    public void testClientAndServerEmptyConfig() throws Exception {
        final FailoverServer server = new FailoverServer(this.port, this.storeS);
        server.start();

        System.setProperty(FailoverClient.CLIENT_ID_PROPERTY_NAME, "Bar");
        final FailoverClient client = new FailoverClient("127.0.0.1", this.port, this.storeC);
        client.start();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(FailoverStatusMBean.JMX_NAME + ",id=*");
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

            assertEquals("master", jmxServer.getAttribute(serverStatus, "Mode"));

            assertEquals(true, jmxServer.getAttribute(serverStatus, "Running"));
            assertEquals(true, jmxServer.getAttribute(clientStatus, "Running"));

            assertEquals(new Long(2), jmxServer.getAttribute(connectionStatus, "TransferredSegments"));
            assertEquals(new Long(128), jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes"));

            // stop the master
            jmxServer.invoke(serverStatus, "stop", null, null);
            assertEquals(false, jmxServer.getAttribute(serverStatus, "Running"));
            m = jmxServer.getAttribute(serverStatus, "Status").toString();
            if (!m.equals(FailoverStatusMBean.STATUS_STOPPED) && !m.equals("channel unregistered"))
                fail("unexpected Status" + m);

            // restart the master
            jmxServer.invoke(serverStatus, "start", null, null);
            assertEquals(true, jmxServer.getAttribute(serverStatus, "Running"));
            assertEquals(true, jmxServer.getAttribute(clientStatus, "Running"));
            m = jmxServer.getAttribute(serverStatus, "Status").toString();
            if (!m.equals(FailoverStatusMBean.STATUS_STARTING) && !m.equals("channel unregistered"))
                fail("unexpected Status" + m);

            // stop the slave
            jmxServer.invoke(clientStatus, "stop", null, null);
            assertEquals(true, jmxServer.getAttribute(serverStatus, "Running"));
            assertEquals(false, jmxServer.getAttribute(clientStatus, "Running"));
            assertEquals(FailoverStatusMBean.STATUS_STOPPED, jmxServer.getAttribute(clientStatus, "Status"));

            // restart the slave
            jmxServer.invoke(clientStatus, "start", null, null);
            assertEquals(true, jmxServer.getAttribute(clientStatus, "Running"));
            assertEquals(FailoverStatusMBean.STATUS_RUNNING, jmxServer.getAttribute(clientStatus, "Status"));

        } finally {
            client.close();
            server.close();
        }

        assertTrue(!jmxServer.isRegistered(clientStatus));
        assertTrue(!jmxServer.isRegistered(serverStatus));
    }
}
