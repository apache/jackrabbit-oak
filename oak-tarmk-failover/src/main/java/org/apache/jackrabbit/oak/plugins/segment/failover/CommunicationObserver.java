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


import org.apache.jackrabbit.oak.plugins.segment.failover.jmx.FailoverStatusMBean;
import org.apache.jackrabbit.oak.plugins.segment.failover.jmx.ObservablePartnerMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CommunicationObserver {

    private class CommunicationPartnerMBean implements ObservablePartnerMBean {
        private final ObjectName mbeanName;
        private final String clientName;
        public String lastRequest;
        public Date lastSeen;
        public String remoteAddress;
        public int remotePort;

        public CommunicationPartnerMBean(String clientName) throws MalformedObjectNameException {
            this.clientName = clientName;
            this.mbeanName = new ObjectName(FailoverStatusMBean.JMX_NAME + ",id=\"Client " + clientName + "\"");
        }

        public ObjectName getMBeanName() {
            return this.mbeanName;
        }

        @Override
        public String getName() {
            return this.clientName;
        }

        @Override
        public String getRemoteAddress() {
            return this.remoteAddress;
        }

        @Override
        public String getLastRequest() {
            return this.lastRequest;
        }

        @Override
        public int getRemotePort() {
            return this.remotePort;
        }

        @Override
        public String getLastSeenTimestamp() {
            return this.lastSeen == null ? null : this.lastSeen.toString();
        }
    }

    private static final Logger log = LoggerFactory
            .getLogger(CommunicationObserver.class);

    private final String identifier;
    private final Map<String, CommunicationPartnerMBean> partnerDetails;

    public CommunicationObserver(String myID) {
        this.identifier = myID;
        this.partnerDetails = new HashMap<String, CommunicationPartnerMBean>();
    }

    public void unregister() {
        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        for (CommunicationPartnerMBean m : this.partnerDetails.values()) {
            try {
                jmxServer.unregisterMBean(m.getMBeanName());
            }
            catch (Exception e) {
                log.error("error unregistering mbean for client '" + m.getName() + "'", e);
            }
        }
    }

    public void gotMessageFrom(String client, String request, InetSocketAddress remote) throws MalformedObjectNameException {
        CommunicationPartnerMBean m = this.partnerDetails.get(client);
        boolean register = false;
        if (m == null) {
            m = new CommunicationPartnerMBean(client);
            m.remoteAddress = remote.getAddress().getHostAddress();
            m.remotePort = remote.getPort();
            register = true;
        }
        m.lastSeen = new Date();
        m.lastRequest = request;
        this.partnerDetails.put(client, m);
        if (register) {
            final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
            try {
                jmxServer.registerMBean(new StandardMBean(m, ObservablePartnerMBean.class), m.getMBeanName());
            }
            catch (Exception e) {
                log.error("can register mbean for client '" + m.getName() + "'", e);
            }
        }
    }

    public String getID() {
        return this.identifier;
    }
}
