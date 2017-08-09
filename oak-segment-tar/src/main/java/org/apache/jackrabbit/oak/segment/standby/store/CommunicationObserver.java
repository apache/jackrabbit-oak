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

package org.apache.jackrabbit.oak.segment.standby.store;

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.management.MalformedObjectNameException;
import javax.management.StandardMBean;

import org.apache.jackrabbit.oak.segment.standby.jmx.ObservablePartnerMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunicationObserver {

    static final int MAX_CLIENT_STATISTICS = 10;

    private static final Logger log = LoggerFactory.getLogger(CommunicationObserver.class);

    private final Map<String, CommunicationPartnerMBean> partnerDetails = new HashMap<>();

    private final String id;

    public CommunicationObserver(String id) {
        this.id = id;
    }

    void unregisterCommunicationPartner(CommunicationPartnerMBean m) throws Exception {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(m.getMBeanName());
    }

    void registerCommunicationPartner(CommunicationPartnerMBean m) throws Exception {
        ManagementFactory.getPlatformMBeanServer().registerMBean(new StandardMBean(m, ObservablePartnerMBean.class), m.getMBeanName());
    }

    private void safeUnregisterCommunicationPartner(CommunicationPartnerMBean m) {
        try {
            unregisterCommunicationPartner(m);
        } catch (Exception e) {
            log.error(String.format("Unable to unregister MBean for client %s", m.getName()), e);
        }
    }

    private void safeRegisterCommunicationPartner(CommunicationPartnerMBean m) {
        try {
            registerCommunicationPartner(m);
        } catch (Exception e) {
            log.error(String.format("Unable to register MBean for client %s", m.getName()), e);
        }
    }

    public void unregister() {
        for (CommunicationPartnerMBean m : partnerDetails.values()) {
            safeUnregisterCommunicationPartner(m);
        }
    }

    public void gotMessageFrom(String client, String request, String address, int port) throws MalformedObjectNameException {
        log.debug("Message '{}' received from client {}", request, client);
        CommunicationPartnerMBean m = partnerDetails.get(client);
        boolean register = false;
        if (m == null) {
            cleanUp();
            m = new CommunicationPartnerMBean(client);
            m.setRemoteAddress(address);
            m.setRemotePort(port);
            register = true;
        }
        m.setLastSeen(new Date());
        m.setLastRequest(request);
        partnerDetails.put(client, m);
        if (register) {
            safeRegisterCommunicationPartner(m);
        }
    }

    public void didSendSegmentBytes(String client, int size) {
        log.debug("Segment with size {} sent to client {}", size, client);
        CommunicationPartnerMBean m = partnerDetails.get(client);
        m.onSegmentSent(size);
        partnerDetails.put(client, m);
    }

    public void didSendBinariesBytes(String client, long size) {
        log.debug("Binary with size {} sent to client {}", size, client);
        CommunicationPartnerMBean m = partnerDetails.get(client);
        m.onBinarySent(size);
        partnerDetails.put(client, m);
    }

    public String getID() {
        return id;
    }

    private void cleanUp() {
        while (partnerDetails.size() >= MAX_CLIENT_STATISTICS) {
            CommunicationPartnerMBean oldestEntry = oldestEntry();
            log.info("Housekeeping: Removing statistics for client " + oldestEntry.getName());
            safeUnregisterCommunicationPartner(oldestEntry);
            partnerDetails.remove(oldestEntry.getName());
        }
    }

    private CommunicationPartnerMBean oldestEntry() {
        CommunicationPartnerMBean ret = null;
        for (CommunicationPartnerMBean m : partnerDetails.values()) {
            if (ret == null || ret.getLastSeen().after(m.getLastSeen())) {
                ret = m;
            }
        }
        return ret;
    }

}
