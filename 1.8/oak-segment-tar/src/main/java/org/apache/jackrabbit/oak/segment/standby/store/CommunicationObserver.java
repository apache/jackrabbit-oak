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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import io.netty.handler.codec.marshalling.DefaultMarshallerProvider;
import org.apache.jackrabbit.oak.segment.standby.jmx.ObservablePartnerMBean;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunicationObserver {

    private static final int DEFAULT_MAX_CLIENT_MBEANS = 10;

    private static final Logger log = LoggerFactory.getLogger(CommunicationObserver.class);

    private static ObjectName getMBeanName(CommunicationPartnerMBean bean) throws MalformedObjectNameException {
        return new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=\"Client " + bean.getName() + "\"");
    }

    private static String oldest(Map<String, CommunicationPartnerMBean> beans) {
        CommunicationPartnerMBean oldest = null;

        for (CommunicationPartnerMBean bean : beans.values()) {
            if (oldest == null || oldest.getLastSeen().after(bean.getLastSeen())) {
                oldest = bean;
            }
        }

        if (oldest == null) {
            throw new IllegalArgumentException("no clients available");
        }

        return oldest.getName();
    }

    private final Map<String, CommunicationPartnerMBean> beans = new HashMap<>();

    private final int maxClientMBeans;

    private final String id;

    public CommunicationObserver(String id) {
        this(id, DEFAULT_MAX_CLIENT_MBEANS);
    }

    CommunicationObserver(String id, int maxClientMBeans) {
        this.id = id;
        this.maxClientMBeans = maxClientMBeans;
    }

    void registerCommunicationPartner(CommunicationPartnerMBean m) throws Exception {
        ManagementFactory.getPlatformMBeanServer().registerMBean(new StandardMBean(m, ObservablePartnerMBean.class), getMBeanName(m));
    }

    private void safeRegisterCommunicationPartner(CommunicationPartnerMBean m) {
        try {
            registerCommunicationPartner(m);
        } catch (Exception e) {
            log.error(String.format("Unable to register MBean for client %s", m.getName()), e);
        }
    }

    void unregisterCommunicationPartner(CommunicationPartnerMBean m) throws Exception {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(getMBeanName(m));
    }

    private void safeUnregisterCommunicationPartner(CommunicationPartnerMBean m) {
        try {
            unregisterCommunicationPartner(m);
        } catch (Exception e) {
            log.error(String.format("Unable to unregister MBean for client %s", m.getName()), e);
        }
    }

    public void unregister() {
        Collection<CommunicationPartnerMBean> unregister;

        synchronized (beans) {
            unregister = new ArrayList<>(beans.values());
            beans.clear();
        }

        for (CommunicationPartnerMBean bean : unregister) {
            safeUnregisterCommunicationPartner(bean);
        }
    }

    public void gotMessageFrom(String client, String request, String address, int port) throws MalformedObjectNameException {
        log.debug("Message '{}' received from client {}", request, client);
        createOrUpdateClientMBean(client, address, port, m -> m.onMessageReceived(new Date(), request));
    }

    public void didSendSegmentBytes(String client, int size) {
        log.debug("Segment with size {} sent to client {}", size, client);
        updateClientMBean(client, m -> m.onSegmentSent(size));
    }

    public void didSendBinariesBytes(String client, long size) {
        log.debug("Binary with size {} sent to client {}", size, client);
        updateClientMBean(client, m -> m.onBinarySent(size));
    }

    public String getID() {
        return id;
    }

    private void createOrUpdateClientMBean(String clientName, String remoteAddress, int remotePort, Consumer<CommunicationPartnerMBean> update) throws MalformedObjectNameException {
        List<CommunicationPartnerMBean> unregister = null;
        boolean register = false;
        CommunicationPartnerMBean bean;

        synchronized (beans) {
            bean = beans.get(clientName);

            if (bean == null) {
                bean = new CommunicationPartnerMBean(clientName, remoteAddress, remotePort);

                while (beans.size() >= maxClientMBeans) {
                    if (unregister == null) {
                        unregister = new ArrayList<>();
                    }
                    unregister.add(beans.remove(oldest(beans)));
                }

                beans.put(clientName, bean);

                register = true;
            }
        }

        update.accept(bean);

        if (register) {
            safeRegisterCommunicationPartner(bean);
        }

        if (unregister != null) {
            for (CommunicationPartnerMBean c : unregister) {
                safeUnregisterCommunicationPartner(c);
            }
        }
    }

    private void updateClientMBean(String id, Consumer<CommunicationPartnerMBean> update) {
        CommunicationPartnerMBean c;

        synchronized (beans) {
            c = beans.get(id);
        }

        if (c == null) {
            throw new IllegalStateException("no client found with id " + id);
        }

        update.accept(c);
    }

}
