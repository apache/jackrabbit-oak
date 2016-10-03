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

package org.apache.jackrabbit.oak.segment.standby.server;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.store.CommunicationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandbyServerSync implements StandbyStatusMBean, StateConsumer, StoreProvider, Closeable {

    private static final Logger log = LoggerFactory.getLogger(StandbyServer.class);

    private final FileStore fileStore;

    private final CommunicationObserver observer;

    private final int port;

    private final String[] allowedClientIPRanges;

    private final boolean secure;

    private volatile String state;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private StandbyServer server;

    public StandbyServerSync(final int port, final FileStore fileStore) {
        this(port, fileStore, null, false);
    }

    public StandbyServerSync(final int port, final FileStore fileStore, final boolean secure) {
        this(port, fileStore, null, secure);
    }

    public StandbyServerSync(final int port, final FileStore fileStore, final String[] allowedClientIPRanges) {
        this(port, fileStore, allowedClientIPRanges, false);
    }

    public StandbyServerSync(final int port, final FileStore fileStore, final String[] allowedClientIPRanges, final boolean secure) {
        this.port = port;
        this.fileStore = fileStore;
        this.allowedClientIPRanges = allowedClientIPRanges;
        this.secure = secure;
        this.observer = new CommunicationObserver("primary");

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();

        try {
            jmxServer.registerMBean(new StandardMBean(this, StandbyStatusMBean.class), new ObjectName(this.getMBeanName()));
        } catch (Exception e) {
            log.error("can't register standby status mbean", e);
        }
    }

    @Override
    public void consumeState(String state) {
        this.state = state;
    }

    @Override
    public FileStore provideStore() {
        return fileStore;
    }

    @Override
    public void start() {
        if (isRunning()) {
            return;
        }

        state = STATUS_STARTING;

        try {
            server = StandbyServer.builder(port, this)
                    .secure(secure)
                    .allowIPRanges(allowedClientIPRanges)
                    .withStateConsumer(this)
                    .withObserver(observer)
                    .build();
            server.start();

            state = STATUS_RUNNING;
            running.set(true);
        } catch (Exception e) {
            log.error("Server could not be started.", e);
            state = null;
            running.set(false);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop();
        }

        running.set(false);
        state = STATUS_STOPPED;
    }

    @Override
    public void close() {
        stop();
        state = STATUS_CLOSING;

        observer.unregister();
        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.unregisterMBean(new ObjectName(this.getMBeanName()));
        } catch (InstanceNotFoundException e) {
            // ignore
        } catch (Exception e) {
            log.error("can't unregister standby status mbean", e);
        }

        state = STATUS_CLOSED;
    }

    @Override
    public String getMode() {
        return "primary";
    }

    @Override
    public String getStatus() {
        return state == null ? STATUS_INITIALIZING : state;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    public String getMBeanName() {
        return StandbyStatusMBean.JMX_NAME + ",id=" + this.port;
    }

}
