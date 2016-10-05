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

package org.apache.jackrabbit.oak.segment.standby.client;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.net.ssl.SSLException;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.jmx.ClientStandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.store.CommunicationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StandbyClientSync implements ClientStandbyStatusMBean, Runnable, Closeable {

    public static final String CLIENT_ID_PROPERTY_NAME = "standbyID";

    private static final Logger log = LoggerFactory.getLogger(StandbyClientSync.class);

    private final String host;

    private final int port;

    private final int readTimeoutMs;

    private final boolean autoClean;

    private final CommunicationObserver observer;

    private final boolean secure;

    private boolean active = false;

    private int failedRequests;

    private long lastSuccessfulRequest;

    private volatile String state;

    private final Object sync = new Object();

    private final FileStore fileStore;

    private final AtomicBoolean running = new AtomicBoolean(true);

    private long syncStartTimestamp;

    private long syncEndTimestamp;

    public StandbyClientSync(String host, int port, FileStore store, boolean secure, int readTimeoutMs, boolean autoClean) throws SSLException {
        this.state = STATUS_INITIALIZING;
        this.lastSuccessfulRequest = -1;
        this.syncStartTimestamp = -1;
        this.syncEndTimestamp = -1;
        this.failedRequests = 0;
        this.host = host;
        this.port = port;
        this.secure = secure;
        this.readTimeoutMs = readTimeoutMs;
        this.autoClean = autoClean;
        this.fileStore = store;
        String s = System.getProperty(CLIENT_ID_PROPERTY_NAME);
        this.observer = new CommunicationObserver((s == null || s.length() == 0) ? UUID.randomUUID().toString() : s);

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.registerMBean(new StandardMBean(this, ClientStandbyStatusMBean.class), new ObjectName(this.getMBeanName()));
        } catch (Exception e) {
            log.error("can register standby status mbean", e);
        }
    }

    public String getMBeanName() {
        return StandbyStatusMBean.JMX_NAME + ",id=\"" + this.observer.getID() + "\"";
    }

    @Override
    public void close() {
        stop();
        state = STATUS_CLOSING;
        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.unregisterMBean(new ObjectName(this.getMBeanName()));
        } catch (Exception e) {
            log.error("can unregister standby status mbean", e);
        }
        observer.unregister();
        state = STATUS_CLOSED;
    }

    @Override
    public void run() {
        if (!isRunning()) {
            // manually stopped
            return;
        }

        state = STATUS_STARTING;

        synchronized (sync) {
            if (active) {
                return;
            }
            state = STATUS_RUNNING;
            active = true;
        }

        try {
            long startTimestamp = System.currentTimeMillis();
            try (StandbyClient client = new StandbyClient(observer.getID(), secure, readTimeoutMs)) {
                client.connect(host, port);

                long sizeBefore = fileStore.getStats().getApproximateSize();
                new StandbyClientSyncExecution(fileStore, client, newRunningSupplier()).execute();
                long sizeAfter = fileStore.getStats().getApproximateSize();

                if (autoClean && sizeAfter > 1.25 * sizeBefore) {
                    log.info("Store size increased from {} to {}, will run cleanup.", humanReadableByteCount(sizeBefore), humanReadableByteCount(sizeAfter));
                    cleanupAndRemove();
                }
            }
            this.failedRequests = 0;
            this.syncStartTimestamp = startTimestamp;
            this.syncEndTimestamp = System.currentTimeMillis();
            this.lastSuccessfulRequest = syncEndTimestamp / 1000;
        } catch (Exception e) {
            this.failedRequests++;
            log.error("Failed synchronizing state.", e);
        } finally {
            synchronized (this.sync) {
                this.active = false;
            }
        }
    }

    private void cleanupAndRemove() throws IOException {
        for (File file : fileStore.cleanup()) {
            log.info("Removing file {}", file);

            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                log.warn(String.format("Unable to remove file %s", file), e);
            }
        }
    }

    private Supplier<Boolean> newRunningSupplier() {
        return new Supplier<Boolean>() {

            @Override
            public Boolean get() {
                return running.get();
            }

        };
    }

    @Override
    public String getMode() {
        return "client: " + this.observer.getID();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void start() {
        running.set(true);
        state = STATUS_RUNNING;
    }

    @Override
    public void stop() {
        running.set(false);
        state = STATUS_STOPPED;
    }

    @Override
    public String getStatus() {
        return this.state;
    }

    @Override
    public int getFailedRequests() {
        return this.failedRequests;
    }

    @Override
    public int getSecondsSinceLastSuccess() {
        if (this.lastSuccessfulRequest < 0) {
            return -1;
        }
        return (int) (System.currentTimeMillis() / 1000 - this.lastSuccessfulRequest);
    }

    @Override
    public int calcFailedRequests() {
        return this.getFailedRequests();
    }

    @Override
    public int calcSecondsSinceLastSuccess() {
        return this.getSecondsSinceLastSuccess();
    }

    @Override
    public void cleanup() {
        try {
            cleanupAndRemove();
        } catch (IOException e) {
            log.error("Error while cleaning up", e);
        }
    }

    @Override
    public long getSyncStartTimestamp() {
        return syncStartTimestamp;
    }

    @Override
    public long getSyncEndTimestamp() {
        return syncEndTimestamp;
    }

}
