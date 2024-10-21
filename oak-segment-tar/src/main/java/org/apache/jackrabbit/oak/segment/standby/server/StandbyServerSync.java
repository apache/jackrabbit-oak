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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandbyServerSync implements StandbyStatusMBean, StateConsumer, StoreProvider, Closeable {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private int port;

        private FileStore fileStore;

        private int blobChunkSize;

        private boolean secure;

        private String[] allowedClientIPRanges;

        private StandbyBlobReader standbyBlobReader;

        private StandbyHeadReader standbyHeadReader;

        private StandbyReferencesReader standbyReferencesReader;

        private StandbySegmentReader standbySegmentReader;

        private String sslKeyFile;

        private String sslChainFile;

        private boolean sslValidateClient;

        private String sslKeyPassword;

        private String sslSubjectPattern;

        private Builder() {
            // Prevent external instantiation
        }

        public Builder withPort(int port) {
            checkArgument(port > 0, "port");
            this.port = port;
            return this;
        }

        public Builder withFileStore(FileStore fileStore) {
            checkArgument(fileStore != null, "fileStore");
            this.fileStore = fileStore;
            return this;
        }

        public Builder withBlobChunkSize(int blobChunkSize) {
            checkArgument(blobChunkSize > 0, "blobChunkSize");
            this.blobChunkSize = blobChunkSize;
            return this;
        }

        public Builder withSecureConnection(boolean secure) {
            this.secure = secure;
            return this;
        }

        public Builder withAllowedClientIPRanges(String[] allowedClientIPRanges) {
            this.allowedClientIPRanges = allowedClientIPRanges;
            return this;
        }

        Builder withStandbyBlobReader(StandbyBlobReader standbyBlobReader) {
            checkArgument(standbyBlobReader != null, "standbyBlobReader");
            this.standbyBlobReader = standbyBlobReader;
            return this;
        }

        Builder withStandbyHeadReader(StandbyHeadReader standbyHeadReader) {
            checkArgument(standbyHeadReader != null, "standbyHeadReader");
            this.standbyHeadReader = standbyHeadReader;
            return this;
        }

        Builder withStandbyReferencesReader(StandbyReferencesReader standbyReferencesReader) {
            checkArgument(standbyReferencesReader != null, "standbyReferencesReader");
            this.standbyReferencesReader = standbyReferencesReader;
            return this;
        }

        Builder withStandbySegmentReader(StandbySegmentReader standbySegmentReader) {
            checkArgument(standbySegmentReader != null, "standbySegmentReader");
            this.standbySegmentReader = standbySegmentReader;
            return this;
        }

        public Builder withSSLKeyFile(String sslKeyFile) {
            this.sslKeyFile = sslKeyFile;
            return this;
        }

        public Builder withSSLKeyPassword(String sslKeyPassword) {
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        public Builder withSSLChainFile(String sslChainFile) {
            this.sslChainFile = sslChainFile;
            return this;
        }

        public Builder withSSLClientValidation(boolean sslValidateClient) {
            this.sslValidateClient = sslValidateClient;
            return this;
        }

        public Builder withSSLSubjectPattern(String sslSubjectPattern) {
            this.sslSubjectPattern = sslSubjectPattern;
            return this;
        }

        public StandbyServerSync build() {
            checkArgument(port > 0);
            checkArgument(fileStore != null);
            checkArgument(blobChunkSize > 0);
            return new StandbyServerSync(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StandbyServer.class);

    private final FileStore fileStore;

    private final CommunicationObserver observer;

    private final int port;

    private final String[] allowedClientIPRanges;

    private final boolean secure;
    
    private final int blobChunkSize;

    private volatile String state;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final StandbyBlobReader standbyBlobReader;

    private final StandbyHeadReader standbyHeadReader;

    private final StandbyReferencesReader standbyReferencesReader;

    private final StandbySegmentReader standbySegmentReader;

    private StandbyServer server;

    private final String sslKeyFile;

    private final String sslKeyPassword;

    private final String sslChainFile;

    private final boolean sslValidateClient;

    private final String sslSubjectPattern;

    private StandbyServerSync(Builder builder) {
        this.port = builder.port;
        this.fileStore = builder.fileStore;
        this.blobChunkSize = builder.blobChunkSize;
        this.allowedClientIPRanges = builder.allowedClientIPRanges;
        this.secure = builder.secure;
        this.standbyBlobReader = builder.standbyBlobReader;
        this.standbyHeadReader = builder.standbyHeadReader;
        this.standbyReferencesReader = builder.standbyReferencesReader;
        this.standbySegmentReader = builder.standbySegmentReader;
        this.sslKeyFile = builder.sslKeyFile;
        this.sslKeyPassword = builder.sslKeyPassword;
        this.sslChainFile = builder.sslChainFile;
        this.sslValidateClient = builder.sslValidateClient;
        this.sslSubjectPattern = builder.sslSubjectPattern;
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
            StandbyServer.Builder builder = StandbyServer.builder(port, this, blobChunkSize)
                .secure(secure)
                .allowIPRanges(allowedClientIPRanges)
                .withStateConsumer(this)
                .withObserver(observer)
                .withStandbyBlobReader(standbyBlobReader)
                .withStandbyHeadReader(standbyHeadReader)
                .withStandbyReferencesReader(standbyReferencesReader)
                .withStandbySegmentReader(standbySegmentReader)
                .withSSLKeyFile(sslKeyFile)
                .withSSLKeyPassword(sslKeyPassword)
                .withSSLChainFile(sslChainFile)
                .withSSLClientValidation(sslValidateClient)
                .withSSLSubjectPattern(sslSubjectPattern);

            server = builder.build();
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
        if (server != null) {
            server.close();
        }
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

    @NotNull
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
