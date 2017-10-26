/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.segment.standby.store;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.management.MalformedObjectNameException;

import org.apache.jackrabbit.oak.segment.standby.jmx.ObservablePartnerMBean;

class CommunicationPartnerMBean implements ObservablePartnerMBean {

    private final String clientName;

    private final String remoteAddress;

    private final int remotePort;

    private final AtomicLong segmentsSent = new AtomicLong();

    private final AtomicLong segmentBytesSent = new AtomicLong();

    private final AtomicLong binariesSent = new AtomicLong();

    private final AtomicLong binariesBytesSent = new AtomicLong();

    private volatile String lastRequest;

    private volatile Date lastSeen;

    CommunicationPartnerMBean(String clientName, String remoteAddress, int remotePort) throws MalformedObjectNameException {
        this.clientName = clientName;
        this.remoteAddress = remoteAddress;
        this.remotePort = remotePort;
    }

    @Nonnull
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
        return this.lastSeen.toString();
    }

    @Override
    public long getTransferredSegments() {
        return this.segmentsSent.get();
    }

    @Override
    public long getTransferredSegmentBytes() {
        return this.segmentBytesSent.get();
    }

    @Override
    public long getTransferredBinaries() {
        return this.binariesSent.get();
    }

    @Override
    public long getTransferredBinariesBytes() {
        return this.binariesBytesSent.get();
    }

    void onMessageReceived(Date lastSeen, String lastRequest) {
        synchronized (this) {
            this.lastSeen = lastSeen;
            this.lastRequest = lastRequest;
        }
    }

    void onSegmentSent(long bytes) {
        this.segmentsSent.incrementAndGet();
        this.segmentBytesSent.addAndGet(bytes);
    }

    void onBinarySent(long bytes) {
        this.binariesSent.incrementAndGet();
        this.binariesBytesSent.addAndGet(bytes);
    }

    Date getLastSeen() {
        return this.lastSeen;
    }

}
