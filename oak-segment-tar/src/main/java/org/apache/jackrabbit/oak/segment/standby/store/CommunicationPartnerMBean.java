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

import javax.annotation.Nonnull;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.jackrabbit.oak.segment.standby.jmx.ObservablePartnerMBean;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;

class CommunicationPartnerMBean implements ObservablePartnerMBean {

    private final ObjectName mbeanName;

    private final String clientName;

    private String lastRequest;

    private Date lastSeen;

    private String lastSeenTimestamp;

    private String remoteAddress;

    private int remotePort;

    private long segmentsSent;

    private long segmentBytesSent;

    private long binariesSent;

    private long binariesBytesSent;

    CommunicationPartnerMBean(String clientName) throws MalformedObjectNameException {
        this.clientName = clientName;
        this.mbeanName = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=\"Client " + clientName + "\"");
    }

    ObjectName getMBeanName() {
        return this.mbeanName;
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
        return this.lastSeenTimestamp;
    }

    @Override
    public long getTransferredSegments() {
        return this.segmentsSent;
    }

    @Override
    public long getTransferredSegmentBytes() {
        return this.segmentBytesSent;
    }

    @Override
    public long getTransferredBinaries() {
        return this.binariesSent;
    }

    @Override
    public long getTransferredBinariesBytes() {
        return this.binariesBytesSent;
    }

    void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    void setRemotePort(int remotePort) {
        this.remotePort = remotePort;
    }

    Date getLastSeen() {
        return lastSeen;
    }

    void setLastSeen(Date lastSeen) {
        this.lastSeen = lastSeen;
        this.lastSeenTimestamp = lastSeen.toString();
    }

    void setLastRequest(String lastRequest) {
        this.lastRequest = lastRequest;
    }

    void onSegmentSent(long bytes) {
        segmentsSent++;
        segmentBytesSent += bytes;
    }

    void onBinarySent(long bytes) {
        binariesSent++;
        binariesBytesSent += bytes;
    }

}
