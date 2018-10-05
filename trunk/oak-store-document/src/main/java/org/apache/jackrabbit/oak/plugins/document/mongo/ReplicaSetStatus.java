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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.mongodb.ServerAddress;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListenerAdapter;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of the status of a replica set based on information provided
 * by heartbeat events. This status provides a replica set lag estimate, which
 * can be used to decide whether secondaries are sufficiently up-to-date and
 * read operations can be sent to a secondary. This is particularly useful when
 * causal consistent client sessions are used with the MongoDB Java driver. Read
 * operations shouldn't be sent to a secondary when it lags too much behind,
 * otherwise the read operation will block until it was able to catch up.
 */
public class ReplicaSetStatus extends ServerMonitorListenerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetStatus.class);

    public static final long UNKNOWN_LAG = Long.MAX_VALUE;

    /**
     * Most recent heartbeats from connected servers
     */
    private final Map<ServerAddress, Heartbeat> heartbeats = new HashMap<>();

    private final Set<ServerAddress> members = new HashSet<>();

    private final Deque<Long> estimatesPerMember = new LinkedList<>();

    private long lagEstimate = UNKNOWN_LAG;

    @Override
    public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event) {
        synchronized (heartbeats) {
            ServerAddress address = event.getConnectionId().getServerId().getAddress();
            Heartbeat beat = new Heartbeat(event);
            heartbeats.put(address, beat);
            members.addAll(beat.getHosts());
            if (!members.isEmpty()) {
                updateLag();
            }
        }
    }

    public long getLagEstimate() {
        return lagEstimate;
    }

    private void updateLag() {
        if (!heartbeats.keySet().containsAll(members)) {
            lagEstimate = UNKNOWN_LAG;
            return;
        }

        long oldestUpdate = Long.MAX_VALUE;
        long newestUpdate = Long.MIN_VALUE;
        long oldestWrite = Long.MAX_VALUE;
        long newestWrite = Long.MIN_VALUE;
        for (Map.Entry<ServerAddress, Heartbeat> entry : heartbeats.entrySet()) {
            if (!members.contains(entry.getKey())) {
                continue;
            }
            Heartbeat beat = entry.getValue();
            Date lastWrite = beat.getLastWrite();
            if (lastWrite == null) {
                oldestWrite = 0;
                newestWrite = Long.MAX_VALUE;
            } else {
                oldestWrite = Math.min(oldestWrite, lastWrite.getTime());
                newestWrite = Math.max(newestWrite, lastWrite.getTime());
            }
            long updateTime = beat.getTime();
            oldestUpdate = Math.min(oldestUpdate, updateTime);
            newestUpdate = Math.max(newestUpdate, updateTime);
        }
        // heartbeats happen concurrently for all servers. It may happen we
        // have some fresh and some stale heartbeats with update times up to
        // heartbeatFreqMillis apart
        long uncertaintyMillis = newestUpdate - oldestUpdate;
        estimatesPerMember.addFirst(Math.max(0, newestWrite - oldestWrite - uncertaintyMillis));

        // average estimates over up to number of members and remove old value
        long estimate = 0;
        int i = 0;
        for (Iterator<Long> it = estimatesPerMember.iterator(); it.hasNext(); ) {
            long v = it.next();
            if (i++ < members.size()) {
                estimate += v;
            } else {
                it.remove();
            }
        }
        lagEstimate = estimate / members.size();
        LOG.debug("lagEstimate: {} ms ({})", lagEstimate, estimatesPerMember);
    }

    private static class Heartbeat {

        private final List<ServerAddress> hosts;

        private final Date lastWrite;

        private final long localTime;

        Heartbeat(ServerHeartbeatSucceededEvent event) {
            this.hosts = hostsFrom(event);
            this.lastWrite = lastWriteFrom(event);
            this.localTime = localTimeFrom(event).getTime();
        }

        Collection<ServerAddress> getHosts() {
            return hosts;
        }

        long getTime() {
            return localTime;
        }

        @Nullable
        Date getLastWrite() {
            return lastWrite;
        }

    }

    private static List<ServerAddress> hostsFrom(ServerHeartbeatSucceededEvent event) {
        return event.getReply().getArray("hosts", new BsonArray()).stream()
                .map(bsonValue -> new ServerAddress(bsonValue.asString().getValue()))
                .collect(Collectors.toList());
    }

    private static Date localTimeFrom(ServerHeartbeatSucceededEvent event) {
        BsonDocument reply = event.getReply();
        return new Date(reply.getDateTime("localTime").getValue());
    }

    private static Date lastWriteFrom(ServerHeartbeatSucceededEvent event) {
        BsonDocument reply = event.getReply();
        if (!reply.containsKey("lastWrite")) {
            return null;
        }
        return new Date(reply.getDocument("lastWrite")
                .getDateTime("lastWriteDate").getValue());
    }
}
