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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ServerHeartbeatSucceededEvent;

import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReplicaSetStatusTest {

    private List<ServerAddress> hosts = Arrays.asList(
            new ServerAddress("localhost", 27017),
            new ServerAddress("localhost", 27018),
            new ServerAddress("localhost", 27019)
    );

    private List<BsonValue> hostValues = hosts.stream()
            .map(sa -> new BsonString(sa.toString()))
            .collect(Collectors.toList());

    private List<ConnectionDescription> connections = Arrays.asList(
            new ConnectionDescription(new ServerId(new ClusterId(), hosts.get(0))),
            new ConnectionDescription(new ServerId(new ClusterId(), hosts.get(1))),
            new ConnectionDescription(new ServerId(new ClusterId(), hosts.get(2)))
    );

    @Test
    public void estimateLag() {
        ReplicaSetStatus status = new ReplicaSetStatus();
        assertEquals(ReplicaSetStatus.UNKNOWN_LAG, status.getLagEstimate());
        status.serverHeartbeatSucceeded(newEvent(0, 0, 1000));
        assertEquals(ReplicaSetStatus.UNKNOWN_LAG, status.getLagEstimate());
        status.serverHeartbeatSucceeded(newEvent(1, 0, 800));
        assertEquals(ReplicaSetStatus.UNKNOWN_LAG, status.getLagEstimate());
        status.serverHeartbeatSucceeded(newEvent(2, 50, 1000));
        // lastWrite difference: 200
        // localTime difference: 50
        // lag estimate: max(0, 200 - 50) = 150
        // avg lag estimate: avg(150, 0, 0) = 50
        assertEquals(50, status.getLagEstimate());

        status.serverHeartbeatSucceeded(newEvent(0, 5000, 4800));
        // lastWrite difference: 4000
        // localTime difference: 5000
        // lag estimate: max(0, 4000 - 5000) = 0
        // avg lag estimate: avg(0, 150, 0) = 50
        assertEquals(50, status.getLagEstimate());

        status.serverHeartbeatSucceeded(newEvent(1, 5050, 2000));
        // lastWrite difference: 3800
        // localTime difference: 5000
        // lag estimate: max(0, 3800 - 5000) = 0
        // avg lag estimate: avg(0, 0, 150) = 50
        assertEquals(50, status.getLagEstimate());

        status.serverHeartbeatSucceeded(newEvent(2, 5150, 5000));
        // lastWrite difference: 3000
        // localTime difference: 150
        // lag estimate: max(0, 3000 - 150) = 2850
        // avg lag estimate: avg(2850, 0, 0) = 950
        assertEquals(950, status.getLagEstimate());

        status.serverHeartbeatSucceeded(newEvent(0, 10010, 9000));
        // lastWrite difference: 7000
        // localTime difference: 4960
        // lag estimate: max(0, 7000 - 4960) = 2040
        // avg lag estimate: avg(2040, 2850, 0) = 1630
        assertEquals(1630, status.getLagEstimate());

    }

    private ServerHeartbeatSucceededEvent newEvent(int connectionIndex, long localTime, long lastWriteDate) {
        ConnectionDescription description = connections.get(connectionIndex);
        BsonDocument reply = new BsonDocument("localTime", new BsonDateTime(localTime));
        reply.put("hosts", new BsonArray(hostValues));
        BsonDocument lastWrite = new BsonDocument("lastWriteDate", new BsonDateTime(lastWriteDate));
        reply.put("lastWrite", lastWrite);
        return new ServerHeartbeatSucceededEvent(description.getConnectionId(), reply, 0);
    }
}
