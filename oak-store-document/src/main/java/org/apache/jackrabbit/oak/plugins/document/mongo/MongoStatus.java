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

import com.mongodb.BasicDBObject;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.session.ClientSession;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class MongoStatus implements ServerMonitorListener {

    private static final Logger LOG = LoggerFactory.getLogger(MongoStatus.class);

    private static final Set<String> SERVER_DETAIL_FIELD_NAMES = Set.of("host", "process", "connections", "repl", "storageEngine", "mem");

    private final MongoClient client;

    private final String dbName;

    private BasicDBObject serverStatus;

    private BasicDBObject buildInfo;

    private String version;

    private Boolean majorityReadConcernSupported;

    private Boolean majorityReadConcernEnabled;

    private Boolean clientSessionSupported;

    private final ReplicaSetStatus replicaSetStatus = new ReplicaSetStatus();

    public MongoStatus(@NotNull MongoClient client,
                       @NotNull String dbName) {
        this.client = client;
        this.dbName = dbName;
    }

    public void checkVersion() {
        if (!isVersion(2, 6)) {
            String msg = "MongoDB version 2.6.0 or higher required. " +
                    "Currently connected to a MongoDB with version: " + version;
            throw new RuntimeException(msg);
        }
    }

    /**
     * Check if the majority read concern is supported by this storage engine.
     * The fact that read concern is supported doesn't it can be used - it also
     * has to be enabled.
     *
     * @return true if the majority read concern is supported
     */
    public boolean isMajorityReadConcernSupported() {
        if (majorityReadConcernSupported == null) {
            BasicDBObject stat = getServerStatus();
            if (stat.isEmpty()) {
                LOG.debug("User doesn't have privileges to get server status; falling back to the isMajorityReadConcernEnabled()");
                return isMajorityReadConcernEnabled();
            } else {
                if (stat.containsField("storageEngine")) {
                    BasicDBObject storageEngine = (BasicDBObject) stat.get("storageEngine");
                    majorityReadConcernSupported = storageEngine.getBoolean("supportsCommittedReads");
                } else {
                    majorityReadConcernSupported = false;
                }
            }
        }
        return majorityReadConcernSupported;
    }

    /**
     * Check if the majority read concern is enabled and can be used for queries.
     *
     * @return true if the majority read concern is enabled
     */
    public boolean isMajorityReadConcernEnabled() {
        if (majorityReadConcernEnabled == null) {
            // Mongo API doesn't seem to provide an option to check whether the
            // majority read concern has been enabled, so we have to try to use
            // it and optionally catch the exception.
            MongoCollection<?> emptyCollection = client.getDatabase(dbName)
                    .getCollection("emptyCollection-" + System.currentTimeMillis());
            try (MongoCursor cursor = emptyCollection
                    .withReadConcern(ReadConcern.MAJORITY)
                    .find(new BasicDBObject()).iterator()) {
                cursor.hasNext();
                majorityReadConcernEnabled = true;
            } catch (MongoQueryException | IllegalArgumentException e) {
                majorityReadConcernEnabled = false;
            }
        }
        return majorityReadConcernEnabled;
    }

    @NotNull
    public String getServerDetails() {
        Map<String, Object> details = new HashMap<>();
        for (String key : SERVER_DETAIL_FIELD_NAMES) {
            Object value = getServerStatus().get(key);
            if (value != null) {
                details.put(key, value);
            }
        }
        return details.toString();
    }

    @NotNull
    public String getVersion() {
        if (version == null) {
            String v = getServerStatus().getString("version");
            if (v == null) {
                // OAK-4841: serverStatus was probably unauthorized,
                // use buildInfo command to get version
                v = getBuildInfo().getString("version");
            }
            version = v;
        }
        return version;
    }

    boolean isVersion(int requiredMajor, int requiredMinor) {
        String v = getVersion();
        Matcher m = Pattern.compile("^(\\d+)\\.(\\d+)\\..*").matcher(v);
        if (!m.matches()) {
            throw new IllegalArgumentException("Malformed MongoDB version: " + v);
        }
        int major = Integer.parseInt(m.group(1));
        int minor = Integer.parseInt(m.group(2));

        if (major > requiredMajor) {
            return true;
        } else if (major == requiredMajor) {
            return minor >= requiredMinor;
        } else {
            return false;
        }
    }

    /**
     * @return {@code true} if client sessions are supported.
     */
    boolean isClientSessionSupported() {
        if (clientSessionSupported == null) {
            // must be at least 3.6
            if (isVersion(3, 6)) {
                ClientSessionOptions options = ClientSessionOptions.builder()
                        .causallyConsistent(true).build();
                try (ClientSession ignored = client.startSession(options)) {
                    clientSessionSupported = true;
                } catch (MongoClientException e) {
                    clientSessionSupported = false;
                }
            } else {
                clientSessionSupported = false;
            }
        }
        return clientSessionSupported;
    }

    /**
     * Returns an estimate of the replica-set lag in milliseconds. The returned
     * value is not an accurate measurement of the replication lag and should
     * only be used as a rough estimate to decide whether secondaries can be
     * used for queries in general. 
     * <p>
     * This method may return {@link ReplicaSetStatus#UNKNOWN_LAG} if the value
     * is currently unknown.
     *
     * @return an estimate of the
     */
    long getReplicaSetLagEstimate() {
        return replicaSetStatus.getLagEstimate();
    }

    //------------------------< ServerMonitorListener >-------------------------

    @Override
    public void serverHearbeatStarted(ServerHeartbeatStartedEvent event) {
        LOG.debug("serverHeartbeatStarted {}", event.getConnectionId());
        replicaSetStatus.serverHearbeatStarted(event);
    }

    @Override
    public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event) {
        LOG.debug("serverHeartbeatSucceeded {}, {}", event.getConnectionId(), event.getReply());
        replicaSetStatus.serverHeartbeatSucceeded(event);
    }

    @Override
    public void serverHeartbeatFailed(ServerHeartbeatFailedEvent event) {
        LOG.debug("serverHeartbeatFailed {} ({} ms)", event.getConnectionId(),
                event.getElapsedTime(TimeUnit.MILLISECONDS),
                event.getThrowable());
        replicaSetStatus.serverHeartbeatFailed(event);
    }

    //-------------------------------< internal >-------------------------------

    private BasicDBObject getServerStatus() {
        if (serverStatus == null) {
            try {
                serverStatus = client.getDatabase(dbName).runCommand(
                        new BasicDBObject("serverStatus", 1), BasicDBObject.class);
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == -1) {
                    // OAK-7485: workaround when running on
                    // MongoDB Atlas shared instances
                    serverStatus = new BasicDBObject();
                } else if (e.getErrorCode() == 13) {
                    // "Unauthorized"
                    // User is not authorized to run the
                    // serverStatus command (OAK-8122).
                    serverStatus = new BasicDBObject();
                } else {
                    throw e;
                }
            }
        }
        return serverStatus;
    }

    private BasicDBObject getBuildInfo() {
        if (buildInfo == null) {
            buildInfo = client.getDatabase(dbName).runCommand(
                    new BasicDBObject("buildInfo", 1), BasicDBObject.class);
        }
        return buildInfo;
    }

    // for testing purposes
    void setVersion(String version) {
        this.version = version;
    }

    void setServerStatus(BasicDBObject serverStatus) {
        this.majorityReadConcernSupported = null;
        this.serverStatus = serverStatus;
    }
}
