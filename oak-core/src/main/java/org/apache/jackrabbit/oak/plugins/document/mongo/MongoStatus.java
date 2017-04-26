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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoQueryException;
import com.mongodb.ReadConcern;
import com.mongodb.client.model.DBCollectionFindOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoStatus {

    private static final Logger LOG = LoggerFactory.getLogger(MongoStatus.class);

    private static final ImmutableSet<String> SERVER_DETAIL_FIELD_NAMES
            = ImmutableSet.<String>builder()
            .add("host", "process", "connections", "repl", "storageEngine", "mem")
            .build();

    private final DB db;

    private BasicDBObject serverStatus;

    private BasicDBObject buildInfo;

    private String version;

    private Boolean majorityReadConcernSupported;

    private Boolean majorityReadConcernEnabled;

    public MongoStatus(@Nonnull DB db) {
        this.db = db;
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
            DBCollection emptyCollection = db.getCollection("emptyCollection-" + System.currentTimeMillis());
            DBCursor cursor = emptyCollection.find(new BasicDBObject(), new DBCollectionFindOptions().readConcern(ReadConcern.MAJORITY));
            try {
                cursor.hasNext();
                majorityReadConcernEnabled = true;
            } catch (MongoQueryException | IllegalArgumentException e) {
                majorityReadConcernEnabled = false;
            } finally {
                cursor.close();
            }
        }
        return majorityReadConcernEnabled;
    }

    @Nonnull
    public String getServerDetails() {
        Map<String, Object> details = Maps.newHashMap();
        for (String key : SERVER_DETAIL_FIELD_NAMES) {
            Object value = getServerStatus().get(key);
            if (value != null) {
                details.put(key, value);
            }
        }
        return details.toString();
    }

    @Nonnull
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

    private BasicDBObject getServerStatus() {
        if (serverStatus == null) {
            serverStatus = db.command("serverStatus");
        }
        return serverStatus;
    }

    private BasicDBObject getBuildInfo() {
        if (buildInfo == null) {
            buildInfo = db.command("buildInfo");
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
