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

import static com.google.common.collect.Sets.difference;
import static java.lang.Math.min;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.Revision.RevisionComparator;
import org.bson.BasicBSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;

public class ReplicaSetInfo implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSetInfo.class);

    private final Map<String, DBCollection> collections = new HashMap<String, DBCollection>();

    private final DB adminDb;

    private final String dbName;

    private final long pullFrequencyMillis;

    private final long maxReplicationLagMillis;

    private final RevisionComparator comparator;

    private final String credentials;

    Map<Integer, Revision> rootRevisions;

    Long minRootTimestamp;

    private volatile boolean stop;

    public ReplicaSetInfo(DB db, String credentials, int localClusterId, long maxReplicationLagMillis, long pullFrequencyMillis) {
        this.adminDb = db.getSisterDB("admin");
        this.dbName = db.getName();
        this.pullFrequencyMillis = pullFrequencyMillis;
        this.maxReplicationLagMillis = maxReplicationLagMillis;
        this.comparator = new RevisionComparator(localClusterId);
        this.credentials = credentials;
    }

    public synchronized boolean isSecondarySafe(long maxDocumentAge, long currentTime) {
        if (minRootTimestamp == null) {
            return maxReplicationLagMillis <= maxDocumentAge;
        } else if (currentTime < minRootTimestamp) {
            return false;
        } else {
            return min(currentTime - minRootTimestamp, maxReplicationLagMillis) <= maxDocumentAge;
        }
    }

    public boolean isSecondarySafe(NodeDocument doc, long currentTime) {
        if (doc == null) {
            return false;
        } else if (!doc.hasBeenModifiedSince(currentTime - maxReplicationLagMillis)) {
            // If parent has been modified loooong time back then there children
            // would also have not be modified. In that case we can read from secondary.
            return true;
        } else {
            return isSecondarySafe(doc.getLastRev());
        }
    }

    boolean isSecondarySafe(Map<Integer, Revision> lastRev) {
        Map<Integer, Revision> revisions;
        synchronized (this) {
            revisions = rootRevisions;
        }
        if (revisions == null) {
            return false;
        }

        boolean result = true;
        for (Revision r : lastRev.values()) {
            int clusterId = r.getClusterId();
            if (r.isBranch()) {
                result = false;
                break;
            } else if (!revisions.containsKey(clusterId)) {
                result = false;
                break;
            } else if (comparator.compare(r, revisions.get(clusterId)) > 0) {
                result = false;
                break;
            }
        }
        return result;
    }

    @Nullable
    public synchronized Map<Integer, Revision> getMinimumRootRevisions() {
        return rootRevisions;
    }

    public synchronized void stop() {
        stop = true;
        notify();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        while (!stop) {
            CommandResult result = adminDb.command("replSetGetStatus", ReadPreference.primary());
            Iterable<BasicBSONObject> members = (Iterable<BasicBSONObject>) result.get("members");
            if (members == null) {
                members = Collections.emptyList();
            }
            updateRevisions(members);
            synchronized (this) {
                try {
                    if (!stop) {
                        wait(pullFrequencyMillis);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        closeConnections(collections.keySet());
        collections.clear();
    }

    void updateRevisions(Iterable<BasicBSONObject> members) {
        Set<String> secondaries = new HashSet<String>();
        boolean unknownState = false;
        for (BasicBSONObject member : members) {
            ReplicaSetMemberState state = ReplicaSetMemberState.valueOf(member.getString("stateStr"));
            String name = member.getString("name");

            switch (state) {
            case PRIMARY:
                continue;

            case SECONDARY:
                secondaries.add(name);
                break;

            case ARBITER:
                continue;

            default:
                LOG.debug("Invalid state {} for instance {}", state, name);
                unknownState = true;
                break;
            }
        }

        if (secondaries.isEmpty()) {
            unknownState = true;
        }

        if (!unknownState) {
            Map<Integer, Revision> minRevisions = getMinimumRootRevisions(secondaries);
            if (minRevisions == null) {
                unknownState = true;
            } else {
                Long minTimestamp = null;
                for (Revision r : minRevisions.values()) {
                    long timestamp = r.getTimestamp();
                    if (minTimestamp == null || minTimestamp > timestamp) {
                        minTimestamp = timestamp;
                    }
                }
                synchronized (this) {
                    rootRevisions = minRevisions;
                    minRootTimestamp = minTimestamp;
                }
            }
        }
        if (unknownState) {
            synchronized (this) {
                rootRevisions = null;
                minRootTimestamp = null;
            }
        }

        closeConnections(difference(collections.keySet(), secondaries));
    }

    private Map<Integer, Revision> getMinimumRootRevisions(Set<String> secondaries) {
        Map<Integer, Revision> minimumRevisions = new HashMap<Integer, Revision>();
        for (String name : secondaries) {
            try {
                for (Revision r : getRootRevisions(name)) {
                    int clusterId = r.getClusterId();
                    if (minimumRevisions.containsKey(clusterId)) {
                        if (comparator.compare(r, minimumRevisions.get(clusterId)) < 0) {
                            minimumRevisions.put(clusterId, r);
                        }
                    } else {
                        minimumRevisions.put(clusterId, r);
                    }
                }
            } catch (UnknownHostException e) {
                LOG.error("Can't connect to {}", name, e);
                return null;
            }
        }
        return minimumRevisions;
    }

    protected List<Revision> getRootRevisions(String hostName) throws UnknownHostException {
        List<Revision> revisions = new ArrayList<Revision>();
        DBCollection collection = getNodeCollection(hostName);
        DBObject root = collection.findOne(new BasicDBObject(Document.ID, "0:/"));
        DBObject lastRev = (DBObject) root.get("_lastRev");
        for (String clusterId : lastRev.keySet()) {
            String rev = (String) lastRev.get(clusterId);
            revisions.add(Revision.fromString(rev));
        }
        return revisions;
    }

    private void closeConnections(Set<String> hostNames) {
        Iterator<String> it = collections.keySet().iterator();
        while (it.hasNext()) {
            String hostName = it.next();
            if (!hostNames.contains(hostName)) {
                try {
                    collections.remove(hostName).getDB().getMongo().close();
                } catch (MongoClientException e) {
                    LOG.error("Can't close Mongo client", e);
                }
            }
        }
    }

    private DBCollection getNodeCollection(String hostName) throws UnknownHostException {
        if (collections.containsKey(hostName)) {
            return collections.get(hostName);
        }

        StringBuilder uriBuilder = new StringBuilder("mongodb://");
        if (credentials != null) {
            uriBuilder.append(credentials).append('@');
        }
        uriBuilder.append(hostName);

        MongoClientURI uri = new MongoClientURI(uriBuilder.toString());
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(dbName);
        DBCollection collection = db.getCollection(Collection.NODES.toString());
        collections.put(hostName, collection);
        return collection;
    }

    enum ReplicaSetMemberState {
        STARTUP, PRIMARY, SECONDARY, RECOVERING, STARTUP2, UNKNOWN, ARBITER, DOWN, ROLLBACK, REMOVED
    }

}