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

package org.apache.jackrabbit.oak.plugins.document;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;


/**
 * Checkpoints provide details around which revision are to be kept. These
 * are stored in Settings collection.
 */
class Checkpoints {
    private static final String ID = "checkpoint";

    /**
     * Property name to store all checkpoint data. The data is either stored as
     * Revision => expiryTime or Revision => JSON with expiryTime and info.
     */
    private static final String PROP_CHECKPOINT = "data";

    /**
     * Number of create calls after which old expired checkpoints entries would
     * be removed
     */
    static final int CLEANUP_INTERVAL = 100;

    private final DocumentNodeStore nodeStore;

    private final DocumentStore store;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomicInteger createCounter = new AtomicInteger();

    private final Object cleanupLock = new Object();

    Checkpoints(DocumentNodeStore store) {
        this.nodeStore = store;
        this.store = store.getDocumentStore();
        createIfNotExist();
    }

    public Revision create(long lifetimeInMillis, Map<String, String> info) {
        Revision r = nodeStore.getHeadRevision();
        createCounter.getAndIncrement();
        performCleanupIfRequired();
        UpdateOp op = new UpdateOp(ID, false);
        long endTime = BigInteger.valueOf(nodeStore.getClock().getTime())
                .add(BigInteger.valueOf(lifetimeInMillis))
                .min(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
        op.setMapEntry(PROP_CHECKPOINT, r, new Info(endTime, info).toString());
        store.createOrUpdate(Collection.SETTINGS, op);
        return r;
    }

    public void release(String checkpoint) {
        UpdateOp op = new UpdateOp(ID, false);
        op.removeMapEntry(PROP_CHECKPOINT, Revision.fromString(checkpoint));
        store.findAndUpdate(Collection.SETTINGS, op);
    }

    /**
     * Returns the oldest valid checkpoint registered.
     *
     * <p>It also performs cleanup of expired checkpoint
     *
     * @return oldest valid checkpoint registered. Might return null if no valid
     * checkpoint found
     */
    @SuppressWarnings("unchecked")
    @CheckForNull
    public Revision getOldestRevisionToKeep() {
        //Get uncached doc
        SortedMap<Revision, Info> checkpoints = getCheckpoints();

        if(checkpoints == null){
            log.debug("No checkpoint registered so far");
            return null;
        }

        final long currentTime = nodeStore.getClock().getTime();
        UpdateOp op = new UpdateOp(ID, false);
        Revision lastAliveRevision = null;
        long oldestExpiryTime = 0;

        for (Map.Entry<Revision, Info> e : checkpoints.entrySet()) {
            final long expiryTime = e.getValue().getExpiryTime();
            if (currentTime > expiryTime) {
                op.removeMapEntry(PROP_CHECKPOINT, e.getKey());
            } else if (expiryTime > oldestExpiryTime) {
                oldestExpiryTime = expiryTime;
                lastAliveRevision = e.getKey();
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.SETTINGS, op);
            log.debug("Purged {} expired checkpoints", op.getChanges().size());
        }

        return lastAliveRevision;
    }

    @SuppressWarnings("unchecked")
    @CheckForNull
    SortedMap<Revision, Info> getCheckpoints() {
        Document cdoc = store.find(Collection.SETTINGS, ID, 0);
        SortedMap<Revision, String> data =
                (SortedMap<Revision, String>) cdoc.get(PROP_CHECKPOINT);
        if (data == null) {
            return null;
        }
        SortedMap<Revision, Info> checkpoints = Maps.newTreeMap(data.comparator());
        for (Map.Entry<Revision, String> entry : data.entrySet()) {
            checkpoints.put(entry.getKey(), Info.fromString(entry.getValue()));
        }
        return checkpoints;
    }

    int size(){
        SortedMap<Revision, Info> checkpoints = getCheckpoints();
        return checkpoints == null ? 0 : checkpoints.size();
    }

    /**
     * Triggers collection of expired checkpoints createCounter exceeds certain size
     */
    private void performCleanupIfRequired() {
        if(createCounter.get() > CLEANUP_INTERVAL){
            synchronized (cleanupLock){
                getOldestRevisionToKeep();
                createCounter.set(0);
            }
        }
    }

    private void createIfNotExist() {
        if (store.find(Collection.SETTINGS, ID) == null) {
            UpdateOp updateOp = new UpdateOp(ID, true);
            updateOp.set(Document.ID, ID);
            store.createOrUpdate(Collection.SETTINGS, updateOp);
        }
    }

    static final class Info {

        private static final String EXPIRES = "expires";

        private final long expiryTime;

        private final Map<String, String> info;

        private Info(long expiryTime, Map<String, String> info) {
            this.expiryTime = expiryTime;
            this.info = Collections.unmodifiableMap(info);
        }

        static Info fromString(String info) {
            long expiryTime;
            Map<String, String> map;
            if (info.startsWith("{")) {
                map = Maps.newHashMap();
                JsopReader reader = new JsopTokenizer(info);
                reader.read('{');
                String key = reader.readString();
                if (!EXPIRES.equals(key)) {
                    throw new IllegalArgumentException("First entry in the " +
                            "checkpoint info must be the expires date: " + info);
                }
                reader.read(':');
                expiryTime = Long.parseLong(reader.readString());
                while (reader.matches(',')) {
                    key = reader.readString();
                    reader.read(':');
                    map.put(key, reader.readString());
                }
                reader.read('}');
                reader.read(JsopReader.END);
            } else {
                // old format
                map = Collections.emptyMap();
                expiryTime = Long.parseLong(info);
            }
            return new Info(expiryTime, map);
        }

        Map<String, String> get() {
            return info;
        }

        long getExpiryTime() {
            return expiryTime;
        }

        @Override
        public String toString() {
            JsopWriter writer = new JsopBuilder();
            writer.object();
            writer.key(EXPIRES).value(Long.toString(expiryTime));
            for (Map.Entry<String, String> entry : info.entrySet()) {
                writer.key(entry.getKey()).value(entry.getValue());
            }
            writer.endObject();
            return writer.toString();
        }
    }
}
