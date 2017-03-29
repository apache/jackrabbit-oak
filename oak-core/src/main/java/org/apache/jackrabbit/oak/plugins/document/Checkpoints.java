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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Checkpoints provide details around which revision are to be kept. These
 * are stored in Settings collection.
 */
class Checkpoints {

    private static final Logger LOG = LoggerFactory.getLogger(Checkpoints.class);

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
        // create a unique dummy commit we can use as checkpoint revision
        Revision r = nodeStore.commitQueue.createRevision();
        final RevisionVector[] rv = new RevisionVector[1];
        nodeStore.commitQueue.done(r, new CommitQueue.Callback() {
            @Override
            public void headOfQueue(@Nonnull Revision revision) {
                rv[0] = nodeStore.getHeadRevision();
            }
        });
        createCounter.getAndIncrement();
        performCleanupIfRequired();
        UpdateOp op = new UpdateOp(ID, false);
        long endTime = BigInteger.valueOf(nodeStore.getClock().getTime())
                .add(BigInteger.valueOf(lifetimeInMillis))
                .min(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
        op.setMapEntry(PROP_CHECKPOINT, r, new Info(endTime, rv[0], info).toString());
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

        if(checkpoints.isEmpty()){
            log.debug("No checkpoint registered so far");
            return null;
        }

        final long currentTime = nodeStore.getClock().getTime();
        UpdateOp op = new UpdateOp(ID, false);
        Revision lastAliveRevision = null;

        for (Map.Entry<Revision, Info> e : checkpoints.entrySet()) {
            long expiryTime = e.getValue().getExpiryTime();
            if (currentTime > expiryTime) {
                op.removeMapEntry(PROP_CHECKPOINT, e.getKey());
            } else {
                Revision cpRev = e.getKey();
                RevisionVector rv = e.getValue().getCheckpoint();
                if (rv != null) {
                    cpRev = rv.getRevision(cpRev.getClusterId());
                }
                lastAliveRevision = Utils.min(lastAliveRevision, cpRev);
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.SETTINGS, op);
            log.debug("Purged {} expired checkpoints", op.getChanges().size());
        }

        return lastAliveRevision;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    SortedMap<Revision, Info> getCheckpoints() {
        Document cdoc = store.find(Collection.SETTINGS, ID, 0);
        SortedMap<Revision, String> data = null;
        if (cdoc != null) {
            data = (SortedMap<Revision, String>) cdoc.get(PROP_CHECKPOINT);
        }
        SortedMap<Revision, Info> checkpoints = Maps.newTreeMap(StableRevisionComparator.REVERSE);
        if (data != null) {
            for (Map.Entry<Revision, String> entry : data.entrySet()) {
                checkpoints.put(entry.getKey(), Info.fromString(entry.getValue()));
            }
        }
        return checkpoints;
    }

    /**
     * Retrieves the head revision for the given {@code checkpoint}.
     *
     * @param checkpoint the checkpoint reference.
     * @return the head revision associated with the checkpoint or {@code null}
     *      if there is no such checkpoint.
     * @throws IllegalArgumentException if the checkpoint is malformed.
     */
    @CheckForNull
    RevisionVector retrieve(@Nonnull String checkpoint)
            throws IllegalArgumentException {
        Revision r;
        try {
            r = Revision.fromString(checkNotNull(checkpoint));
        } catch (IllegalArgumentException e) {
            LOG.warn("Malformed checkpoint reference: {}", checkpoint);
            return null;
        }
        Info info = getCheckpoints().get(r);
        if (info == null) {
            return null;
        }
        RevisionVector rv = info.getCheckpoint();
        if (rv == null) {
            rv = expand(r);
        }
        return rv;
    }

    void setInfoProperty(@Nonnull String checkpoint, @Nonnull String key, @Nullable String value) {
        Revision r = Revision.fromString(checkNotNull(checkpoint));
        Info info = getCheckpoints().get(r);
        if (info == null) {
            throw new IllegalArgumentException("No such checkpoint: " + checkpoint);
        }
        Map<String, String> metadata = new LinkedHashMap<>(info.get());
        if (value == null) {
            metadata.remove(key);
        } else {
            metadata.put(key, value);
        }
        Info newInfo = new Info(info.getExpiryTime(), info.getCheckpoint(), metadata);

        UpdateOp op = new UpdateOp(Checkpoints.ID, false);
        op.setMapEntry(PROP_CHECKPOINT, r, newInfo.toString());
        store.findAndUpdate(Collection.SETTINGS, op);
    }

    int size() {
        return getCheckpoints().size();
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
            store.createOrUpdate(Collection.SETTINGS, updateOp);
        }
    }

    private RevisionVector expand(Revision checkpoint) {
        LOG.warn("Expanding {} single revision checkpoint into a " +
                "RevisionVector. Please make sure all cluster nodes run " +
                "with the same Oak version.", checkpoint);
        // best effort conversion
        Map<Integer, Revision> revs = Maps.newHashMap();
        RevisionVector head = nodeStore.getHeadRevision();
        for (Revision r : head) {
            int cId = r.getClusterId();
            if (cId == checkpoint.getClusterId()) {
                revs.put(cId, checkpoint);
            } else {
                revs.put(cId, new Revision(checkpoint.getTimestamp(), 0, cId));
            }
        }
        return head.pmin(new RevisionVector(revs.values()));
    }

    static final class Info {

        private static final String EXPIRES = "expires";

        private static final String REVISION_VECTOR = "rv";

        private final long expiryTime;

        private final RevisionVector checkpoint;

        private final Map<String, String> info;

        private Info(long expiryTime,
                     @Nullable  RevisionVector checkpoint,
                     @Nonnull Map<String, String> info) {
            this.expiryTime = expiryTime;
            this.checkpoint = checkpoint;
            this.info = Collections.unmodifiableMap(info);
        }

        static Info fromString(String info) {
            long expiryTime;
            RevisionVector rv = null;
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
                    String value = reader.readString();
                    // second entry is potentially checkpoint revision vector
                    if (rv == null && map.isEmpty() && REVISION_VECTOR.equals(key)) {
                        // try to read checkpoint
                        try {
                            rv = RevisionVector.fromString(value);
                        } catch (IllegalArgumentException e) {
                            // not a revision vector, read as regular info entry
                            map.put(key, value);
                        }
                    } else {
                        map.put(key, value);
                    }
                }
                reader.read('}');
                reader.read(JsopReader.END);
            } else {
                // old format
                map = Collections.emptyMap();
                expiryTime = Long.parseLong(info);
            }
            return new Info(expiryTime, rv, map);
        }

        Map<String, String> get() {
            return info;
        }

        long getExpiryTime() {
            return expiryTime;
        }

        /**
         * The revision vector associated with this checkpoint or {@code null}
         * if this checkpoint was created with a version of Oak, which did not
         * yet support revision vectors.
         *
         * @return the revision vector checkpoint or {@code null}.
         */
        @CheckForNull
        RevisionVector getCheckpoint() {
            return checkpoint;
        }

        @Override
        public String toString() {
            JsopWriter writer = new JsopBuilder();
            writer.object();
            writer.key(EXPIRES).value(Long.toString(expiryTime));
            if (checkpoint != null) {
                writer.key(REVISION_VECTOR).value(checkpoint.toString());
            }
            for (Map.Entry<String, String> entry : info.entrySet()) {
                writer.key(entry.getKey()).value(entry.getValue());
            }
            writer.endObject();
            return writer.toString();
        }
    }
}
