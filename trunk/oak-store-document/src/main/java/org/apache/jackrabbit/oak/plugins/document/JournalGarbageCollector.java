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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;

/**
 * The JournalGarbageCollector can clean up JournalEntries that are older than a
 * particular age.
 * <p>
 * It would typically be invoked in conjunction with the VersionGarbageCollector
 * but must not be confused with that one - 'journal' refers to the separate
 * collection that contains changed paths per background writes used for
 * observation.
 */
public class JournalGarbageCollector {

    private static final Logger log = LoggerFactory.getLogger(JournalGarbageCollector.class);

    /**
     * ID of the journalGC document in the settings collection.
     */
    private static final String JOURNAL_GC_ID = "journalGC";

    /**
     * Key name of the entry that contains the timestamp of the journal tail.
     */
    private static final String TAIL_TIMESTAMP = "tailTimestamp";

    private final DocumentNodeStore ns;

    private final long maxRevisionAgeMillis;

    private volatile long lastTailTimestampRefresh = Long.MIN_VALUE;

    private Revision tailRevision;

    public JournalGarbageCollector(DocumentNodeStore nodeStore,
                                   long maxRevisionAgeMillis) {
        this.ns = nodeStore;
        this.maxRevisionAgeMillis = maxRevisionAgeMillis;
        this.tailRevision = new Revision(0, 0, ns.getClusterId());
    }

    /**
     * Deletes entries in the journal that are older than
     * {@link #getMaxRevisionAgeMillis()}.
     *
     * @return the number of entries that have been removed
     */
    public int gc() {
        DocumentStore ds = ns.getDocumentStore();
        Revision checkpointRev = ns.getCheckpoints().getOldestRevisionToKeep();
        Long keep = null;
        if (checkpointRev != null) {
            // keep more entries than just up to the checkpoint to account
            // for branch commits that may be referenced by merge commits
            keep = checkpointRev.getTimestamp() - maxRevisionAgeMillis / 2;
        }
        long now = ns.getClock().getTime();
        long gcOlderThan = now - maxRevisionAgeMillis;
        if (keep != null && keep < gcOlderThan) {
            gcOlderThan = keep;
            log.debug("gc: Checkpoint {} is older than maxRevisionAge: {} min",
                    keep, MILLISECONDS.toMinutes(maxRevisionAgeMillis));
        }
        if (log.isDebugEnabled()) {
            log.debug("gc: Journal garbage collection starts with maxAge: {} min.",
                    MILLISECONDS.toMinutes(maxRevisionAgeMillis));
        }
        Stopwatch sw = Stopwatch.createStarted();

        // update the tail timestamp in the journalGC document
        // of the settings collection
        updateTailTimestamp(gcOlderThan);

        int numDeleted = ds.remove(Collection.JOURNAL, JournalEntry.MODIFIED, 0, gcOlderThan);

        sw.stop();

        if (numDeleted > 0) {
            log.info("gc: Journal garbage collection took {}, deleted {} entries that were older than {} min.",
                    sw, numDeleted, MILLISECONDS.toMinutes(now - gcOlderThan));
        }
        return numDeleted;
    }

    private void updateTailTimestamp(long gcOlderThan) {
        UpdateOp op = new UpdateOp(JOURNAL_GC_ID, true);
        op.max(TAIL_TIMESTAMP, gcOlderThan);
        ns.getDocumentStore().createOrUpdate(SETTINGS, op);
    }

    public Revision getTailRevision() {
        refreshTailRevisionIfNecessary();
        return tailRevision;
    }

    long getMaxRevisionAgeMillis() {
        return maxRevisionAgeMillis;
    }

    private void refreshTailRevisionIfNecessary() {
        // refresh once a minute
        long now = ns.getClock().getTime();
        if (lastTailTimestampRefresh + TimeUnit.MINUTES.toMillis(1) > now) {
            return;
        }
        lastTailTimestampRefresh = now;

        Document doc = ns.getDocumentStore().find(SETTINGS, JOURNAL_GC_ID);
        if (doc == null) {
            // no gc yet
            return;
        }
        Long ts = Utils.asLong((Number) doc.get(TAIL_TIMESTAMP));
        if (ts != null) {
            tailRevision = Utils.max(tailRevision, new Revision(ts, 0, ns.getClusterId()));
        }
    }

    private List<String> asKeys(List<JournalEntry> deletionBatch) {
        final List<String> keys = new ArrayList<String>(deletionBatch.size());
        for (JournalEntry e : deletionBatch) {
            keys.add(e.getId());
        }
        return keys;
    }

}
