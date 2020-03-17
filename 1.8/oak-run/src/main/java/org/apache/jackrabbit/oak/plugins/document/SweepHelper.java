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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.REMOVE_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.SET_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;

/**
 * Helper class to perform a revision sweep for a given clusterId.
 */
public final class SweepHelper {

    private static final TimeDurationFormatter TDF = TimeDurationFormatter.forLogging();

    public static void sweep(final DocumentStore store,
                             final RevisionContext context,
                             final MissingLastRevSeeker seeker) {
        int clusterId = context.getClusterId();
        NodeDocument rootDoc = getRootDocument(store);
        Revision lastRev = rootDoc.getLastRev().get(clusterId);
        if (lastRev == null) {
            throw new IllegalArgumentException("root document does not" +
                    " have a lastRev entry for clusterId " + clusterId);
        }
        final AtomicReference<Revision> sweepRev = new AtomicReference<>(
                rootDoc.getSweepRevisions().getRevision(clusterId));
        if (sweepRev.get() == null) {
            sweepRev.set(new Revision(0, 0, clusterId));
        }
        final AtomicLong sweepUpdates = new AtomicLong();
        final AtomicLong bcUpdates = new AtomicLong();
        final AtomicLong revertUpdates = new AtomicLong();
        long time = System.currentTimeMillis();
        NodeDocumentSweeper sweeper = new NodeDocumentSweeper(context, true);
        sweeper.sweep(seeker.getCandidates(sweepRev.get().getTimestamp()),
                new NodeDocumentSweepListener() {
            @Override
            public void sweepUpdate(Map<String, UpdateOp> updates)
                    throws DocumentStoreException {
                // create an invalidate entry
                JournalEntry inv = JOURNAL.newDocument(store);
                inv.modified(updates.keySet());
                Revision r = context.newRevision().asBranchRevision();
                UpdateOp invOp = inv.asUpdateOp(r);
                // and reference it from a regular entry
                JournalEntry entry = JOURNAL.newDocument(store);
                entry.invalidate(Collections.singleton(r));
                Revision jRev = context.newRevision();
                UpdateOp jOp = entry.asUpdateOp(jRev);
                if (!store.create(JOURNAL, newArrayList(invOp, jOp))) {
                    String msg = "Unable to create journal entries for " +
                            "document invalidation.";
                    throw new DocumentStoreException(msg);
                }
                sweepRev.set(Utils.max(sweepRev.get(), jRev));
                // now that journal entry is in place, perform the actual
                // updates on the documents
                store.createOrUpdate(NODES, newArrayList(updates.values()));
                sweepUpdates.incrementAndGet();
                trackStats(updates.values());
                System.out.println("Sweeper updated " + updates.keySet());
            }

            private void trackStats(Iterable<UpdateOp> ops) {
                for (UpdateOp op : ops) {
                    boolean bcUpdate = false;
                    boolean revertUpdate = false;
                    for (UpdateOp.Operation o : op.getChanges().values()) {
                        if (o.type == SET_MAP_ENTRY) {
                            bcUpdate = true;
                        } else if (o.type == REMOVE_MAP_ENTRY) {
                            revertUpdate = true;
                        }
                    }
                    if (bcUpdate) {
                        bcUpdates.incrementAndGet();
                    }
                    if (revertUpdate) {
                        revertUpdates.incrementAndGet();
                    }
                }
            }
        });
        time = System.currentTimeMillis() - time;
        // write back sweep revision if none was there or revisions were swept
        if (rootDoc.getSweepRevisions().getRevision(clusterId) == null
                || sweepUpdates.get() > 0) {
            // sweepRev must be at least the current lastRev
            sweepRev.set(Utils.max(sweepRev.get(), lastRev));
            // perform a conditional update on the last seen lastRev
            // to ensure the cluster node is still inactive
            UpdateOp op = new UpdateOp(rootDoc.getId(), false);
            op.equals("_lastRev",
                    new Revision(0, 0, clusterId),
                    lastRev.toString());
            NodeDocument.setSweepRevision(op, sweepRev.get());
            NodeDocument.setLastRev(op, sweepRev.get());
            if (store.findAndUpdate(NODES, op) != null) {
                System.out.println("Updated sweep revision to " +
                        sweepRev.get() + ". Branch commit markers added to " +
                        bcUpdates.get() + " documents. Reverted uncommitted " +
                        "changes on " + revertUpdates.get() + " documents. " +
                        "(" + formatMillis(time) + ")");
            } else {
                System.out.println("Sweep revision updated failed. " +
                        "Is clusterId " + clusterId + " active again?");
            }
        } else {
            System.out.println("Revision sweep not needed for " +
                    "clusterId " + clusterId);
        }
    }

    private static String formatMillis(long millis) {
        return TDF.format(millis, TimeUnit.MILLISECONDS);
    }
}
