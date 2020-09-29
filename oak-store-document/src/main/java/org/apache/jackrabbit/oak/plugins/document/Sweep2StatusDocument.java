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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the sweep2 status as recorded in the settings collection.
 */
public class Sweep2StatusDocument {

    private static final Logger LOG = LoggerFactory.getLogger(Sweep2StatusDocument.class);

    static final String SWEEP2_STATUS_ID = "sweep2Status";

    private static final String STATUS_PROPERTY = "status";
    private static final String STATUS_VALUE_SWEEPING = "sweeping";
    private static final String STATUS_VALUE_SWEPT = "swept";

    private static final String LOCK_PROPERTY = "lock";
    private static final String MOD_COUNT_PROPERTY = "_modCount";

    private static final String SWEPT_BY_PROPERTY = "sweptBy";

    public static Sweep2StatusDocument readFrom(DocumentStore documentStore) {
        Document doc = documentStore.find(Collection.SETTINGS, SWEEP2_STATUS_ID,
                -1 /* -1; avoid caching */);
        if (doc == null) {
            return null;
        } else {
            return new Sweep2StatusDocument(doc);
        }
    }

    /**
     * Acquires the sweep2 lock.
     * @param documentNodeStore
     * @param clusterId
     * @return <ul>
     * <li>
     * -1 if the lock could not be acquired (another instance got in between)
     * </li>
     * <li>
     * &gt; 0 if a lock was acquired (in which case this returned value is the lock value, which is always &gt; 0)
     * </li>
     * </ul>
     */
    public static long acquireSweep2Lock(DocumentNodeStore documentNodeStore,
            int clusterId) {
        DocumentStore documentStore = documentNodeStore.getDocumentStore();
        Document existingStatusDoc = documentStore.find(Collection.SETTINGS, SWEEP2_STATUS_ID,
                -1 /* -1; avoid caching */);
        UpdateOp updateOp = new UpdateOp(SWEEP2_STATUS_ID, true);
        updateOp.set(STATUS_PROPERTY, STATUS_VALUE_SWEEPING);
        updateOp.set(LOCK_PROPERTY, clusterId);
        ArrayList<UpdateOp> updateOps = new ArrayList<UpdateOp>();
        updateOps.add(updateOp);
        if (existingStatusDoc == null) {
            updateOp.setNew(true);
            updateOp.set(MOD_COUNT_PROPERTY, 1L);
            if (!documentNodeStore.getDocumentStore().create(Collection.SETTINGS, updateOps)) {
                LOG.info("acquireLock: another instance just acquired the (new) sweep2 lock a few moments ago.");
                return -1;
            } else {
                return 1;
            }
        } else {
            final Sweep2StatusDocument existingStatus = new Sweep2StatusDocument(existingStatusDoc);
            if (existingStatus.isSwept()) {
                // not needed => -1
                return -1;
            }
            if (existingStatus.getLockClusterId() == clusterId) {
                // already locked by local instance => return existing lock value
                return existingStatus.getLockValue();
            }
            updateOp.setNew(false);
            updateOp.equals(MOD_COUNT_PROPERTY, existingStatusDoc.getModCount());
            final long newModCount = existingStatusDoc.getModCount() + 1;
            updateOp.set(MOD_COUNT_PROPERTY, newModCount);
            if (documentNodeStore.getDocumentStore().findAndUpdate(Collection.SETTINGS, updateOp) == null) {
                LOG.info("acquireLock: another instance just acquired the (expired) sweep2 lock a few moments ago");
                return -1;
            } else {
                return newModCount;
            }
        }
    }

    /**
     * Release the sweep2 lock and record swept2 successful.
     * Note that the clusterId is only for recording purpose - this method
     * makes no checks on the current owner of the lock
     * @param documentStore
     * @param clusterId
     * @return true if the sweep2 status is now marked swept(2) - false if that failed
     * (in the latter case the caller can consider retrying the acquire/sweep2/release sequence)
     */
    public static boolean forceReleaseSweep2LockAndMarkSwept(DocumentStore documentStore, int clusterId) {
        Document existing = documentStore.find(Collection.SETTINGS, SWEEP2_STATUS_ID,
                -1 /* -1; avoid caching */);

        if (existing == null) {
            // we directly mark the sweep2 as done if no sweep2 is even necessary.
            // so it is legal that we have no existingSweep2Doc yet
            // lock is ignored when there was no sweep2Status yet
            UpdateOp updateOp = new UpdateOp(SWEEP2_STATUS_ID, true);
            updateOp.set(STATUS_PROPERTY, STATUS_VALUE_SWEPT);
            updateOp.setNew(true);
            updateOp.set(MOD_COUNT_PROPERTY, 1L);
            updateOp.set(SWEPT_BY_PROPERTY, clusterId);
            ArrayList<UpdateOp> updateOps = new ArrayList<UpdateOp>();
            updateOps.add(updateOp);
            if (!documentStore.create(Collection.SETTINGS, updateOps)) {
                LOG.info("forceReleaseLockAndMarkSwept: another instance just wanted to mark sweep2 as done a few moments ago too.");
                return false;
            } else {
                return true;
            }
        } else {
            // there was a lock (probably) - at least there was a sweep2 status
            // we don't care about what that status was, we only
            // (force) mark it as done
            UpdateOp updateOp = new UpdateOp(SWEEP2_STATUS_ID, false);
            updateOp.set(STATUS_PROPERTY, STATUS_VALUE_SWEPT);
            updateOp.set(MOD_COUNT_PROPERTY, existing.getModCount() + 1);
            updateOp.set(SWEPT_BY_PROPERTY, clusterId);
            if (existing.keySet().contains(LOCK_PROPERTY)) {
                updateOp.remove(LOCK_PROPERTY);
            }
            if (documentStore.findAndUpdate(Collection.SETTINGS, updateOp) == null) {
                LOG.info("forceReleaseLockAndMarkSwept: another instance just wanted to mark sweep2 as done a few moments ago too.");
                Sweep2StatusDocument status = readFrom(documentStore);
                if (status == null) {
                    LOG.warn("forceReleaseLockAndMarkSwept: no existing sweep2 status after updating failed");
                    return false;
                } else {
                    // so, someone else force-marked as swept in between, if that succeeded
                    // the status is now swept - in that case we consider the job done anyway
                    return status.isSwept();
                }
            } else {
                return true;
            }
        }
    }

    private final Document doc;

    private Sweep2StatusDocument(Document doc) {
        this.doc = doc;
    }

    public boolean isSwept() {
        return STATUS_VALUE_SWEPT.equals(doc.get(STATUS_PROPERTY));
    }

    public boolean isSweeping() {
        return STATUS_VALUE_SWEEPING.equals(doc.get(STATUS_PROPERTY));
    }

    public int getLockClusterId() {
        return Integer.parseInt(String.valueOf(doc.get(LOCK_PROPERTY)));
    }

    public long getLockValue() {
        return doc.getModCount();
    }

    @Override
    public String toString() {
        return "Sweep2StatusDocument(isSwept=" + isSwept() + ",lockClusterId=" + getLockClusterId() + ",lockValue=" + getLockValue()+")";
    }

}
