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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the document stored in the settings collection containing a
 * 'cluster view'.
 * <p>
 * A 'cluster view' is the state of the membership of instances that are or have
 * all been connected to the same oak repository. The 'cluster view' is
 * maintained by all instances in the cluster concurrently - the faster one
 * wins. Its information is derived from the clusterNodes collection. From there
 * the following three states are derived and instances are grouped into these:
 * <ul>
 * <li>Active: an instance is active and has no recoveryLock is currently
 * acquired. The lease timeout is ignored. When the lease times out, this is
 * noticed by one of the instances at some point and a recovery is started, at
 * which point the instance transitions from 'Active' to 'Recovering'.</li>
 * <li>Recovering: an instance that was active but currently has the
 * recoveryLock acquired by one of the instances.</li>
 * <li>Inactive: an instance is not set to active (in which case the
 * recoveryLock is never set)</li>
 * </ul>
 * <p>
 * Note that the states managed in this ClusterViewDocument differs from the one
 * from ClusterView - since ClusterView also manages the fact that after a
 * recovery of a crashed instance there could be a 'backlog' of changes which it
 * doesn't yet see until a background read is performed.
 */
class ClusterViewDocument {

    private static final Logger logger = LoggerFactory.getLogger(ClusterViewDocument.class);

    /** the id of this document is always 'clusterView' **/
    private static final String CLUSTERVIEW_DOC_ID = "clusterView";

    // keys that we store in the root document - and in the history
    /**
     * document key that stores the monotonically incrementing sequence number
     * of the cluster view. Any update will increase this by 1
     **/
    static final String VIEW_SEQ_NUM_KEY = "seqNum";

    /**
     * document key that stores the comma-separated list of active instance ids
     **/
    static final String ACTIVE_KEY = "active";

    /**
     * document key that stores the comma-separated list of inactive instance
     * ids (they might still have a backlog, that is handled in ClusterView
     * though, never persisted
     */
    static final String INACTIVE_KEY = "inactive";

    /**
     * document key that stores the comma-separated list of recovering instance
     * ids
     **/
    static final String RECOVERING_KEY = "recovering";

    /**
     * document key that stores the date and time when this view was created -
     * for debugging purpose only
     **/
    private static final String CREATED_KEY = "created";

    /**
     * document key that stores the id of the instance that created this view -
     * for debugging purpose only
     **/
    private static final String CREATOR_KEY = "creator";

    /**
     * document key that stores the date and time when this was was retired -
     * for debugging purpose only
     **/
    private static final String RETIRED_KEY = "retired";

    /**
     * document key that stores the id of the instance that retired this view -
     * for debugging purpose only
     **/
    private static final String RETIRER_KEY = "retirer";

    /**
     * document key that stores a short, limited history of previous cluster
     * views - for debugging purpose only
     **/
    private static final String CLUSTER_VIEW_HISTORY_KEY = "clusterViewHistory";

    /** the format used when storing date+time **/
    private static final DateFormat standardDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    /** number of elements kept in the CLUSTERVIEW_HISTORY_KEY field **/
    static final int HISTORY_LIMIT = 10;

    /** the monotonically incrementing sequence number of this cluster view **/
    private final long viewSeqNum;

    /** the ids of instances that are active at this moment **/
    private final Integer[] activeIds;

    /**
     * the ids of instances that are recovering (lastRev-recovery) at this
     * moment
     **/
    private final Integer[] recoveringIds;

    /** the ids of instances that are inactive at this moment **/
    private final Integer[] inactiveIds;

    /**
     * the short, limited history of previous cluster views, for debugging only
     **/
    private final Map<Object, String> viewHistory;

    /** the date+time at which this view was created, for debugging only **/
    private final String createdAt;

    /** the id of the instance that created this view, for debugging only **/
    private final Long createdBy;

    /**
     * Main method by which the ClusterViewDocument is updated in the settings
     * collection
     * 
     * @return the resulting ClusterViewDocument as just updated in the settings
     *         collection - or null if another instance was updating the
     *         clusterview concurrently (in which case the caller should re-read
     *         first and possibly re-update if needed)
     */
    static ClusterViewDocument readOrUpdate(DocumentNodeStore documentNodeStore, Set<Integer> activeIds, Set<Integer> recoveringIds,
            Set<Integer> inactiveIds) {
        logger.trace("readOrUpdate: expected: activeIds: {}, recoveringIds: {}, inactiveIds: {}", activeIds, recoveringIds,
                inactiveIds);
        if (activeIds == null || activeIds.size() == 0) {
            logger.info("readOrUpdate: activeIds must not be null or empty");
            throw new IllegalStateException("activeIds must not be null or empty");
        }
        int localClusterId = documentNodeStore.getClusterId();

        final ClusterViewDocument previousView = doRead(documentNodeStore);
        if (previousView != null) {
            if (previousView.matches(activeIds, recoveringIds, inactiveIds)) {
                logger.trace("readOrUpdate: view unchanged, returning: {}", previousView);
                return previousView;
            }
        }
        logger.trace(
                "readOrUpdate: view change detected, going to update from {} to activeIds: {}, recoveringIds: {}, inactiveIds: {}",
                previousView, activeIds, recoveringIds, inactiveIds);
        UpdateOp updateOp = new UpdateOp(CLUSTERVIEW_DOC_ID, true);
        Date now = new Date();
        updateOp.set(ACTIVE_KEY, setToCsv(activeIds));
        updateOp.set(RECOVERING_KEY, setToCsv(recoveringIds));
        updateOp.set(INACTIVE_KEY, setToCsv(inactiveIds));
        updateOp.set(CREATED_KEY, standardDateFormat.format(now));
        updateOp.set(CREATOR_KEY, (long)localClusterId);
        if (previousView != null) {
            Map<Object, String> previousHistory = previousView.getHistory();
            if (previousHistory!=null) {
                Map<Object, String> mapClone = new HashMap<Object, String>(previousHistory);
                while(mapClone.size()>=HISTORY_LIMIT) {
                    Revision oldestRevision = oldestRevision(mapClone);
                    if (oldestRevision==null) {
                        break;
                    }
                    updateOp.removeMapEntry(CLUSTER_VIEW_HISTORY_KEY, oldestRevision);
                    if (mapClone.remove(oldestRevision)==null) {
                        // prevent an endless loop
                        break;
                    }
                }
            }
            updateOp.setMapEntry(CLUSTER_VIEW_HISTORY_KEY, Revision.newRevision(localClusterId), asHistoryEntry(previousView, localClusterId, now));
        }

        final Long newViewSeqNum;
        if (previousView == null) {
            // we are the first ever, looks like, that the clusterview is
            // defined
            // so we can use viewId==1 and we make sure no other cluster node
            // tries to create this first one simultaneously - so we use
            // 'create'

            ArrayList<UpdateOp> updateOps = new ArrayList<UpdateOp>();
            newViewSeqNum = 1L;
            updateOp.setNew(true); // paranoia as that's already set above
            updateOp.set(VIEW_SEQ_NUM_KEY, newViewSeqNum);
            updateOps.add(updateOp);
            logger.debug("updateAndRead: trying to create the first ever clusterView - hence {}={}", VIEW_SEQ_NUM_KEY,
                    newViewSeqNum);
            if (!documentNodeStore.getDocumentStore().create(Collection.SETTINGS, updateOps)) {
                logger.debug("updateAndRead: someone else just created the first view ever while I tried - reread that one later");
                return null;
            }
        } else {
            // there were earlier clusterViews (the normal case) - thus we
            // use 'findAndUpdate' with the condition that
            // the view id is still at the previousview one
            Long previousViewSeqNum = previousView.getViewSeqNum();
            updateOp.setNew(false); // change to false from true above
            updateOp.equals(VIEW_SEQ_NUM_KEY, null, previousViewSeqNum);
            newViewSeqNum = previousViewSeqNum + 1;
            updateOp.set(VIEW_SEQ_NUM_KEY, newViewSeqNum);
            logger.debug("updateAndRead: trying to update the clusterView to {}={} ", VIEW_SEQ_NUM_KEY, newViewSeqNum);
            if (documentNodeStore.getDocumentStore().findAndUpdate(Collection.SETTINGS, updateOp) == null) {
                logger.debug(
                        "updateAndRead: someone else just updated the view which I wanted to do as well - reread that one later");
                return null;
            }
        }

        // whatever the outcome of the above - we don't care -
        // re-reading will in any case definitely show what has been persisted
        // and if the re-read view contains the same id, it is what we have
        // written
        // - otherwise someone else came in between and we have to step back and
        // retry
        ClusterViewDocument readResult = doRead(documentNodeStore);
        if (readResult == null) {
            logger.debug("updateAndRead: got null from read - whatever the exact reason, we must retry in a moment.");
            return null;
        } else if (newViewSeqNum.equals(readResult.getViewSeqNum())) {
            logger.debug("updateAndRead: matching view - no change");
            return readResult;
        } else {
            logger.debug("updateAndRead: someone else in the cluster was updating right after I also succeeded - re-read in a bit");
            return null;
        }
    }

    /**
     * Determine the oldest history entry (to be removed to keep history
     * limited to HISTORY_LIMIT)
     * 
     * @param historyMap
     */
    private static Revision oldestRevision(Map<Object, String> historyMap) {
        String oldestRevision = null;
        for (Iterator<Object> it = historyMap.keySet().iterator(); it.hasNext();) {
            Object obj = it.next();
            // obj can be a String or a Revision
            // in case of it being a Revision the toString() will
            // be appropriate, hence:
            String r = obj.toString();
            if (oldestRevision == null) {
                oldestRevision = r;
            } else if (Revision.getTimestampDifference(Revision.fromString(r), Revision.fromString(oldestRevision)) < 0) {
                oldestRevision = r;
            }
        }
        if (oldestRevision==null) {
            return null;
        } else {
            return Revision.fromString(oldestRevision);
        }
    }

    /** Converts a previous clusterView document into a history 'string' **/
    private static String asHistoryEntry(final ClusterViewDocument previousView, int retiringClusterNodeId, Date retireTime) {
        if (previousView==null) {
            throw new IllegalArgumentException("previousView must not be null");
        }
        String h;
        JsopBuilder b = new JsopBuilder();
        b.object();
        b.key(VIEW_SEQ_NUM_KEY);
        b.value(previousView.getViewSeqNum());
        b.key(CREATED_KEY);
        b.value(String.valueOf(previousView.getCreatedAt()));
        b.key(CREATOR_KEY);
        b.value(previousView.getCreatedBy());
        b.key(RETIRED_KEY);
        b.value(String.valueOf(standardDateFormat.format(retireTime)));
        b.key(RETIRER_KEY);
        b.value(retiringClusterNodeId);
        b.key(ACTIVE_KEY);
        b.value(setToCsv(previousView.getActiveIds()));
        b.key(RECOVERING_KEY);
        b.value(setToCsv(previousView.getRecoveringIds()));
        b.key(INACTIVE_KEY);
        b.value(setToCsv(previousView.getInactiveIds()));
        b.endObject();
        h = b.toString();
        return h;
    }

    /**
     * helper method to convert a set to a comma-separated string (without using
     * toString() for safety)
     * 
     * @return null if set is null or empty, comma-separated string (no spaces)
     *         otherwise
     */
    private static String setToCsv(Set<Integer> ids) {
        if (ids == null || ids.size() == 0) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (Integer id : ids) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(id);
        }
        return sb.toString();
    }

    /**
     * helper method to convert an array to a comma-separated string
     * 
     * @return null if array is null or empty, comman-separated string (no
     *         spaces) otherwise
     */
    static String arrayToCsv(Integer[] arr) {
        if (arr == null || arr.length == 0) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        for (Integer a : arr) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(a);
        }
        return sb.toString();
    }

    /**
     * inverse helper method which converts a comma-separated string into an
     * integer array
     **/
    static Integer[] csvToIntegerArray(String csv) {
        if (csv == null) {
            return null;
        }
        String[] split = csv.split(",");
        Integer[] result = new Integer[split.length];
        for (int i = 0; i < split.length; i++) {
            result[i] = Integer.parseInt(split[i]);
        }
        return result;
    }

    /**
     * internal reader of an existing clusterView document from the settings
     * collection
     **/
    private static ClusterViewDocument doRead(DocumentNodeStore documentNodeStore) {
        DocumentStore documentStore = documentNodeStore.getDocumentStore();
        Document doc = documentStore.find(Collection.SETTINGS, "clusterView",
                -1 /* -1; avoid caching */);
        if (doc == null) {
            return null;
        } else {
            ClusterViewDocument clusterView = new ClusterViewDocument(doc);
            if (clusterView.isValid()) {
                return clusterView;
            } else {
                logger.warn("read: clusterView document is not valid: " + doc.format());
                return null;
            }
        }
    }

    /** comparison helper that compares an integer array with a set **/
    static boolean matches(Integer[] expected, Set<Integer> actual) {
        boolean expectedIsEmpty = expected == null || expected.length == 0;
        boolean actualIsEmpty = actual == null || actual.size() == 0;
        if (expectedIsEmpty && actualIsEmpty) {
            // if both are null or empty, they match
            return true;
        }
        if (expectedIsEmpty != actualIsEmpty) {
            // if one of them is only empty, but the other not, then they don't
            // match
            return false;
        }
        if (expected.length != actual.size()) {
            // different size
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            Integer aMemberId = expected[i];
            if (!actual.contains(aMemberId)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    ClusterViewDocument(Document doc) {
        if (doc == null) {
            throw new IllegalArgumentException("doc must not be null");
        }
        this.viewSeqNum = (Long) doc.get(VIEW_SEQ_NUM_KEY);
        this.createdAt = (String) doc.get(CREATED_KEY);
        Object creatorId = doc.get(CREATOR_KEY);
        if (creatorId instanceof Number) {
            this.createdBy = ((Number) creatorId).longValue();
        } else if (creatorId == null) {
            this.createdBy = -1L;
        } else {
            throw new IllegalStateException("Unsupported type of creator: "+creatorId);
        }

        Object obj = doc.get(ACTIVE_KEY);
        if (obj == null || !(obj instanceof String)) {
            logger.trace("<init>: {} : {}", ACTIVE_KEY, obj);
            this.activeIds = new Integer[0];
        } else {
            this.activeIds = csvToIntegerArray((String) obj);
        }

        Object obj2 = doc.get(RECOVERING_KEY);
        if (obj2 == null || !(obj2 instanceof String)) {
            logger.trace("<init>: {} : {}", RECOVERING_KEY, obj2);
            this.recoveringIds = new Integer[0];
        } else {
            this.recoveringIds = csvToIntegerArray((String) obj2);
        }

        Object obj3 = doc.get(INACTIVE_KEY);
        if (obj3 == null || !(obj3 instanceof String)) {
            logger.trace("<init>: {} : {}", INACTIVE_KEY, obj3);
            this.inactiveIds = new Integer[0];
        } else {
            this.inactiveIds = csvToIntegerArray((String) obj3);
        }

        Object obj4 = doc.get(CLUSTER_VIEW_HISTORY_KEY);
        if (obj4 == null || !(obj4 instanceof Map)) {
            logger.trace("<init> viewHistory is null");
            this.viewHistory = null;
        } else {
            this.viewHistory = ((Map<Object, String>) obj4);
        }
    }

    /** Returns the set of active ids of this cluster view **/
    Set<Integer> getActiveIds() {
        return new HashSet<Integer>(Arrays.asList(activeIds));
    }

    /** Returns the set of recovering ids of this cluster view **/
    Set<Integer> getRecoveringIds() {
        return new HashSet<Integer>(Arrays.asList(recoveringIds));
    }

    /** Returns the set of inactive ids of this cluster view **/
    Set<Integer> getInactiveIds() {
        return new HashSet<Integer>(Arrays.asList(inactiveIds));
    }

    /** Returns the history map **/
    Map<Object, String> getHistory() {
        return viewHistory;
    }

    @Override
    public String toString() {
        return "a ClusterView[valid=" + isValid() + ", viewSeqNum=" + viewSeqNum
                + ", activeIds=" + arrayToCsv(activeIds) + ", recoveringIds=" + arrayToCsv(recoveringIds) + ", inactiveIds="
                + arrayToCsv(inactiveIds) + "]";
    }

    boolean isValid() {
        return viewSeqNum >= 0 && activeIds != null && activeIds.length > 0;
    }

    /**
     * Returns the date+time when this view was created, for debugging purpose
     * only
     **/
    String getCreatedAt() {
        return createdAt;
    }

    /**
     * Returns the id of the instance that created this view, for debugging
     * purpose only
     **/
    long getCreatedBy() {
        return createdBy;
    }

    /** Returns the monotonically incrementing sequenece number of this view **/
    long getViewSeqNum() {
        return viewSeqNum;
    }

    private boolean matches(Set<Integer> activeIds, Set<Integer> recoveringIds, Set<Integer> inactiveIds) {
        if (!matches(this.activeIds, activeIds)) {
            return false;
        }
        if (!matches(this.recoveringIds, recoveringIds)) {
            return false;
        }
        if (!matches(this.inactiveIds, inactiveIds)) {
            return false;
        }
        return true;
    }

}
