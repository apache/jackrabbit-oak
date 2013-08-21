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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A document storing data about a node.
 */
public class NodeDocument extends Document {

    private static final Logger log = LoggerFactory.getLogger(NodeDocument.class);

    /**
     * Marker document, which indicates the document does not exist.
     */
    public static final NodeDocument NULL = new NodeDocument();

    /**
     * The list of revision to root commit depth mappings to find out if a
     * revision is actually committed.
     */
    private static final String COMMIT_ROOT = "_commitRoot";

    /**
     * The number of previous documents (documents that contain old revisions of
     * this node). This property is only set if multiple documents per node
     * exist. This is the case when a node is updated very often in a short
     * time, such that the document gets very big.
     */
    private static final String PREVIOUS = "_prev";

    /**
     * Whether this node is deleted. Key: revision, value: true/false.
     */
    static final String DELETED = "_deleted";

    /**
     * Revision collision markers set by commits with modifications, which
     * overlap with un-merged branch commits.
     * Key: revision, value:
     */
    static final String COLLISIONS = "_collisions";

    /**
     * The modified time (5 second resolution).
     */
    static final String MODIFIED = "_modified";

    /**
     * The list of recent revisions for this node, where this node is the
     * root of the commit. Key: revision, value: true or the base revision of an
     * un-merged branch commit.
     */
    private static final String REVISIONS = "_revisions";

    /**
     * The last revision. Key: machine id, value: revision.
     */
    private static final String LAST_REV = "_lastRev";

    private final long time = System.currentTimeMillis();

    /**
     * @return the system time this object was created.
     */
    public final long getCreated() {
        return time;
    }

    /**
     * @return a map of the last known revision for each clusterId.
     */
    @Nonnull
    public Map<Integer, Revision> getLastRev() {
        Map<Integer, Revision> map = Maps.newHashMap();
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) get(LAST_REV);
        if (valueMap != null) {
            for (Map.Entry<String, String> e : valueMap.entrySet()) {
                int clusterId = Integer.parseInt(e.getKey());
                Revision rev = Revision.fromString(e.getValue());
                map.put(clusterId, rev);
            }
        }
        return map;
    }

    /**
     * Returns <code>true</code> if the given <code>revision</code> is marked
     * committed in <strong>this</strong> document.
     *
     * @param revision the revision.
     * @return <code>true</code> if committed; <code>false</code> otherwise.
     */
    public boolean isCommitted(@Nonnull String revision) {
        checkNotNull(revision);
        @SuppressWarnings("unchecked")
        Map<String, String> revisions = (Map<String, String>) get(REVISIONS);
        return revisions != null && Utils.isCommitted(revisions.get(revision));
    }

    /**
     * Returns <code>true</code> if this document contains an entry for the
     * given <code>revision</code> in the {@link #REVISIONS} map. Please note
     * that an entry in the {@link #REVISIONS} map does not necessarily mean
     * the the revision is committed. Use {@link #isCommitted(String)} to get
     * the commit state of a revision.
     *
     * @param revision the revision to check.
     * @return <code>true</code> if this document contains the given revision.
     */
    public boolean containsRevision(@Nonnull String revision) {
        checkNotNull(revision);
        @SuppressWarnings("unchecked")
        Map<String, String> revisions = (Map<String, String>) get(REVISIONS);
        return revisions != null && revisions.containsKey(revision);
    }

    /**
     * Gets a sorted map of uncommitted revisions of this document with the
     * local cluster node id as returned by the {@link RevisionContext}. These
     * are the {@link #REVISIONS} entries where {@link Utils#isCommitted(String)}
     * returns false.
     *
     * @param context the revision context.
     * @return the uncommitted revisions of this document.
     */
    public SortedMap<Revision, Revision> getUncommittedRevisions(RevisionContext context) {
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) get(NodeDocument.REVISIONS);
        SortedMap<Revision, Revision> revisions =
                new TreeMap<Revision, Revision>(context.getRevisionComparator());
        if (valueMap != null) {
            for (Map.Entry<String, String> commit : valueMap.entrySet()) {
                if (!Utils.isCommitted(commit.getValue())) {
                    Revision r = Revision.fromString(commit.getKey());
                    if (r.getClusterId() == context.getClusterId()) {
                        Revision b = Revision.fromString(commit.getValue());
                        revisions.put(r, b);
                    }
                }
            }
        }
        return revisions;
    }

    /**
     * Returns the commit root path for the given <code>revision</code> or
     * <code>null</code> if this document does not have a commit root entry for
     * the given <code>revision</code>.
     *
     * @param revision a revision.
     * @return the commit root path or <code>null</code>.
     */
    @CheckForNull
    public String getCommitRootPath(String revision) {
        @SuppressWarnings("unchecked")
        Map<String, Integer> valueMap = (Map<String, Integer>) get(COMMIT_ROOT);
        if (valueMap == null) {
            return null;
        }
        Integer depth = valueMap.get(revision);
        if (depth != null) {
            String p = Utils.getPathFromId(getId());
            return PathUtils.getAncestorPath(p, PathUtils.getDepth(p) - depth);
        } else {
            return null;
        }
    }

    /**
     * Get the revision of the latest change made to this node.
     *
     * @param changeRev the revision of the current change
     * @param handler the conflict handler, which is called for concurrent changes
     *                preceding <code>before</code>.
     * @return the revision, or null if deleted
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public Revision getNewestRevision(RevisionContext context,
                                      DocumentStore store,
                                      Revision changeRev,
                                      CollisionHandler handler) {
        SortedSet<String> revisions = new TreeSet<String>(Collections.reverseOrder());
        if (data.containsKey(REVISIONS)) {
            revisions.addAll(((Map<String, String>) get(REVISIONS)).keySet());
        }
        if (data.containsKey(COMMIT_ROOT)) {
            revisions.addAll(((Map<String, Integer>) get(COMMIT_ROOT)).keySet());
        }
        Map<String, String> deletedMap = (Map<String, String>)get(DELETED);
        if (deletedMap != null) {
            revisions.addAll(deletedMap.keySet());
        }
        Revision newestRev = null;
        for (String r : revisions) {
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(context, propRev, changeRev)) {
                // we have seen a previous change from another cluster node
                // (which might be conflicting or not) - we need to make
                // sure this change is visible from now on
                // TODO verify this is really needed
                context.publishRevision(propRev, changeRev);
            }
            if (newestRev == null || isRevisionNewer(context, propRev, newestRev)) {
                if (!propRev.equals(changeRev)) {
                    if (!isValidRevision(context, store,
                            propRev, changeRev, new HashSet<Revision>())) {
                        handler.concurrentModification(propRev);
                    } else {
                        newestRev = propRev;
                    }
                }
            }
        }
        if (newestRev == null) {
            return null;
        }
        if (deletedMap != null) {
            String value = deletedMap.get(newestRev.toString());
            if ("true".equals(value)) {
                // deleted in the newest revision
                return null;
            }
        }
        return newestRev;
    }

    /**
     * Checks if the revision is valid for the given document. A revision is
     * considered valid if the given document is the root of the commit, or the
     * commit root has the revision set. This method may read further documents
     * to perform this check.
     * This method also takes pending branches into consideration.
     * The <code>readRevision</code> identifies the read revision used by the
     * client, which may be a branch revision logged in {@link RevisionContext#getBranches()}.
     * The revision <code>rev</code> is valid if it is part of the branch
     * history of <code>readRevision</code>.
     *
     * @param rev     revision to check.
     * @param readRevision the read revision of the client.
     * @param validRevisions set of revisions already checked against
     *                       <code>readRevision</code> and considered valid.
     * @return <code>true</code> if the revision is valid; <code>false</code>
     *         otherwise.
     */
    boolean isValidRevision(@Nonnull RevisionContext context,
                            @Nonnull DocumentStore store,
                            @Nonnull Revision rev,
                            @Nonnull Revision readRevision,
                            @Nonnull Set<Revision> validRevisions) {
        if (validRevisions.contains(rev)) {
            return true;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> revisions = (Map<String, String>) get(REVISIONS);
        if (isCommitted(context, rev, readRevision, revisions)) {
            validRevisions.add(rev);
            return true;
        } else if (revisions != null && revisions.containsKey(rev.toString())) {
            // rev is in revisions map of this node, but not committed
            // no need to check _commitRoot field
            return false;
        }
        // check commit root
        @SuppressWarnings("unchecked")
        Map<String, Integer> commitRoot = (Map<String, Integer>) get(COMMIT_ROOT);
        String commitRootPath = null;
        if (commitRoot != null) {
            Integer depth = commitRoot.get(rev.toString());
            if (depth != null) {
                String p = Utils.getPathFromId(getId());
                commitRootPath = PathUtils.getAncestorPath(p, PathUtils.getDepth(p) - depth);
            }
        }
        if (commitRootPath == null) {
            // shouldn't happen, either node is commit root for a revision
            // or has a reference to the commit root
            log.warn("Node {} does not have commit root reference for revision {}",
                    getId(), rev);
            return false;
        }
        // get root of commit
        NodeDocument doc = store.find(Collection.NODES,
                Utils.getIdFromPath(commitRootPath));
        if (doc == null) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> rootRevisions = (Map<String, String>) doc.get(REVISIONS);
        if (isCommitted(context, rev, readRevision, rootRevisions)) {
            validRevisions.add(rev);
            return true;
        }
        return false;
    }

    /**
     * Returns a {@link Node} as seen at the given <code>readRevision</code>.
     *
     * @param context      the revision context.
     * @param readRevision the read revision.
     * @return the node or <code>null</code> if the node doesn't exist at the
     *         given read revision.
     */
    @CheckForNull
    public Node getNodeAtRevision(RevisionContext context,
                                  DocumentStore store,
                                  Revision readRevision) {
        Revision min = getLiveRevision(context, store, readRevision, new HashSet<Revision>());
        if (min == null) {
            // deleted
            return null;
        }
        String path = Utils.getPathFromId(getId());
        Node n = new Node(path, readRevision);
        for (String key : keySet()) {
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            Object v = get(key);
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) v;
            if (valueMap != null) {
                if (valueMap instanceof TreeMap) {
                    // TODO instanceof should be avoided
                    // use descending keys (newest first) if map is sorted
                    valueMap = ((TreeMap<String, String>) valueMap).descendingMap();
                }
                String value = getLatestValue(context, valueMap, min, readRevision);
                String propertyName = Utils.unescapePropertyName(key);
                n.setProperty(propertyName, value);
            }
        }

        // when was this node last modified?
        Branch branch = context.getBranches().getBranch(readRevision);
        Revision lastRevision = null;
        Map<Integer, Revision> lastRevs = Maps.newHashMap(getLastRev());
        // overlay with unsaved last modified from this instance
        Revision lastModified = context.getPendingModifications().get(path);
        if (lastModified != null) {
            lastRevs.put(context.getClusterId(), lastModified);
        }
        // filter out revisions newer than branch base
        if (branch != null) {
            Revision base = branch.getBase(readRevision);
            for (Iterator<Revision> it = lastRevs.values().iterator(); it
                    .hasNext();) {
                Revision r = it.next();
                if (isRevisionNewer(context, r, base)) {
                    it.remove();
                }
            }
        }
        for (Revision r : lastRevs.values()) {
            // ignore if newer than readRevision
            if (isRevisionNewer(context, r, readRevision)) {
                continue;
            }
            if (lastRevision == null || isRevisionNewer(context, r, lastRevision)) {
                lastRevision = r;
            }
        }
        if (branch != null) {
            // read from a branch
            // -> possibly overlay with unsaved last revs from branch
            Revision r = branch.getUnsavedLastRevision(path, readRevision);
            if (r != null) {
                lastRevision = r;
            }
        }
        if (lastRevision == null) {
            // use readRevision if none found
            lastRevision = readRevision;
        }
        n.setLastRevision(lastRevision);
        return n;
    }

    /**
     * Get the earliest (oldest) revision where the node was alive at or before
     * the provided revision, if the node was alive at the given revision.
     *
     * @param maxRev the maximum revision to return
     * @param validRevisions the set of revisions already checked against maxRev
     *            and considered valid.
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    public Revision getLiveRevision(RevisionContext context,
                                    DocumentStore store,
                                    Revision maxRev,
                                    Set<Revision> validRevisions) {
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) get(NodeDocument.DELETED);
        if (valueMap == null) {
            return null;
        }
        // first, search the newest deleted revision
        Revision deletedRev = null;
        if (valueMap instanceof TreeMap) {
            // TODO instanceof should be avoided
            // use descending keys (newest first) if map is sorted
            valueMap = ((TreeMap<String, String>) valueMap).descendingMap();
        }
        for (String r : valueMap.keySet()) {
            String value = valueMap.get(r);
            if (!"true".equals(value)) {
                // only look at deleted revisions now
                continue;
            }
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(context, propRev, maxRev)
                    || !isValidRevision(context, store, propRev, maxRev, validRevisions)) {
                continue;
            }
            if (deletedRev == null || isRevisionNewer(context, propRev, deletedRev)) {
                deletedRev = propRev;
            }
        }
        // now search the oldest non-deleted revision that is newer than the
        // newest deleted revision
        Revision liveRev = null;
        for (String r : valueMap.keySet()) {
            String value = valueMap.get(r);
            if ("true".equals(value)) {
                // ignore deleted revisions
                continue;
            }
            Revision propRev = Revision.fromString(r);
            if (deletedRev != null && isRevisionNewer(context, deletedRev, propRev)) {
                // the node was deleted later on
                continue;
            }
            if (isRevisionNewer(context, propRev, maxRev)
                    || !isValidRevision(context, store, propRev, maxRev, validRevisions)) {
                continue;
            }
            if (liveRev == null || isRevisionNewer(context, liveRev, propRev)) {
                liveRev = propRev;
            }
        }
        return liveRev;
    }

    /**
     * Split this document in two.
     *
     * @param context
     * @param commitRevision
     * @param splitDocumentAgeMillis
     * @return
     */
    public UpdateOp[] splitDocument(@Nonnull RevisionContext context,
                                    @Nonnull Revision commitRevision,
                                    long splitDocumentAgeMillis) {
        String id = getId();
        String path = Utils.getPathFromId(id);
        Long previous = (Long) get(NodeDocument.PREVIOUS);
        if (previous == null) {
            previous = 0L;
        } else {
            previous++;
        }
        UpdateOp old = new UpdateOp(path, id + "/" + previous, true);
        setModified(old, commitRevision);
        UpdateOp main = new UpdateOp(path, id, false);
        setModified(main, commitRevision);
        main.set(NodeDocument.PREVIOUS, previous);
        for (Map.Entry<String, Object> e : data.entrySet()) {
            String key = e.getKey();
            if (key.equals(Document.ID)) {
                // ok
            } else if (key.equals(NodeDocument.MODIFIED)) {
                // ok
            } else if (key.equals(NodeDocument.PREVIOUS)) {
                // ok
            } else if (key.equals(NodeDocument.LAST_REV)) {
                // only maintain the lastRev in the main document
                main.setMap(NodeDocument.LAST_REV,
                        String.valueOf(commitRevision.getClusterId()),
                        commitRevision.toString());
            } else {
                // UpdateOp.DELETED,
                // UpdateOp.REVISIONS,
                // and regular properties
                @SuppressWarnings("unchecked")
                Map<String, Object> valueMap = (Map<String, Object>) e.getValue();
                Revision latestRev = null;
                for (String r : valueMap.keySet()) {
                    Revision propRev = Revision.fromString(r);
                    if (latestRev == null || isRevisionNewer(context, propRev, latestRev)) {
                        latestRev = propRev;
                    }
                }
                for (String r : valueMap.keySet()) {
                    Revision propRev = Revision.fromString(r);
                    Object v = valueMap.get(r);
                    if (propRev.equals(latestRev)) {
                        main.setMap(key, propRev.toString(), v);
                    } else {
                        long ageMillis = Revision.getCurrentTimestamp() - propRev.getTimestamp();
                        if (ageMillis > splitDocumentAgeMillis) {
                            old.setMapEntry(key, propRev.toString(), v);
                        } else {
                            main.setMap(key, propRev.toString(), v);
                        }
                    }
                }
            }
        }
        if (Commit.PURGE_OLD_REVISIONS) {
            old = null;
        }
        return new UpdateOp[]{old, main};
    }

    //-------------------------< UpdateOp modifiers >---------------------------

    public static void setModified(@Nonnull UpdateOp op,
                                   @Nonnull Revision revision) {
        checkNotNull(op).set(MODIFIED, Commit.getModified(checkNotNull(revision).getTimestamp()));
    }

    public static void setRevision(@Nonnull UpdateOp op,
                                   @Nonnull Revision revision,
                                   @Nonnull String commitValue) {
        checkNotNull(op).setMapEntry(REVISIONS,
                checkNotNull(revision).toString(), checkNotNull(commitValue));
    }

    public static void unsetRevision(@Nonnull UpdateOp op,
                                     @Nonnull Revision revision) {
        checkNotNull(op).unsetMapEntry(REVISIONS,
                checkNotNull(revision).toString());
    }

    public static void setLastRev(@Nonnull UpdateOp op,
                                  @Nonnull Revision revision) {
        checkNotNull(op).setMapEntry(LAST_REV,
                String.valueOf(checkNotNull(revision).getClusterId()),
                revision.toString());
    }

    public static void setCommitRoot(@Nonnull UpdateOp op,
                                     @Nonnull Revision revision,
                                     int commitRootDepth) {
        checkNotNull(op).setMapEntry(COMMIT_ROOT,
                checkNotNull(revision).toString(), commitRootDepth);
    }

    //----------------------------< internal >----------------------------------

    /**
     * Checks that revision x is newer than another revision.
     *
     * @param x the revision to check
     * @param previous the presumed earlier revision
     * @return true if x is newer
     */
    private static boolean isRevisionNewer(@Nonnull RevisionContext context,
                                           @Nonnull Revision x,
                                           @Nonnull Revision previous) {
        return context.getRevisionComparator().compare(x, previous) > 0;
    }

    /**
     * TODO: turn into instance method?
     * Returns <code>true</code> if the given revision
     * {@link Utils#isCommitted(String)} in the revisions map and is visible
     * from the <code>readRevision</code>.
     *
     * @param revision  the revision to check.
     * @param readRevision the read revision.
     * @param revisions the revisions map, or <code>null</code> if none is set.
     * @return <code>true</code> if the revision is committed, otherwise
     *         <code>false</code>.
     */
    private static boolean isCommitted(@Nonnull RevisionContext context,
                                       @Nonnull Revision revision,
                                       @Nonnull Revision readRevision,
                                       @Nullable Map<String, String> revisions) {
        if (revision.equals(readRevision)) {
            return true;
        }
        if (revisions == null) {
            return false;
        }
        String value = revisions.get(revision.toString());
        if (value == null) {
            return false;
        }
        if (Utils.isCommitted(value)) {
            // resolve commit revision
            revision = Utils.resolveCommitRevision(revision, value);
            if (context.getBranches().getBranch(readRevision) == null) {
                // readRevision is not from a branch
                // compare resolved revision as is
                return !isRevisionNewer(context, revision, readRevision);
            }
        } else {
            // branch commit
            if (Revision.fromString(value).getClusterId() != context.getClusterId()) {
                // this is an unmerged branch commit from another cluster node,
                // hence never visible to us
                return false;
            }
        }
        return includeRevision(context, revision, readRevision);
    }

    private static boolean includeRevision(RevisionContext context,
                                    Revision x,
                                    Revision requestRevision) {
        Branch b = context.getBranches().getBranch(x);
        if (b != null) {
            // only include if requested revision is also a branch revision
            // with a history including x
            if (b.containsCommit(requestRevision)) {
                // in same branch, include if the same revision or
                // requestRevision is newer
                return x.equals(requestRevision) || isRevisionNewer(context, requestRevision, x);
            }
            // not part of branch identified by requestedRevision
            return false;
        }
        // assert: x is not a branch commit
        b = context.getBranches().getBranch(requestRevision);
        if (b != null) {
            // reset requestRevision to branch base revision to make
            // sure we don't include revisions committed after branch
            // was created
            requestRevision = b.getBase(requestRevision);
        }
        return context.getRevisionComparator().compare(requestRevision, x) >= 0;
    }

    /**
     * Get the latest property value that is larger or equal the min revision,
     * and smaller or equal the max revision.
     *
     * @param valueMap the revision-value map
     * @param min the minimum revision (null meaning unlimited)
     * @param max the maximum revision
     * @return the value, or null if not found
     */
    private String getLatestValue(@Nonnull RevisionContext context,
                                  @Nonnull Map<String, String> valueMap,
                                  @Nullable Revision min,
                                  @Nonnull Revision max) {
        String value = null;
        Revision latestRev = null;
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (min != null && isRevisionNewer(context, min, propRev)) {
                continue;
            }
            if (latestRev != null && !isRevisionNewer(context, propRev, latestRev)) {
                continue;
            }
            if (includeRevision(context, propRev, max)) {
                latestRev = propRev;
                value = valueMap.get(r);
            }
        }
        return value;
    }
}
