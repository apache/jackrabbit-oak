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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A document storing data about a node.
 */
public class NodeDocument extends Document {

    /**
     * Marker document, which indicates the document does not exist.
     */
    public static final NodeDocument NULL = new NodeDocument(new MemoryDocumentStore());

    static final Logger LOG = LoggerFactory.getLogger(NodeDocument.class);
    
    /**
     * A size threshold after which to consider a document a split candidate.
     * TODO: check which value is the best one
     */
    static final int SPLIT_CANDIDATE_THRESHOLD = 32 * 1024;

    /**
     * Only split off at least this number of revisions.
     */
    static final int REVISIONS_SPLIT_OFF_SIZE = 1000;

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

    private static final SortedMap<Revision, Range> EMPTY_RANGE_MAP = 
            Collections.unmodifiableSortedMap(new TreeMap<Revision, Range>());

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
    private static final String DELETED = "_deleted";

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

    final DocumentStore store;

    private final long time = System.currentTimeMillis();

    NodeDocument(@Nonnull DocumentStore store) {
        this.store = checkNotNull(store);
    }

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
     * committed in <strong>this</strong> document including previous documents.
     *
     * @param revision the revision.
     * @return <code>true</code> if committed; <code>false</code> otherwise.
     */
    public boolean isCommitted(@Nonnull Revision revision) {
        String rev = checkNotNull(revision).toString();
        String value = getLocalRevisions().get(rev);
        if (value != null) {
            return Utils.isCommitted(value);
        }
        // check previous docs
        for (NodeDocument prev : getPreviousDocs(revision, REVISIONS)) {
            if (prev.containsRevision(revision)) {
                return prev.isCommitted(revision);
            }
        }
        return false;
    }

    /**
     * Returns <code>true</code> if this document contains an entry for the
     * given <code>revision</code> in the {@link #REVISIONS} map. Please note
     * that an entry in the {@link #REVISIONS} map does not necessarily mean
     * the the revision is committed. Use {@link #isCommitted(Revision)} to get
     * the commit state of a revision.
     *
     * @param revision the revision to check.
     * @return <code>true</code> if this document contains the given revision.
     */
    public boolean containsRevision(@Nonnull Revision revision) {
        String rev = checkNotNull(revision).toString();
        if (getLocalRevisions().containsKey(rev)) {
            return true;
        }
        for (NodeDocument prev : getPreviousDocs(revision, REVISIONS)) {
            if (prev.containsRevision(revision)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Same as {@link #containsRevision(Revision)}, but with a String parameter.
     *
     * @param revision the revision.
     * @return <code>true</code> if this document contains the given revision.
     */
    public boolean containsRevision(@Nonnull String revision) {
        return containsRevision(Revision.fromString(checkNotNull(revision)));
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
        // only look at revisions in this document.
        // uncommitted revisions are not split off
        Map<String, String> valueMap = getLocalRevisions();
        SortedMap<Revision, Revision> revisions =
                new TreeMap<Revision, Revision>(context.getRevisionComparator());
        for (Map.Entry<String, String> commit : valueMap.entrySet()) {
            if (!Utils.isCommitted(commit.getValue())) {
                Revision r = Revision.fromString(commit.getKey());
                if (r.getClusterId() == context.getClusterId()) {
                    Revision b = Revision.fromString(commit.getValue());
                    revisions.put(r, b);
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
        Map<String, String> valueMap = getCommitRoot();
        String depth = valueMap.get(revision);
        if (depth != null) {
            String p = Utils.getPathFromId(getId());
            return PathUtils.getAncestorPath(p,
                    PathUtils.getDepth(p) - Integer.parseInt(depth));
        } else {
            return null;
        }
    }

    /**
     * Get the revision of the latest change made to this node.
     *
     * @param context the revision context
     * @param changeRev the revision of the current change
     * @param handler the conflict handler, which is called for concurrent changes
     *                preceding <code>before</code>.
     * @return the revision, or null if deleted
     */
    @SuppressWarnings("unchecked")
    @CheckForNull
    public Revision getNewestRevision(RevisionContext context,
                                      Revision changeRev,
                                      CollisionHandler handler) {
        SortedSet<String> revisions = new TreeSet<String>(Collections.reverseOrder());
        revisions.addAll(getRevisions().keySet());
        revisions.addAll(getCommitRoot().keySet());
        Map<String, String> deletedMap = getDeleted();
        revisions.addAll(deletedMap.keySet());
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
                    if (!isValidRevision(context, propRev, changeRev, new HashSet<Revision>())) {
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
        String value = deletedMap.get(newestRev.toString());
        if ("true".equals(value)) {
            // deleted in the newest revision
            return null;
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
                            @Nonnull Revision rev,
                            @Nonnull Revision readRevision,
                            @Nonnull Set<Revision> validRevisions) {
        if (validRevisions.contains(rev)) {
            return true;
        }
        NodeDocument doc = getCommitRoot(rev);
        if (doc == null) {
            return false;
        }
        if (doc.isCommitted(context, rev, readRevision)) {
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
    public Node getNodeAtRevision(RevisionContext context, Revision readRevision) {
        Revision min = getLiveRevision(context, readRevision, new HashSet<Revision>());
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
                if (valueMap instanceof NavigableMap) {
                    // TODO instanceof should be avoided
                    // use descending keys (newest first) if map is sorted
                    valueMap = ((NavigableMap<String, String>) valueMap).descendingMap();
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
     * Returns <code>true</code> if this node is considered deleted at the
     * given <code>readRevision</code>.
     *
     * @param context the revision context.
     * @param readRevision the read revision.
     * @param validRevisions the set of revisions already checked against
     *                       <code>readRevision</code> and considered valid.
     * @return <code>true</code> if deleted, <code>false</code> otherwise.
     */
    public boolean isDeleted(RevisionContext context,
                             Revision readRevision,
                             Set<Revision> validRevisions) {
        Map<String, String> valueMap = getDeleted();
        if (valueMap.isEmpty()) {
            return false;
        }
        if (valueMap instanceof NavigableMap) {
            // TODO instanceof should be avoided
            // use descending keys (newest first) if map is sorted
            valueMap = ((NavigableMap<String, String>) valueMap).descendingMap();
        }
        Revision mostRecent = null;
        boolean deleted = false;
        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
            Revision r = Revision.fromString(entry.getKey());
            if (isRevisionNewer(context, r, readRevision)) {
                // ignore -> newer than readRevision
                continue;
            }
            if (mostRecent != null && isRevisionNewer(context, mostRecent, r)) {
                continue;
            }
            if (isValidRevision(context, r, readRevision, validRevisions)) {
                mostRecent = r;
                deleted = "true".equals(entry.getValue());
            }
        }
        return mostRecent == null || deleted;
    }

    /**
     * Get the earliest (oldest) revision where the node was alive at or before
     * the provided revision, if the node was alive at the given revision.
     * 
     * @param context the revision context
     * @param maxRev the maximum revision to return
     * @param validRevisions the set of revisions already checked against maxRev
     *            and considered valid.
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    @CheckForNull
    public Revision getLiveRevision(RevisionContext context, Revision maxRev,
                                    Set<Revision> validRevisions) {
        Map<String, String> valueMap = getDeleted();
        if (valueMap.isEmpty()) {
            return null;
        }
        // first, search the newest deleted revision
        Revision deletedRev = null;
        if (valueMap instanceof NavigableMap) {
            // TODO instanceof should be avoided
            // use descending keys (newest first) if map is sorted
            valueMap = ((NavigableMap<String, String>) valueMap).descendingMap();
        }
        for (String r : valueMap.keySet()) {
            String value = valueMap.get(r);
            if (!"true".equals(value)) {
                // only look at deleted revisions now
                continue;
            }
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(context, propRev, maxRev)
                    || !isValidRevision(context, propRev, maxRev, validRevisions)) {
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
                    || !isValidRevision(context, propRev, maxRev, validRevisions)) {
                continue;
            }
            if (liveRev == null || isRevisionNewer(context, liveRev, propRev)) {
                liveRev = propRev;
            }
        }
        return liveRev;
    }

    /**
     * Returns <code>true</code> if the given operation is conflicting with this
     * document.
     *
     * @param op the update operation.
     * @param baseRevision the base revision for the update operation.
     * @param context the revision context.
     * @return <code>true</code> if conflicting, <code>false</code> otherwise.
     */
    public boolean isConflicting(@Nonnull UpdateOp op,
                                 @Nonnull Revision baseRevision,
                                 @Nonnull RevisionContext context) {
        // did existence of node change after baseRevision?
        Map<String, String> deleted = getDeleted();
        for (Map.Entry<String, String> entry : deleted.entrySet()) {
            if (isRevisionNewer(context, Revision.fromString(entry.getKey()), baseRevision)) {
                return true;
            }
        }

        for (Map.Entry<String, UpdateOp.Operation> entry : op.changes.entrySet()) {
            if (entry.getValue().type != UpdateOp.Operation.Type.SET_MAP_ENTRY) {
                continue;
            }
            int idx = entry.getKey().indexOf('.');
            String name = entry.getKey().substring(0, idx);
            if (DELETED.equals(name)) {
                // existence of node changed, this always conflicts with
                // any other concurrent change
                return true;
            }
            if (!Utils.isPropertyName(name)) {
                continue;
            }
            // was this property touched after baseRevision?
            @SuppressWarnings("unchecked")
            Map<String, Object> changes = (Map<String, Object>) get(name);
            if (changes == null) {
                continue;
            }
            for (String rev : changes.keySet()) {
                if (isRevisionNewer(context, Revision.fromString(rev), baseRevision)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns update operations to split this document. The implementation may
     * decide to not return any operations if no splitting is required.
     *
     * @param context the revision context.
     * @return the split operations.
     */
    @Nonnull
    public Iterable<UpdateOp> split(@Nonnull RevisionContext context) {
        // only consider if there are enough commits
        if (getLocalRevisions().size() + getLocalCommitRoot().size() <= REVISIONS_SPLIT_OFF_SIZE) {
            return Collections.emptyList();
        }
        String id = getId();
        SortedMap<Revision, Range> previous = getPreviousRanges();
        // what's the most recent previous revision?
        Revision recentPrevious = null;
        for (Revision rev : previous.keySet()) {
            if (rev.getClusterId() != context.getClusterId()) {
                continue;
            }
            if (recentPrevious == null
                    || isRevisionNewer(context, rev, recentPrevious)) {
                recentPrevious = rev;
            }
        }
        Map<String, NavigableMap<Revision, String>> splitValues
                = new HashMap<String, NavigableMap<Revision, String>>();
        for (String property : new String[]{REVISIONS, COMMIT_ROOT, DELETED}) {
            NavigableMap<Revision, String> splitMap
                    = new TreeMap<Revision, String>(context.getRevisionComparator());
            splitValues.put(property, splitMap);
            Map<String, String> valueMap = getLocalMap(property);
            // collect committed changes of this cluster node after the
            // most recent previous split revision
            for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                Revision rev = Revision.fromString(entry.getKey());
                if (rev.getClusterId() != context.getClusterId()) {
                    continue;
                }
                if (recentPrevious == null
                        || isRevisionNewer(context, rev, recentPrevious)) {
                    if (isCommitted(rev)) {
                        splitMap.put(rev, entry.getValue());
                    }
                }
            }
        }

        List<UpdateOp> splitOps = Collections.emptyList();
        int numValues = 0;
        Revision high = null;
        Revision low = null;
        for (NavigableMap<Revision, String> splitMap : splitValues.values()) {
            // keep the most recent in the main document
            if (!splitMap.isEmpty()) {
                splitMap.remove(splitMap.lastKey());
            }
            if (splitMap.isEmpty()) {
                continue;
            }
            // remember highest / lowest revision
            if (high == null || isRevisionNewer(context, splitMap.lastKey(), high)) {
                high = splitMap.lastKey();
            }
            if (low == null || isRevisionNewer(context, low, splitMap.firstKey())) {
                low = splitMap.firstKey();
            }
            numValues += splitMap.size();
        }
        if (high != null && low != null && numValues >= REVISIONS_SPLIT_OFF_SIZE) {
            // enough revisions to split off
            splitOps = new ArrayList<UpdateOp>(2);
            // move to another document
            UpdateOp main = new UpdateOp(id, false);
            main.setMapEntry(PREVIOUS, high.toString(), low.toString());
            UpdateOp old = new UpdateOp(Utils.getPreviousIdFor(id, high), true);
            old.set(ID, old.getKey());
            for (String property : splitValues.keySet()) {
                NavigableMap<Revision, String> splitMap = splitValues.get(property);
                for (Map.Entry<Revision, String> entry : splitMap.entrySet()) {
                    String r = entry.getKey().toString();
                    main.removeMapEntry(property, r);
                    old.setMapEntry(property, r, entry.getValue());
                }
                splitOps.add(old);
                splitOps.add(main);
            }
        }
        return splitOps;
    }

    @Override
    @Nonnull
    protected Map<?, ?> transformAndSeal(@Nonnull Map<Object, Object> map,
                                         @Nullable String key,
                                         int level) {
        if (level == 1) {
            if (PREVIOUS.equals(key)) {
                SortedMap<Revision, Range> transformed = new TreeMap<Revision, Range>(
                        new Comparator<Revision>() {
                            @Override
                            public int compare(Revision o1, Revision o2) {
                                // in reverse order!
                                int c = o2.compareRevisionTime(o1);
                                if (c == 0) {
                                    c = o1.getClusterId() < o2.getClusterId()
                                            ? -1
                                            : (o1.getClusterId() == o2.getClusterId() ? 0 : 1);
                                }
                                return c;
                            }
                        });
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    Revision high = Revision.fromString(entry.getKey().toString());
                    Revision low = Revision.fromString(entry.getValue().toString());
                    transformed.put(high, new Range(high, low));
                }
                return Collections.unmodifiableSortedMap(transformed);
            }
        }
        return super.transformAndSeal(map, key, level);
    }

    /**
     * Returns previous revision ranges for this document. The revision keys are
     * sorted descending, newest first!
     *
     * @return the previous ranges for this document.
     */
    @Nonnull
    SortedMap<Revision, Range> getPreviousRanges() {
        @SuppressWarnings("unchecked")
        SortedMap<Revision, Range> previous = (SortedMap<Revision, Range>) get(PREVIOUS);
        if (previous == null) {
            previous = EMPTY_RANGE_MAP;
        }
        return previous;
    }

    /**
     * Returns previous {@link NodeDocument}, which include entries for the
     * property in the given revision.
     * If the <code>revision</code> is <code>null</code>, then all previous
     * documents are returned.
     *
     * @param revision the revision to match or <code>null</code>.
     * @param property the name of a property.
     * @return previous documents.
     */
    Iterable<NodeDocument> getPreviousDocs(final @Nullable Revision revision,
                                           final @Nonnull String property) {
        checkNotNull(property);
        Iterable<NodeDocument> docs = Iterables.transform(
                Iterables.filter(getPreviousRanges().entrySet(),
                        new Predicate<Map.Entry<Revision, Range>>() {
                            @Override
                            public boolean apply(Map.Entry<Revision, Range> input) {
                                return revision == null
                                        || input.getValue().includes(revision);
                            }
                        }), new Function<Map.Entry<Revision, Range>, NodeDocument>() {
            @Nullable
            @Override
            public NodeDocument apply(Map.Entry<Revision, Range> input) {
                Revision r = input.getKey();
                String prevId = Utils.getPreviousIdFor(getId(), r);
                NodeDocument prev = store.find(Collection.NODES, prevId);
                if (prev == null) {
                    LOG.warn("Document with previous revisions not found: " + prevId);
                }
                return prev;
            }
        });
        // filter out null docs and check if the revision is actually in there
        return Iterables.filter(docs, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(@Nullable NodeDocument input) {
                if (input == null) {
                    return false;
                }
                return revision == null
                        || input.getLocalMap(property).containsKey(revision.toString());
            }
        });
    }

    /**
     * Returns the local value map for the given key. Returns <code>null</code>
     * if no such value map exists.
     *
     * @param key the key.
     * @return local value map.
     */
    @Nonnull
    Map<String, String> getLocalMap(String key) {
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) get(key);
        if (map == null) {
            map = Collections.emptyMap();
        }
        return map;
    }

    /**
     * @return the {@link #REVISIONS} stored on this document.
     */
    @Nonnull
    Map<String, String> getLocalRevisions() {
        return getLocalMap(REVISIONS);
    }

    @Nonnull
    Map<String, String> getLocalCommitRoot() {
        return getLocalMap(COMMIT_ROOT);
    }

    @Nonnull
    Map<String, String> getLocalDeleted() {
        return getLocalMap(DELETED);
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
                checkNotNull(revision).toString(), String.valueOf(commitRootDepth));
    }

    public static void setDeleted(@Nonnull UpdateOp op,
                                  @Nonnull Revision revision,
                                  boolean deleted) {
        checkNotNull(op).setMapEntry(DELETED, checkNotNull(revision).toString(), String.valueOf(deleted));
    }

    //----------------------------< internal >----------------------------------

    /**
     * Returns the commit root document for the given revision. This may either
     * be this document or another one.
     *
     * @param rev a revision.
     * @return the commit root or <code>null</code> if there is none.
     */
    @CheckForNull
    private NodeDocument getCommitRoot(@Nonnull Revision rev) {
        if (containsRevision(rev)) {
            return this;
        }
        String commitRootPath = getCommitRootPath(rev.toString());
        if (commitRootPath == null) {
            // shouldn't happen, either node is commit root for a revision
            // or has a reference to the commit root
            LOG.warn("Node {} does not have commit root reference for revision {}",
                    getId(), rev);
            return null;
        }
        // get root of commit
        return store.find(Collection.NODES, Utils.getIdFromPath(commitRootPath));
    }

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
     * Returns <code>true</code> if the given revision
     * {@link Utils#isCommitted(String)} in the revisions map (including
     * revisions split off to previous documents) and is visible from the
     * <code>readRevision</code>.
     *
     * @param revision  the revision to check.
     * @param readRevision the read revision.
     * @return <code>true</code> if the revision is committed, otherwise
     *         <code>false</code>.
     */
    private boolean isCommitted(@Nonnull RevisionContext context,
                                @Nonnull Revision revision,
                                @Nonnull Revision readRevision) {
        if (revision.equalsIgnoreBranch(readRevision)) {
            return true;
        }
        String value = getCommitValue(revision);
        if (value == null) {
            return false;
        }
        if (Utils.isCommitted(value)) {
            if (context.getBranches().getBranch(readRevision) == null
                    && !readRevision.isBranch()) {
                // resolve commit revision
                revision = Utils.resolveCommitRevision(revision, value);
                // readRevision is not from a branch
                // compare resolved revision as is
                return !isRevisionNewer(context, revision, readRevision);
            } else {
                // on same merged branch?
                if (value.equals(getCommitValue(readRevision.asTrunkRevision()))) {
                    // compare unresolved revision
                    return !isRevisionNewer(context, revision, readRevision);
                }
            }
        } else {
            // branch commit (not merged)
            if (Revision.fromString(value).getClusterId() != context.getClusterId()) {
                // this is an unmerged branch commit from another cluster node,
                // hence never visible to us
                return false;
            }
        }
        return includeRevision(context, Utils.resolveCommitRevision(revision, value), readRevision);
    }

    /**
     * Returns the commit value for the given <code>revision</code>.
     *
     * @param revision a revision.
     * @return the commit value or <code>null</code> if the revision is unknown.
     */
    @CheckForNull
    private String getCommitValue(Revision revision) {
        String r = revision.toString();
        String value = getLocalRevisions().get(r);
        if (value == null) {
            // check previous
            for (NodeDocument prev : getPreviousDocs(revision, REVISIONS)) {
                value = prev.getLocalRevisions().get(r);
                if (value != null) {
                    break;
                }
            }
        }
        return value;
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
                return x.equalsIgnoreBranch(requestRevision)
                        || isRevisionNewer(context, requestRevision, x);
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
    @CheckForNull
    private static String getLatestValue(@Nonnull RevisionContext context,
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

    @Nonnull
    private Map<String, String> getRevisions() {
        return ValueMap.create(this, REVISIONS);
    }

    @Nonnull
    private Map<String, String> getDeleted() {
        return ValueMap.create(this, DELETED);
    }

    @Nonnull
    private Map<String, String> getCommitRoot() {
        return ValueMap.create(this, COMMIT_ROOT);
    }
}
