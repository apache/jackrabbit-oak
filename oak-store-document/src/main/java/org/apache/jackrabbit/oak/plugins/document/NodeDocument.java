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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.guava.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.mergeSorted;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator.REVERSE;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.abortingIterable;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.resolveCommitRevision;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.collect.Ordering;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.commons.log.LogSilencer;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A document storing data about a node.
 */
public final class NodeDocument extends Document {

    /**
     * Marker document, which indicates the document does not exist.
     */
    public static final NodeDocument NULL = new NodeDocument(new MemoryDocumentStore());

    static {
        NULL.seal();
    }

    static final Logger LOG = LoggerFactory.getLogger(NodeDocument.class);
    static final Logger PREV_NO_PROP_LOG = LoggerFactory.getLogger(NodeDocument.class + ".prevNoProp");

    private static final LogSilencer LOG_SILENCER = new LogSilencer();

    /**
     * All NodeDocument ID value would be greater than this value
     * It can be used as startKey in DocumentStore#query methods
     */
    public static final String MIN_ID_VALUE = "0000000";

    /**
     * All NodeDocument ID value would be less than this value
     * It can be used as endKey in DocumentStore#query methods
     */
    public static final String MAX_ID_VALUE = ";";

    /**
     * A size threshold after which to consider a document a split candidate.
     * TODO: check which value is the best one
     */
    static final int SPLIT_CANDIDATE_THRESHOLD = 8 * 1024;

    /**
     * A document size threshold after which a split is forced even if
     * {@link #NUM_REVS_THRESHOLD} is not reached.
     */
    static final int DOC_SIZE_THRESHOLD = 1024 * 1024;

    /**
     * Only split off at least this number of revisions.
     */
    static final int NUM_REVS_THRESHOLD = 100;

    /**
     * Create an intermediate previous document when there are this many
     * previous documents of equal height.
     */
    static final int PREV_SPLIT_FACTOR = 10;

    /**
     * Revision collision markers set by commits with modifications, which
     * overlap with un-merged branch commits.
     * Key: revision, value: always true
     */
    public static final String COLLISIONS = "_collisions";

    /**
     * The modified time in seconds (5 second resolution).
     */
    public static final String MODIFIED_IN_SECS = "_modified";

    /**
     * The resolution of the modified time.
     */
    static final int MODIFIED_IN_SECS_RESOLUTION = 5;

    private static final NavigableMap<Revision, Range> EMPTY_RANGE_MAP =
            Maps.unmodifiableNavigableMap(new TreeMap<Revision, Range>(REVERSE));

    /**
     * The list of revision to root commit depth mappings to find out if a
     * revision is actually committed. Depth 0 means the commit is in the root node,
     * depth 1 means one node below the root, and so on.
     */
    static final String COMMIT_ROOT = "_commitRoot";

    /**
     * The number of previous documents (documents that contain old revisions of
     * this node). This property is only set if multiple documents per node
     * exist. This is the case when a node is updated very often in a short
     * time, such that the document gets very big.
     * <p>
     * Key: high revision
     * <p>
     * Value: low revision / height (see {@link Range#getLowValue()}
     */
    private static final String PREVIOUS = "_prev";

    /**
     * Whether this node is deleted. Key: revision, value: true/false.
     */
    static final String DELETED = "_deleted";

    /**
     * Flag indicating that whether this node was ever deleted. Its just used as
     * a hint. If set to true then it indicates that node was once deleted.
     * <p>
     * Note that a true value does not mean that node should be considered
     * deleted as it might have been resurrected in later revision. Further note
     * that it might get reset by maintenance tasks once they discover that it
     * indeed was resurrected.
     */
    public static final String DELETED_ONCE = "_deletedOnce";

    /**
     * The list of recent revisions for this node, where this node is the
     * root of the commit.
     * <p>
     * Key: revision.
     * <p>
     * Value: "c" for a regular (non-branch) commit,
     * "c-" + base revision of the successfully merged branch commit,
     * "b" + base revision of an un-merged branch commit
     */
    static final String REVISIONS = "_revisions";

    /**
     * The last revision.
     * <p>
     * Key: machine id, in the form "r0-0-1".
     * <p>
     * Value: the revision.
     */
    private static final String LAST_REV = "_lastRev";

    /**
     * Flag indicating that there are child nodes present. Its just used as a hint.
     * If false then that indicates that there are no child. However if its true its
     * not necessary that there are child nodes. It just means at some moment this
     * node had a child node
     */
    private static final String CHILDREN_FLAG = "_children";

    /**
     * The node path, in case the id can not be converted to a path.
     */
    public static final String PATH = "_path";

    public static final String HAS_BINARY_FLAG = "_bin";

    /**
     * Contains {@link #PREVIOUS} entries that are considered stale (pointing
     * to a previous document that had been deleted) and should be removed
     * during the next split run.
     */
    private static final String STALE_PREV = "_stalePrev";

    /**
     * Contains revision entries for changes done by branch commits.
     */
    static final String BRANCH_COMMITS = "_bc";

    /**
     * The revision set by the background document sweeper. The revision
     * indicates up to which revision documents have been cleaned by the sweeper
     * and all previous non-branch revisions by this cluster node can be
     * considered committed.
     */
    static final String SWEEP_REV = "_sweepRev";

    //~----------------------------< Split Document Types >

    /**
     * Defines the type of split document. Its value is an integer whose value is
     * defined as per
     *
     * @see org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType
     */
    public static final String SD_TYPE = "_sdType";

    /**
     * Property name which refers to timestamp (long) of the latest revision kept
     * in the document
     */
    public static final String SD_MAX_REV_TIME_IN_SECS = "_sdMaxRevTime";

    /**
     * The path prefix for previous documents.
     */
    private static final Path PREVIOUS_PREFIX = new Path("p");

    /**
     * Return time in seconds with 5 second resolution
     *
     * @param timestamp time in millis to convert
     * @return the time in seconds with the given resolution.
     */
    public static long getModifiedInSecs(long timestamp) {
        // 5 second resolution
        long timeInSec = TimeUnit.MILLISECONDS.toSeconds(timestamp);
        return timeInSec - timeInSec % MODIFIED_IN_SECS_RESOLUTION;
    }

    /**
     * A document which is created from splitting a main document can be classified
     * into multiple types depending on the content i.e. weather it contains
     * REVISIONS, COMMIT_ROOT, property history etc
     */
    public enum SplitDocType {
        /**
         * Not a split document
         */
        NONE(-1),
        /**
         * A split document which contains all types of data
         */
        DEFAULT(10),
        /**
         * A split document which contains all types of data. In addition
         * when the split document was created the main document did not had
         * any child.
         * This type is deprecated because these kind of documents cannot be
         * garbage collected independently. The main document may still
         * reference _commitRoot entries in the previous document. See OAK-1794
         */
        @Deprecated
        DEFAULT_NO_CHILD(20),
        /**
         * A split document which does not contain REVISIONS history.
         * This type is deprecated because these kind of documents cannot be
         * garbage collected independently. The main document may still
         * reference _commitRoot entries in the previous document. See OAK-1794
         */
        @Deprecated
        PROP_COMMIT_ONLY(30),
        /**
         * Its an intermediate split document which only contains version ranges
         * and does not contain any other attributes
         */
        INTERMEDIATE(40),
        /**
         * A split document which contains all types of data. In addition
         * when the split document was created the main document did not had
         * any child.
         */
        DEFAULT_LEAF(50),
        /**
         * A split document which does not contain REVISIONS history.
         */
        COMMIT_ROOT_ONLY(60),
        /**
         * A split document which contains all types of data, but no branch
         * commits.
         */
        DEFAULT_NO_BRANCH(70),
        ;

        final int type;

        SplitDocType(int type){
            this.type = type;
        }

        public int typeCode() {
            return type;
        }

        static SplitDocType valueOf(Integer type){
            if(type == null){
                return NONE;
            }
            for(SplitDocType docType : values()){
                if(docType.type == type){
                    return docType;
                }
            }
            throw new IllegalArgumentException("Not a valid SplitDocType :" + type);
        }
    }

    public static final long HAS_BINARY_VAL = 1;

    final DocumentStore store;

    /**
     * Parsed and sorted set of previous revisions (without stale references
     * to removed previous documents).
     */
    private NavigableMap<Revision, Range> previous;

    /**
     * Time at which this object was check for cache consistency
     */
    private final AtomicLong lastCheckTime = new AtomicLong(Revision.getCurrentTimestamp());

    private final long creationTime;

    NodeDocument(@NotNull DocumentStore store) {
        this(store, Revision.getCurrentTimestamp());
    }

    /**
     * Required for serialization
     *
     * @param store the document store.
     * @param creationTime time at which it was created. Would be different from current time
     *                     in case of being resurrected from a serialized for
     */
    public NodeDocument(@NotNull DocumentStore store, long creationTime) {
        this.store = requireNonNull(store);
        this.creationTime = creationTime;
    }

    /**
     * Gets the value map for the given key. This method is similar to {@link
     * #get(String)} but will always return a value map. The returned value map
     * may span multiple documents if the values of the given <code>key</code>
     * were split off to {@link #PREVIOUS} documents.
     *
     * @param key a string key.
     * @return the map associated with the key.
     */
    @NotNull
    public Map<Revision, String> getValueMap(@NotNull String key) {
        return ValueMap.create(this, key);
    }

    /**
     * @return the system time this object was created.
     */
    public long getCreated() {
        return creationTime;
    }

    /**
     * See also {@link #MODIFIED_IN_SECS}.
     *
     * @return the time in seconds this document was last modified with five
     *          seconds precision. Returns {@code null} if none is set.
     */
    @Nullable
    public Long getModified() {
        return (Long) get(MODIFIED_IN_SECS);
    }

    /**
     * Returns {@code true} if this node possibly has children.
     * If false then that indicates that there are no child
     *
     * @return {@code true} if this node has children
     */
    public boolean hasChildren() {
        Boolean childrenFlag = (Boolean) get(CHILDREN_FLAG);
        return childrenFlag != null && childrenFlag;
    }

    /**
     * Returns {@code true} if this document was ever deleted in past.
     */
    public boolean wasDeletedOnce() {
        Boolean deletedOnceFlag = (Boolean) get(DELETED_ONCE);
        return deletedOnceFlag != null && deletedOnceFlag;
    }

    /**
     * Checks if this document has been modified after the given lastModifiedTime
     *
     * @param lastModifiedTime time to compare against in millis
     * @return {@code true} if this document was modified after the given
     *  lastModifiedTime
     */
    public boolean hasBeenModifiedSince(long lastModifiedTime){
        Long modified = (Long) get(MODIFIED_IN_SECS);
        return modified != null && modified > TimeUnit.MILLISECONDS.toSeconds(lastModifiedTime);
    }

    /**
     * Checks if revision time of all entries in this document is less than the passed
     * time
     *
     * @param maxRevisionTime timemstamp (in millis) of revision to check
     * @return {@code true} if timestamp of maximum revision stored in this document
     * is less than than the passed revision timestamp
     */
    public boolean hasAllRevisionLessThan(long maxRevisionTime){
        Long maxRevTimeStamp = (Long) get(SD_MAX_REV_TIME_IN_SECS);
        return maxRevTimeStamp != null && maxRevTimeStamp < TimeUnit.MILLISECONDS.toSeconds(maxRevisionTime);
    }

    /**
     * Determines if this document is a split document
     *
     * @return {@code true} if this document is a split document
     */
    public boolean isSplitDocument(){
        return getSplitDocType() != SplitDocType.NONE;
    }

    /**
     * Determines the type of split document
     *
     * @return type of Split Document
     */
    public SplitDocType getSplitDocType() {
        Object t = get(SD_TYPE);
        return t == null ? SplitDocType.valueOf((Integer) null) : SplitDocType.valueOf(((Number) t).intValue());
    }

    /**
     * Mark this instance as up-to-date (matches the state in persistence
     * store).
     *
     * @param checkTime time at which the check was performed
     */
    public void markUpToDate(long checkTime) {
        lastCheckTime.set(checkTime);
    }

    /**
     * Returns the last time when this object was checked for consistency.
     *
     * @return the last check time
     */
    public long getLastCheckTime() {
        return lastCheckTime.get();
    }

    public boolean hasBinary() {
        Number flag = (Number) get(HAS_BINARY_FLAG);
        return flag != null && flag.intValue() == HAS_BINARY_VAL;
    }

    /**
     * Returns the path of the main document if this document is part of a _prev
     * history tree. Otherwise this method simply returns {@link #getPath()}.
     *
     * @return the path of the main document.
     */
    @NotNull
    public Path getMainPath() {
        String p = getPathString();
        if (p.startsWith("p")) {
            p = PathUtils.getAncestorPath(p, 2);
            if (p.length() == 1) {
                return Path.ROOT;
            } else {
                p = p.substring(1);
            }
        }
        return Path.fromString(p);
    }

    /**
     * @return a map of the last known revision for each clusterId.
     */
    @NotNull
    public Map<Integer, Revision> getLastRev() {
        Map<Integer, Revision> map = Maps.newHashMap();
        Map<Revision, String> valueMap = getLocalMap(LAST_REV);
        for (Map.Entry<Revision, String> e : valueMap.entrySet()) {
            int clusterId = e.getKey().getClusterId();
            Revision rev = Revision.fromString(e.getValue());
            map.put(clusterId, rev);
        }
        return map;
    }

    /**
     * Returns <code>true</code> if this document contains an entry for the
     * given <code>revision</code> in the {@link #REVISIONS} map. Please note
     * that an entry in the {@link #REVISIONS} map does not necessarily mean
     * the the revision is committed.
     * Use {@link RevisionContext#getCommitValue(Revision, NodeDocument)} to get
     * the commit state of a revision.
     *
     * @param revision the revision to check.
     * @return <code>true</code> if this document contains the given revision.
     */
    public boolean containsRevision(@NotNull Revision revision) {
        if (getLocalRevisions().containsKey(revision)) {
            return true;
        }
        for (NodeDocument prev : getPreviousDocs(REVISIONS, revision)) {
            if (prev.containsRevision(revision)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Purge the  uncommitted revisions of this document with the
     * local cluster node id as returned by the {@link RevisionContext}. These
     * are the {@link #REVISIONS} and {@link #BRANCH_COMMITS} entries where
     * {@link Utils#isCommitted(String)} returns false.
     *
     * <p>
     *     <bold>Note</bold> - This method should only be invoked upon startup
     *     as then only we can safely assume that these revisions would not be
     *     committed
     * </p>
     *
     * @param clusterId the clusterId.
     * @param batchSize the batch size to purge uncommitted revisions
     * @param olderThanLastWrittenRootRevPredicate {@link java.util.function.Predicate} to filter revisions older than lastWrittenRootRev
     * @return count of the revision entries purged
     */

    int purgeUncommittedRevisions(final int clusterId, final int batchSize,
                                  final java.util.function.Predicate<Revision> olderThanLastWrittenRootRevPredicate) {
        // only look at revisions in this document.
        // uncommitted revisions are not split off
        Map<Revision, String> localRevisions = getLocalRevisions();
        UpdateOp op = new UpdateOp(requireNonNull(getId()), false);
        Set<Revision> uniqueRevisions = new HashSet<>();
        for (Map.Entry<Revision, String> commit : localRevisions.entrySet()) {
            if (!Utils.isCommitted(commit.getValue())) {
                Revision r = commit.getKey();
                if (r.getClusterId() == clusterId && olderThanLastWrittenRootRevPredicate.test(r)) {
                    uniqueRevisions.add(r);
                    removeRevision(op, r);
                }
            }
            if (op.getChanges().size() >= batchSize) {
                store.findAndUpdate(Collection.NODES, op);
                op = new UpdateOp(requireNonNull(getId()), false);
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.NODES, op);
            op = new UpdateOp(requireNonNull(getId()), false);
        }


        for (Revision r : getLocalBranchCommits()) {
            String commitValue = localRevisions.get(r);
            if (!Utils.isCommitted(commitValue) && r.getClusterId() == clusterId && olderThanLastWrittenRootRevPredicate.test(r)) {
                uniqueRevisions.add(r);
                removeBranchCommit(op, r);
                if (op.getId().equals(Utils.getIdFromPath(Path.ROOT))
                        && getLocalCommitRoot().containsKey(r)) {
                    removeCommitRoot(op, r);
                }
            }
            if (op.getChanges().size() >= batchSize) {
                store.findAndUpdate(Collection.NODES, op);
                op = new UpdateOp(requireNonNull(getId()), false);
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.NODES, op);
        }
        return uniqueRevisions.size();
    }

    /**
     * Purge collision markers with the local clusterId on this document. Use
     * only on start when there are no ongoing or pending commits.
     *
     * @param clusterId the cluster Id.
     * @param batchSize the batch size to purge collision markers
     * @param olderThanLastWrittenRootRevPredicate {@link java.util.function.Predicate} to filter revisions older than lastWrittenRootRev
     * @return the number of removed collision markers.
     */
    int purgeCollisionMarkers(final int clusterId, final int batchSize,
                              final java.util.function.Predicate<Revision> olderThanLastWrittenRootRevPredicate) {
        Map<Revision, String> valueMap = getLocalMap(COLLISIONS);
        UpdateOp op = new UpdateOp(requireNonNull(getId()), false);
        int purgeCount = 0;
        for (Map.Entry<Revision, String> commit : valueMap.entrySet()) {
            Revision r = commit.getKey();
            if (r.getClusterId() == clusterId && olderThanLastWrittenRootRevPredicate.test(r)) {
                purgeCount++;
                removeCollision(op, r);
            }

            if (op.getChanges().size() >= batchSize) {
                store.findAndUpdate(Collection.NODES, op);
                op = new UpdateOp(requireNonNull(getId()), false);
            }
        }
        if (op.hasChanges()) {
            store.findAndUpdate(Collection.NODES, op);
        }
        return purgeCount;
    }

    /**
     * Returns the conflicts on the given {@code changes} if there are any. The
     * returned revisions are the commits, which created the collision markers
     * for one of the {@code changes}.
     *
     * @param changes the changes to check.
     * @return the conflict revisions.
     */
    @NotNull
    Set<Revision> getConflictsFor(@NotNull Iterable<Revision> changes) {
        requireNonNull(changes);

        Set<Revision> conflicts = new HashSet<>();
        Map<Revision, String> collisions = getLocalMap(COLLISIONS);
        for (Revision r : changes) {
            String value = collisions.get(r.asTrunkRevision());
            if (value == null) {
                continue;
            }
            try {
                conflicts.add(Revision.fromString(value));
            } catch (IllegalArgumentException e) {
                // backward compatibility: collision marker with value 'true'
            }
        }
        return conflicts;
    }

    /**
     * Returns the commit root path for the given <code>revision</code> or
     * <code>null</code> if this document does not have a commit root entry for
     * the given <code>revision</code>.
     *
     * @param revision a revision.
     * @return the commit root path or <code>null</code>.
     */
    @Nullable
    public Path getCommitRootPath(Revision revision) {
        String depth = getCommitRootDepth(revision);
        if (depth != null) {
            return getPathAtDepth(depth);
        }
        return null;
    }

    /**
     * Get the revision of the latest change made to this node. At the same
     * time this method collects all collisions that happened for the given
     * {@code changeRev}. The reported latest change takes branches into
     * account. This means, if {@code changeRev} is on a branch, the latest
     * change is either a change that was done by a preceding branch commit or
     * a change that happened before the base of the branch. Changes done after
     * the branch base on trunk are not considered in this case. For a trunk
     * commit the latest change is reported similarly. In this case, unmerged
     * branch commits are not considered as latest change. Only commits to trunk
     * are considered.
     *
     * Collisions include the following cases:
     * <ul>
     *     <li>The other change is not yet committed</li>
     *     <li>The other change is a branch commit and not yet merged</li>
     *     <li>The {@code changeRev} is a branch commit and the other change
     *       happened after the base revision of the branch</li>
     *     <li>The other change is from another cluster node and not yet
     *       visible</li>
     * </ul>
     *
     * @param context the revision context.
     * @param baseRev the base revision of the current change.
     * @param changeRev the revision of the current change.
     * @param branch the branch associated with the current change or
     *              {@code null} if {@code changeRev} is not a branch commit.
     * @param collisions changes that happened after {@code baseRev}.
     */
    @Nullable
    Revision getNewestRevision(final RevisionContext context,
                               final RevisionVector baseRev,
                               final Revision changeRev,
                               final Branch branch,
                               final Set<Revision> collisions) {
        checkArgument(!baseRev.isBranch() || branch != null,
                "Branch must be non-null if baseRev is a branch revision");
        RevisionVector head = context.getHeadRevision();
        RevisionVector lower = branch != null ? branch.getBase() : baseRev;
        // the clusterIds to check when walking the changes
        Set<Integer> clusterIds = Collections.emptySet();
        if (!getPreviousRanges().isEmpty()) {
            clusterIds = new HashSet<>();
            for (Revision prevRev : getPreviousRanges().keySet()) {
                if (lower.isRevisionNewer(prevRev) ||
                        Objects.equals(prevRev, lower.getRevision(prevRev.getClusterId()))) {
                    clusterIds.add(prevRev.getClusterId());
                }
            }
            if (!clusterIds.isEmpty()) {
                // add clusterIds of local changes as well
                for (Revision r : getLocalCommitRoot().keySet()) {
                    clusterIds.add(r.getClusterId());
                }
                for (Revision r : getLocalRevisions().keySet()) {
                    clusterIds.add(r.getClusterId());
                }
            }
        }
        // if we don't have clusterIds, we can use the local changes only
        boolean fullScan = true;
        Iterable<Revision> changes = Iterables.mergeSorted(
                ImmutableList.of(
                        getLocalRevisions().keySet(),
                        getLocalCommitRoot().keySet()),
                getLocalRevisions().comparator()
        );
        if (!clusterIds.isEmpty()) {
            // there are some previous documents that potentially
            // contain changes after 'lower' revision vector
            // include previous documents as well (only needed in rare cases)
            fullScan = false;
            changes = Iterables.mergeSorted(
                    ImmutableList.of(
                            changes,
                            getChanges(REVISIONS, lower),
                            getChanges(COMMIT_ROOT, lower)
                    ), getLocalRevisions().comparator()
            );
            if (LOG.isDebugEnabled()) {
                LOG.debug("getNewestRevision() with changeRev {} on {}, " +
                                "_revisions {}, _commitRoot {}",
                        changeRev, getId(), getLocalRevisions(), getLocalCommitRoot());
            }
        }
        Map<Integer, Revision> newestRevs = Maps.newHashMap();
        Map<Revision, String> validRevisions = Maps.newHashMap();
        for (Revision r : changes) {
            if (r.equals(changeRev)) {
                continue;
            }
            if (!fullScan) {
                // check if we can stop going through changes
                if (clusterIds.contains(r.getClusterId())
                        && !lower.isRevisionNewer(r)
                        && newestRevs.containsKey(r.getClusterId())) {
                    clusterIds.remove(r.getClusterId());
                    if (clusterIds.isEmpty()) {
                        // all remaining revisions are older than
                        // the lower bound
                        break;
                    }
                }
            }
            if (newestRevs.containsKey(r.getClusterId())) {
                // we already found the newest revision for this clusterId
                // from a baseRev point of view
                // we still need to find collisions up to the base
                // of the branch if this is for a commit on a branch
                if (branch != null && !branch.containsCommit(r)) {
                    // change does not belong to the branch
                    if (branch.getBase(changeRev).isRevisionNewer(r)) {
                        // and happened after the base of the branch
                        collisions.add(r);
                    }
                }
            } else {
                // we don't yet have the newest committed change
                // for this clusterId
                // check if change is visible from baseRev
                if (isValidRevision(context, r, null, baseRev, validRevisions)) {
                    // consider for newestRev
                    newestRevs.put(r.getClusterId(), r);
                } else {
                    // not valid means:
                    // 1) 'r' is not committed -> collision
                    // 2) 'r' is on a branch, but not the same as
                    //    changeRev -> collisions
                    // 3) changeRev is on a branch and 'r' is newer than
                    //    the base of the branch -> collision
                    // 4) 'r' is committed but not yet visible to current
                    //    cluster node -> collisions
                    // 5) changeRev is not on a branch, 'r' is committed and
                    //    newer than baseRev -> newestRev

                    Revision commitRevision = null;
                    String cv = context.getCommitValue(r, this);
                    if (Utils.isCommitted(cv)) {
                        commitRevision = resolveCommitRevision(r, cv);
                    }
                    if (commitRevision != null // committed but not yet visible
                            && head.isRevisionNewer(commitRevision)) {
                        // case 4)
                        collisions.add(r);
                    } else if (commitRevision != null // committed
                            && branch == null         // changeRev not on branch
                            && baseRev.isRevisionNewer(r)) {
                        // case 5)
                        newestRevs.put(r.getClusterId(), r);
                    } else {
                        // remaining cases 1), 2) and 3)
                        collisions.add(r);
                    }
                }
            }

        }
        // select the newest committed change
        Revision newestRev = null;
        for (Revision r : newestRevs.values()) {
            newestRev = Utils.max(newestRev, r, StableRevisionComparator.INSTANCE);
        }

        if (newestRev == null) {
            return null;
        }

        // the local deleted map contains the most recent revisions
        SortedMap<Revision, String> deleted = getLocalDeleted();
        String value = deleted.get(newestRev);
        if (value == null && deleted.headMap(newestRev).isEmpty()) {
            // newestRev is newer than most recent entry in local deleted
            // no need to check previous docs
            return newestRev;
        }

        if (value == null) {
            // get from complete map
            value = getDeleted().get(newestRev);
        }
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
     * @param commitValue the commit value of the revision to check or
     *                    <code>null</code> if unknown.
     * @param readRevision the read revision of the client.
     * @param validRevisions map of revisions to commit value already checked
     *                       against <code>readRevision</code> and considered
     *                       valid.
     * @return <code>true</code> if the revision is valid; <code>false</code>
     *         otherwise.
     */
    private boolean isValidRevision(@NotNull RevisionContext context,
                                    @NotNull Revision rev,
                                    @Nullable String commitValue,
                                    @NotNull RevisionVector readRevision,
                                    @NotNull Map<Revision, String> validRevisions) {
        if (validRevisions.containsKey(rev)) {
            return true;
        }
        // get the commit value if it is not yet available
        if (commitValue == null) {
            commitValue = context.getCommitValue(rev, this);
        }
        if (commitValue == null) {
            // this change is not committed, hence not valid/visible
            return false;
        }
        if (isVisible(context, rev, commitValue, readRevision)) {
            validRevisions.put(rev, commitValue);
            return true;
        }
        return false;
    }

    /**
     * Resolve the commit revision that holds the current value of a property based
     * on provided readRevision if the current value is in the local
     * map - null if the current value might be in a split doc or the node or property
     * does not exist at all.
     *
     * @param nodeStore    the node store.
     * @param readRevision the read revision.
     * @param key          the key of the property to resolve
     * @return a Revision if the value of the property resolves to a value based
     *         on what's in the local document, null if the node or property does
     *         not exist at all or the value is in a split document.
     */
    Revision localCommitRevisionOfProperty(@NotNull DocumentNodeStore nodeStore,
                                           @NotNull RevisionVector readRevision,
                                           @NotNull String key) {
        Map<Revision, String> validRevisions = new HashMap<>();
        Branch branch = nodeStore.getBranches().getBranch(readRevision);
        LastRevs lastRevs = createLastRevs(readRevision,
                nodeStore, branch, null);

        Revision min = getLiveRevision(nodeStore, readRevision, validRevisions, lastRevs);
        if (min == null) {
            // node is deleted
            return null;
        }

        // ignore when local map is empty (OAK-2442)
        SortedMap<Revision, String> local = getLocalMap(key);
        if (local.isEmpty()) {
            return null;
        }

        // first check local map, which contains most recent values
        Value value = getLatestValue(nodeStore, local.entrySet(),
                readRevision, validRevisions, lastRevs);
        if (value == null) {
            return null;
        }
        // check if there may be more recent values in a previous document
        if (requiresCompleteMapCheck(value, local, nodeStore)) {
            return null;
        } else {
            return value.valueEntry.getKey();
        }
    }

    /**
     * Returns a {@link DocumentNodeState} as seen at the given
     * <code>readRevision</code>.
     *
     * @param nodeStore    the node store.
     * @param readRevision the read revision.
     * @param lastModified the revision when this node was last modified, but
     *                     the value is potentially not yet reflected in this
     *                     document.
     *                     See {@link RevisionContext#getPendingModifications()}.
     * @return the node or <code>null</code> if the node doesn't exist at the
     *         given read revision.
     */
    @Nullable
    public DocumentNodeState getNodeAtRevision(@NotNull DocumentNodeStore nodeStore,
                                               @NotNull RevisionVector readRevision,
                                               @Nullable Revision lastModified) {
        Map<Revision, String> validRevisions = Maps.newHashMap();
        Branch branch = nodeStore.getBranches().getBranch(readRevision);
        LastRevs lastRevs = createLastRevs(readRevision,
                nodeStore, branch, lastModified);

        Revision min = getLiveRevision(nodeStore, readRevision, validRevisions, lastRevs);
        if (min == null) {
            // deleted
            return null;
        }
        Path path = getPath();
        List<PropertyState> props = new ArrayList<>();
        for (String key : keySet()) {
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            // ignore when local map is empty (OAK-2442)
            SortedMap<Revision, String> local = getLocalMap(key);
            if (local.isEmpty()) {
                continue;
            }
            // first check local map, which contains most recent values
            Value value = getLatestValue(nodeStore, local.entrySet(),
                    readRevision, validRevisions, lastRevs);

            if (value == null
                    // only filter if prevNoProp feature toggle is enabled:
                    && nodeStore.getPrevNoPropCache() != null
                    && !anyRevisionCommitted(local.keySet(), nodeStore, validRevisions)) {
                // OAK-11184 : if the locally resolved value is null AND
                // there are no committed revisions in the local map at all,
                // then don't scan previous documents as that should not
                // find anything. The split algorithm always ensures
                // that at least one committed revision remains in the
                // local map. From that we can derive that if there's
                // no committed revision in the local map, there isn't
                // any in previous documents neither.
                // This should only occur when a property is being newly
                // added or was deleted, then fullGC-ed and now re-added.
                PREV_NO_PROP_LOG.debug("getNodeAtRevision : skipping as no committed revision locally for path={}, key={}", path, key);
                continue;
            }

            // check if there may be more recent values in a previous document
            value = requiresCompleteMapCheck(value, local, nodeStore) ? null : value;

            if (value == null && !getPreviousRanges().isEmpty()) {
                // check revision history
                value = getLatestValue(nodeStore, getVisibleChanges(key, readRevision, nodeStore.getPrevNoPropCache()),
                        readRevision, validRevisions, lastRevs);
            }
            String propertyName = Utils.unescapePropertyName(key);
            String v = value != null ? value.valueEntry.getValue() : null;
            if (v != null){
                props.add(nodeStore.createPropertyState(propertyName, v));
            }
        }

        // when was this node last modified?
        RevisionVector lastRevision = new RevisionVector(min);
        RevisionVector branchBase = null;
        if (branch != null) {
            branchBase = branch.getBase(readRevision.getBranchRevision());
        }
        for (Revision r : lastRevs) {
            if (readRevision.isRevisionNewer(r)) {
                // the node has a _lastRev which is newer than readRevision
                // this means we don't know when this node was
                // modified by an operation on a descendant node between
                // current lastRevision and readRevision. therefore we have
                // to stay on the safe side and use readRevision
                Revision rev = readRevision.getRevision(r.getClusterId());
                if (rev != null) {
                    lastRevision = lastRevision.update(rev);
                } else {
                    // readRevision does not have a revision for this
                    // clusterId -> remove from lastRevision
                    lastRevision = lastRevision.remove(r.getClusterId());
                }
            } else if (branchBase != null && branchBase.isRevisionNewer(r)) {
                // readRevision is on a branch and the node has a
                // _lastRev which is newer than the base of the branch
                // we cannot use this _lastRev because it is not visible
                // from this branch. highest possible revision of visible
                // changes is the base of the branch
                Revision rev = branchBase.getRevision(r.getClusterId());
                if (rev != null) {
                    lastRevision = lastRevision.update(rev);
                } else {
                    // branchBase does not have a revision for this
                    // clusterId -> remove from lastRevision
                    lastRevision = lastRevision.remove(r.getClusterId());
                }
            } else if (lastRevision.isRevisionNewer(r)) {
                lastRevision = lastRevision.update(r);
            }
        }
        if (branch != null) {
            // read from a branch
            // -> possibly overlay with unsaved last revs from branch
            lastRevs.updateBranch(branch.getUnsavedLastRevision(path, readRevision.getBranchRevision()));
            Revision r = lastRevs.getBranchRevision();
            if (r != null) {
                lastRevision = lastRevision.update(r);
            }
        }

        return new DocumentNodeState(nodeStore, path, readRevision, props, hasChildren(), lastRevision);
    }

    /**
     * Checks if any of the provided revisions are committed - given the
     * RevisionContext. Uses validRevisions as cached earlier
     * confirmed valid revisions (but chooses not to add to that map, to limit
     * side-effects of this new-ish method).
     *
     * @param revisions      the revisions to check if any of them are committed
     * @param context        the RevisionContext to use for commit value resolving
     * @param validRevisions map of revision to commit value considered valid
     *                       against the given readRevision.
     * @return true if the provided (local) map of revisions (of a property) has
     * any revisions that are committed (irrespective of visible or not).
     */
    private boolean anyRevisionCommitted(Set<Revision> revisions, @NotNull RevisionContext context,
            Map<Revision, String> validRevisions) {
        for (Revision propRev : revisions) {
            String commitValue = validRevisions.get(propRev);
            if (commitValue == null) {
                commitValue = context.getCommitValue(propRev, this);
            }
            if (commitValue == null) {
                // then it's not committed
                continue;
            }
            if (Utils.isCommitted(commitValue)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the earliest (oldest) revision where the node was alive at or before
     * the provided revision, if the node was alive at the given revision.
     *
     * @param context the revision context
     * @param readRevision the read revision
     * @param validRevisions the map of revisions to commit value already
     *                       checked against readRevision and considered valid.
     * @param lastRevs to keep track of the last modification.
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    @Nullable
    public Revision getLiveRevision(RevisionContext context,
                                    RevisionVector readRevision,
                                    Map<Revision, String> validRevisions,
                                    LastRevs lastRevs) {
        final SortedMap<Revision, String> local = getLocalDeleted();
        // check local deleted map first
        Value value = getLatestValue(context, local.entrySet(), readRevision, validRevisions, lastRevs);
        // check if there may be more recent values in a previous document
        value = requiresCompleteMapCheck(value, local, context) ? null : value;
        if (value == null && !getPreviousRanges().isEmpty()) {
            // need to check complete map
            value = getLatestValue(context, getDeleted().entrySet(), readRevision, validRevisions, lastRevs);
        }

        return value != null && "false".equals(value.valueEntry.getValue()) ? value.revision : null;
    }

    /**
     * Returns <code>true</code> if the given operation is conflicting with this
     * document.
     *
     * @param op the update operation.
     * @param baseRevision the base revision for the update operation.
     * @param commitRevision the commit revision of the update operation.
     * @param enableConcurrentAddRemove feature flag for OAK-2673.
     * @return <code>true</code> if conflicting, <code>false</code> otherwise.
     */
    boolean isConflicting(@NotNull UpdateOp op,
                          @NotNull RevisionVector baseRevision,
                          @NotNull Revision commitRevision,
                          boolean enableConcurrentAddRemove) {
        // did existence of node change after baseRevision?
        // only check local deleted map, which contains the most
        // recent values
        Map<Revision, String> deleted = getLocalDeleted();
        boolean allowConflictingDeleteChange =
                enableConcurrentAddRemove && allowConflictingDeleteChange(op);
        for (Map.Entry<Revision, String> entry : deleted.entrySet()) {
            if (entry.getKey().equals(commitRevision)) {
                continue;
            }

            if (baseRevision.isRevisionNewer(entry.getKey())) {
                boolean newerDeleted = Boolean.parseBoolean(entry.getValue());
                if (!allowConflictingDeleteChange || op.isDelete() != newerDeleted) {
                    return true;
                }
            }
        }

        for (Map.Entry<Key, Operation> entry : op.getChanges().entrySet()) {
            if (entry.getValue().type != Operation.Type.SET_MAP_ENTRY) {
                continue;
            }
            String name = entry.getKey().getName();
            if (DELETED.equals(name) && !allowConflictingDeleteChange) {
                // existence of node changed, this always conflicts with
                // any other concurrent change
                return true;
            }
            if (!Utils.isPropertyName(name)) {
                continue;
            }
            // was this property touched after baseRevision?
            for (Revision rev : getChanges(name, baseRevision)) {
                if (rev.equals(commitRevision)) {
                    continue;
                }
                if (baseRevision.isRevisionNewer(rev)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Utility method to check if {@code op} can be allowed to change
     * {@link #DELETED} property. Basic idea is that a change in
     * {@link #DELETED} property should be consistent if final value is same
     * and there are no observation semantic change. Thus, this method tries to
     * be very conservative and allows delete iff:
     * <ul>
     *     <li>{@code doc} represents and internal path</li>
     *     <li>{@code op} represents an add or delete operation</li>
     *     <li>{@code op} doesn't change add/delete any exposed property</li>
     *     <li>{@code doc} doesn't have any exposed property</li>
     * </ul>
     * <i>
     * Note: This method is a broad level check if we can allow such conflict
     * resolution. Actual cases, like allow-delete-delete, allow-add-add wrt to
     * revision are not handled here.
     * </i>
     * @param op {@link UpdateOp} instance having changes to check {@code doc} against
     * @return if conflicting change in {@link #DELETED} property is allowed
     */
    private boolean allowConflictingDeleteChange(UpdateOp op) {
        String path = getPathString();
        if (!Utils.isHiddenPath(path)) {
            return false;
        }

        if (!op.isNew() && !op.isDelete()) {
            return false;//only handle added/delete operations
        }

        for (Key opKey : op.getChanges().keySet()) {
            String name = opKey.getName();
            if (Utils.isPropertyName(name)) {
                return false; //only handle changes to internal properties
            }
        }

        // Only look at local data ...
        // even remotely updated properties should have an entry (although invisible)
        // by the time we are looking for conflicts
        for (String dataKey : keySet()) {
            if (Utils.isPropertyName(dataKey)) {
                return false; //only handle changes to internal properties
            }
        }

        return true;
    }

    /**
     * Returns update operations to split this document. The implementation may
     * decide to not return any operations if no splitting is required. A caller
     * must explicitly pass a head revision even though it is available through
     * the {@link RevisionContext}. The given head revision must reflect a head
     * state before {@code doc} was retrieved from the document store. This is
     * important in order to maintain consistency. See OAK-3081 for details.
     *
     * @param context the revision context.
     * @param head    the head revision before this document was retrieved from
     *                the document store.
     * @param binarySize a function that returns the binary size of the given
     *                   JSON property value String.
     * @return the split operations, whereby the last updateOp is guaranteed to be
     * the update of the main document (unless the entire list is empty)
     */
    @NotNull
    public Iterable<UpdateOp> split(@NotNull RevisionContext context,
                                    @NotNull RevisionVector head,
                                    @NotNull Function<String, Long> binarySize) {
        return SplitOperations.forDocument(this, context, head,
                binarySize, NUM_REVS_THRESHOLD);
    }

    /**
     * Returns previous revision ranges for this document. The revision keys are
     * sorted descending, newest first! The returned map does not include stale
     * entries.
     * This method is equivalent to calling {@link #getPreviousRanges(boolean)}
     * with {@code includeStale} set to false.
     *
     * @return the previous ranges for this document.
     */
    @NotNull
    NavigableMap<Revision, Range> getPreviousRanges() {
        return getPreviousRanges(false);
    }

    /**
     * Returns previous revision ranges for this document. The revision keys are
     * sorted descending, newest first!
     *
     * @param includeStale whether stale revision ranges are included or not.
     * @return the previous ranges for this document.
     */
    @NotNull
    NavigableMap<Revision, Range> getPreviousRanges(boolean includeStale) {
        if (includeStale) {
            return createPreviousRanges(true);
        } else {
            if (previous == null) {
                previous = createPreviousRanges(false);
            }
            return previous;
        }
    }

    /**
     * Creates a map with previous revision ranges for this document. The
     * revision keys are sorted descending, newest first!
     *
     * @param includeStale whether stale revision ranges are included or not.
     * @return the previous ranges for this document.
     */
    @NotNull
    private NavigableMap<Revision, Range> createPreviousRanges(boolean includeStale) {
        NavigableMap<Revision, Range> ranges;
        Map<Revision, String> map = getLocalMap(PREVIOUS);
        if (map.isEmpty()) {
            ranges = EMPTY_RANGE_MAP;
        } else {
            Map<Revision, String> stale = Collections.emptyMap();
            if (!includeStale) {
                stale = getLocalMap(STALE_PREV);
            }
            NavigableMap<Revision, Range> transformed =
                    new TreeMap<Revision, Range>(REVERSE);
            for (Map.Entry<Revision, String> entry : map.entrySet()) {
                Range r = Range.fromEntry(entry.getKey(), entry.getValue());
                if (String.valueOf(r.height).equals(stale.get(r.high))) {
                    continue;
                }
                transformed.put(r.high, r);
            }
            ranges = Maps.unmodifiableNavigableMap(transformed);
        }
        return ranges;
    }

    /**
     * Returns previous {@link NodeDocument}, which include entries for the
     * property in the given revision.
     * If the <code>revision</code> is <code>null</code>, then all previous
     * documents with changes for the given property are returned. The returned
     * documents are returned in descending revision order (newest first).
     *
     * @param property the name of a property.
     * @param revision the revision to match or <code>null</code>.
     * @return previous documents.
     */
    @NotNull
    Iterable<NodeDocument> getPreviousDocs(@NotNull final String property,
                                           @Nullable final Revision revision) {
        if (getPreviousRanges().isEmpty()) {
            return Collections.emptyList();
        }
        if (revision == null) {
            return new PropertyHistory(this, property);
        } else {
            final Path mainPath = getMainPath();
            // first try to lookup revision directly
            Map.Entry<Revision, Range> entry = getPreviousRanges().floorEntry(revision);
            if (entry != null) {
                Revision r = entry.getKey();
                int h = entry.getValue().height;
                String prevId = Utils.getPreviousIdFor(mainPath, r, h);
                NodeDocument prev = getPreviousDocument(prevId);
                if (prev != null) {
                    if (prev.getValueMap(property).containsKey(revision)) {
                        return Collections.singleton(prev);
                    }
                } else {
                    previousDocumentNotFound(prevId, revision);
                }
            }

            // didn't find entry -> scan through remaining head ranges
            return filter(transform(getPreviousRanges().headMap(revision).entrySet(), input -> {
                    if (input.getValue().includes(revision)) {
                       return getPreviousDoc(input.getKey(), input.getValue());
                    }
                    return null;
                }), input ->input != null && input.getValueMap(property).containsKey(revision));
        }
    }

    NodeDocument getPreviousDocument(String prevId){
        LOG.trace("get previous document {}", prevId);
        NodeDocument doc = store.find(Collection.NODES, prevId);
        if (doc == null) {
            // In case secondary read preference is used and node is not found
            // then check with primary again as it might happen that node document has not been
            // replicated. We know that document with such an id must exist but possibly dut to
            // replication lag it has not reached to secondary. So in that case read again
            // from primary
            doc = store.find(Collection.NODES, prevId, 0);
        }
        return doc;
    }

    @NotNull
    Iterator<NodeDocument> getAllPreviousDocs() {
        if (getPreviousRanges().isEmpty()) {
            return Collections.emptyIterator();
        }
        //Currently this method would fire one query per previous doc
        //If that poses a problem we can try to find all prev doc by relying
        //on property that all prevDoc id would starts <depth+2>:p/path/to/node
        return new AbstractIterator<NodeDocument>(){
            private Queue<Map.Entry<Revision, Range>> previousRanges =
                    CollectionUtils.toArrayDeque(getPreviousRanges().entrySet());
            @Override
            protected NodeDocument computeNext() {
                if(!previousRanges.isEmpty()){
                    Map.Entry<Revision, Range> e = previousRanges.remove();
                    NodeDocument prev = getPreviousDoc(e.getKey(), e.getValue());
                    if(prev != null){
                        previousRanges.addAll(prev.getPreviousRanges().entrySet());
                        return prev;
                    }
                }
                return endOfData();
            }
        };
    }

    /**
     * Returns previous leaf documents. Those are the previous documents with
     * a type {@code !=} {@link SplitDocType#INTERMEDIATE}. The documents are
     * returned in descending order based on the most recent change recorded
     * in the previous document. A change is defined as an entry in either the
     * {@link #REVISIONS} or {@link #COMMIT_ROOT} map.
     *
     * @return the leaf documents in descending order.
     */
    @NotNull
    Iterator<NodeDocument> getPreviousDocLeaves() {
        if (getPreviousRanges().isEmpty()) {
            return Collections.emptyIterator();
        }
        // create a mutable copy
        final NavigableMap<Revision, Range> ranges = new TreeMap<>(getPreviousRanges());
        return new AbstractIterator<NodeDocument>() {
            @Override
            protected NodeDocument computeNext() {
                NodeDocument next;
                for (;;) {
                    Map.Entry<Revision, Range> topEntry = ranges.pollFirstEntry();
                    if (topEntry == null) {
                        // no more ranges
                        next = endOfData();
                        break;
                    }
                    NodeDocument prev = getPreviousDoc(topEntry.getKey(), topEntry.getValue());
                    if (prev == null) {
                        // move on to next range
                        continue;
                    }
                    if (topEntry.getValue().getHeight() == 0) {
                        // this is a leaf
                        next = prev;
                        break;
                    } else {
                        // replace intermediate entry with its previous ranges
                        ranges.putAll(prev.getPreviousRanges());
                    }
                }
                return next;
            }
        };
    }

    private String getPreviousDocId(Revision rev, Range range) {
        return Utils.getPreviousIdFor(getMainPath(), rev, range.height);
    }

    @Nullable
    private NodeDocument getPreviousDoc(Revision rev, Range range){
        String prevId = getPreviousDocId(rev, range);
        NodeDocument prev = getPreviousDocument(prevId);
        if (prev != null) {
            return prev;
        } else {
            previousDocumentNotFound(prevId, rev);
        }
        return null;
    }

    /**
     * Returns the document that contains a reference to the previous document
     * identified by {@code revision} and {@code height}. This is either the
     * current document or an intermediate split document. This method returns
     * {@code null} if there is no such reference.
     *
     * @param revision the high revision of a range entry in {@link #PREVIOUS}.
     * @param height the height of the entry in {@link #PREVIOUS}.
     * @return the document with the entry or {@code null} if not found.
     */
    @Nullable
    NodeDocument findPrevReferencingDoc(Revision revision, int height) {
        for (Range range : getPreviousRanges().values()) {
            if (range.getHeight() == height && range.high.equals(revision)) {
                return this;
            } else if (range.includes(revision)) {
                String prevId = Utils.getPreviousIdFor(
                        getMainPath(), range.high, range.height);
                NodeDocument prev = store.find(NODES, prevId);
                if (prev == null) {
                    LOG.warn("Split document {} does not exist anymore. Main document is {}",
                            prevId, Utils.getIdFromPath(getMainPath()));
                    continue;
                }
                // recurse into the split hierarchy
                NodeDocument doc = prev.findPrevReferencingDoc(revision, height);
                if (doc != null) {
                    return doc;
                }
            }
        }
        return null;
    }

    /**
     * Returns an {@link Iterable} of {@link Revision} of all changes performed
     * on this document. This covers all entries for {@link #REVISIONS} and
     * {@link #COMMIT_ROOT} including previous documents. The revisions are
     * returned in descending stable revision order using
     * {@link StableRevisionComparator#REVERSE}.
     *
     * @return revisions of all changes performed on this document.
     */
    Iterable<Revision> getAllChanges() {
        RevisionVector empty = new RevisionVector();
        return Iterables.mergeSorted(ImmutableList.of(
                getChanges(REVISIONS, empty),
                getChanges(COMMIT_ROOT, empty)
        ), StableRevisionComparator.REVERSE);
    }

    /**
     * Returns all changes for the given property back to {@code min} revision
     * (exclusive). The revisions include committed as well as uncommitted
     * changes. The returned revisions are sorted in reverse order (newest
     * first).
     *
     * @param property the name of the property.
     * @param min the lower bound revision (exclusive).
     * @return changes back to {@code min} revision.
     */
    @NotNull
    Iterable<Revision> getChanges(@NotNull final String property,
                                  @NotNull final RevisionVector min) {
        Predicate<Revision> p = input -> min.isRevisionNewer(input);
        List<Iterable<Revision>> changes = new ArrayList<>();
        changes.add(abortingIterable(getLocalMap(property).keySet(), p));
        for (Map.Entry<Revision, Range> e : getPreviousRanges().entrySet()) {
            if (min.isRevisionNewer(e.getKey())) {
                final NodeDocument prev = getPreviousDoc(e.getKey(), e.getValue());
                if (prev != null) {
                    changes.add(abortingIterable(prev.getValueMap(property).keySet(), p));
                }
            }
        }
        if (changes.size() == 1) {
            return changes.get(0);
        } else {
            return Iterables.mergeSorted(changes, StableRevisionComparator.REVERSE);
        }
    }

    /**
     * Returns all changes for the given property that are visible from the
     * {@code readRevision} vector. The revisions include committed as well as
     * uncommitted changes. The returned revisions are sorted in reverse order
     * (newest first).
     *
     * @param property the name of the property.
     * @param readRevision the read revision vector.
     * @param prevNoPropCache optional cache for remembering non existence
     * of any property revisions in previous documents (by their id)
     * @return property changes visible from the given read revision vector.
     */
    @NotNull
    Iterable<Map.Entry<Revision, String>> getVisibleChanges(@NotNull final String property,
                                                            @NotNull final RevisionVector readRevision,
                                                            @Nullable final Cache<StringValue, StringValue> prevNoPropCache) {
        return getVisibleChanges(property, readRevision, prevNoPropCache, null);
    }

    /**
     * Variation of getVisibleChanges that allows to provide a non-null propRevFound.
     * The latter is used to detect whether previous documents had any property revisions at all.
     * This method is invoked in two different ways:
     * <ul>
     * <li>prevNoPropCache != null : this is used in the top most invocation only and
     * when passed causes top level previous documents to be handled via the cache.
     * To do that, for these cases the
     * changesFor method will do iterable-yoga to sneak into the iterator() code while
     * having taken note of whether any previous document had any revision at all for the
     * given property (this later aspect is checked in getVisibleChanges in a child iteration).</li>
     * <li>prevNoPropCache == null : this is used in invocations on all previous documents.
     * In this case the method checks if there are any revisions for the given property.
     * If there are, then the provided propRevFound AtomicBoolean is set to true.
     * That information is then used in the top most call in this getVisibleChanges-iteration
     * to decide whether we can cache the fact that no propery (whatsoever) was found in the
     * given previous document (and all its children) or not. That decision is based on the
     * AtomciBoolean being true or false.</li>
     * </ul>
     */
    @NotNull
    Iterable<Map.Entry<Revision, String>> getVisibleChanges(@NotNull final String property,
                                                            @NotNull final RevisionVector readRevision,
                                                            @Nullable final Cache<StringValue, StringValue> prevNoPropCache,
                                                            @Nullable final AtomicBoolean propRevFound) {
        Predicate<Map.Entry<Revision, String>> p = input -> !readRevision.isRevisionNewer(input.getKey());
        List<Iterable<Map.Entry<Revision, String>>> changes = new ArrayList<>();
        Map<Revision, String> localChanges = getLocalMap(property);
        if (!localChanges.isEmpty()) {
            if (propRevFound != null) {
                propRevFound.set(true);
            }
            changes.add(filter(localChanges.entrySet(), p::test));
        }

        for (Revision r : readRevision) {
            // collect changes per clusterId
            collectVisiblePreviousChanges(property, r, changes, prevNoPropCache, propRevFound);
        }

        if (changes.size() == 1) {
            return changes.get(0);
        } else {
            return mergeSorted(changes, ValueComparator.REVERSE);
        }
    }

    /**
     * Collect changes in previous documents into {@code changes} visible from
     * the given {@code readRevision} and for the given {@code property}. The
     * {@code Iterable} added to the {@code changes} list must be in descending
     * revision order.
     *
     * @param property the name of the property.
     * @param readRevision collect changes for this part of the readRevision.
     * @param changes where to add the changes to.
     */
    private void collectVisiblePreviousChanges(@NotNull final String property,
                                               @NotNull final Revision readRevision,
                                               @NotNull final List<Iterable<Entry<Revision, String>>> changes,
                                               @Nullable final Cache<StringValue, StringValue> prevNoPropCache,
                                               @Nullable final AtomicBoolean propRevFound) {
        List<Iterable<Map.Entry<Revision, String>>> revs = new ArrayList<>();

        RevisionVector readRV = new RevisionVector(readRevision);
        List<Range> ranges = new ArrayList<>();
        for (Range r : getPreviousRanges().values()) {
            if (r.low.getClusterId() == readRevision.getClusterId()
                    && readRevision.compareRevisionTime(r.low) >= 0) {
                // clusterId matches and range is visible from read revision
                ranges.add(r);
            }
        }
        List<Range> batch = new ArrayList<>();
        while (!ranges.isEmpty()) {
            // create batches of non-overlapping ranges
            Range previous = null;
            Iterator<Range> it = ranges.iterator();
            while (it.hasNext()) {
                Range r = it.next();
                if (previous == null || r.high.compareRevisionTime(previous.low) < 0) {
                    // first range or does not overlap with previous in batch
                    batch.add(r);
                    it.remove();
                    previous = r;
                }
            }
            revs.add(changesFor(batch, readRV, property, prevNoPropCache, propRevFound));
            batch.clear();
        }

        if (revs.size() == 1) {
            // optimize single batch case
            changes.add(revs.get(0));
        } else if (!revs.isEmpty()) {
            // merge sort them
            changes.add(mergeSorted(revs, ValueComparator.REVERSE));
        }
    }

    /**
     * Get changes of {@code property} for the given list of {@code ranges}
     * visible from {@code readRev}.
     *
     * @param ranges a list of ranges of previous document where to read the
     *               changes from.
     * @param readRev get changes visible from this read revision.
     * @param property the name of the property to read changes.
     * @return iterable over the changes.
     */
    private Iterable<Map.Entry<Revision, String>> changesFor(final List<Range> ranges,
                                                             final RevisionVector readRev,
                                                             final String property,
                                                             @Nullable final Cache<StringValue, StringValue> prevNoPropCache,
                                                             @Nullable final AtomicBoolean parentPropRevFound) {
        if (ranges.isEmpty()) {
            return Collections.emptyList();
        }

        final Function<Range, Iterable<Map.Entry<Revision, String>>> rangeToChanges;
        if (prevNoPropCache != null && parentPropRevFound == null) {
            // then we are in the main doc. at this point we thus need to
            // check the cache, if miss then scan prev docs and cache the result
            //TODO: consider refactoring of the getVisibleChanges/collectVisiblePreviousChanges/changesFor
            // logic. The way these methods create a sequence of Iterables and lambdas make
            // for a rather complex logic that is difficult to fiddle with.
            // It might thus be worth while to look into some loop logic rather than iteration here.
            // Except that refactoring is likely a bigger task, hence postponed for now.
            rangeToChanges = input -> {
                    final String prevDocId = getPreviousDocId(input.high, input);
                    final StringValue cacheKey = new StringValue(property + "@" + prevDocId);
                    if (prevNoPropCache.getIfPresent(cacheKey) != null) {
                        // cache hit, awesome!
                        // (we're not interested in the actual cache value btw, as finding 
                        // a cache value actually indicates "the property does not exist 
                        // in any previous document whatsoever" - no need for value check)
                        PREV_NO_PROP_LOG.trace("changesFor : empty changes cache hit for cacheKey={}", cacheKey);
                        return Collections.emptyList();
                    }
                    // cache miss - let's do the heavy lifting then
                    NodeDocument doc = getPreviousDoc(input.high, input);
                    if (doc == null) {
                        // this could be a candidate for caching probably.
                        // but might also indicate some race-condition.
                        // so let's not cache for now.
                        return Collections.emptyList();
                    }
                    // initiate counting
                    final AtomicBoolean childrenPropRevFound = new AtomicBoolean(false);
                    // create that Iterable - but wrap it so that we know how many
                    // property revisions were actually found in the scan.
                    // (we're mostly interested if that's zero ro non-zero though)
                    final Iterable<Entry<Revision, String>> vc = doc.getVisibleChanges(property, readRev, null, childrenPropRevFound);
                    // wrap that Iterable to intercept the call to hasNext().
                    // at that point if the counter is non-null it means
                    // that any previous documents scanned does indeed have
                    // the property we're interested in. It might not be visible
                    // or committed, but at least it does have it.
                    // In which case we'd skip that. But if it does not exist
                    // at all, then we cache that fact.
                    return new Iterable<Entry<Revision, String>>() {
                        @Override
                        public Iterator<Entry<Revision, String>> iterator() {
                            // grab the iterator - this typically triggers previous doc scan
                            final Iterator<Entry<Revision, String>> wrappee = vc.iterator();
                            // but let's invoke hasNext here explicitly still. to ensure
                            // we do indeed scan - without a scan hasNext can't work
                            wrappee.hasNext();
                            if (!childrenPropRevFound.get()) {
                                // then let's cache that
                                PREV_NO_PROP_LOG.debug("changesFor : caching empty changes for cacheKey={}", cacheKey);
                                prevNoPropCache.put(cacheKey, StringValue.EMPTY);
                            }
                            return wrappee;
                        }
                    };
                };
        } else {
            // without a cache either the caller is not interested at caching,
            // or we are within a previous doc reading n+1 level previous docs.
            // in both cases nothing much to do other than passing along the
            // counter (propRevCount).
            // also no caching other than in the main doc
            rangeToChanges = input -> {
                NodeDocument doc = getPreviousDoc(input.high, input);
                if (doc == null) {
                    return Collections.emptyList();
                }
                return doc.getVisibleChanges(property, readRev, null, parentPropRevFound);
            };
        }

        Iterable<Map.Entry<Revision, String>> changes;
        if (ranges.size() == 1) {
            final Range range = ranges.get(0);
            changes = new Iterable<Entry<Revision, String>>() {
                @SuppressWarnings("ConstantConditions")
                @Override
                public Iterator<Entry<Revision, String>> iterator() {
                    return rangeToChanges.apply(range).iterator();
                }
            };
        } else {
            changes = Iterables.concat(transform(copyOf(ranges), rangeToChanges::apply));
        }
        return filter(changes, input -> !readRev.isRevisionNewer(input.getKey()));
    }

    /**
     * Returns the local value map for the given key.
     *
     * @param key the key.
     * @return local value map.
     */
    @NotNull
    SortedMap<Revision, String> getLocalMap(String key) {
        @SuppressWarnings("unchecked")
        SortedMap<Revision, String> map = (SortedMap<Revision, String>) data.get(key);
        if (map == null) {
            map = ValueMap.EMPTY;
        }
        return map;
    }

    /**
     * Returns name of all the properties on this document
     * <p>
     * Note: property names returned are escaped
     *
     * @return Set of all property names (escaped)
     * @see Utils#unescapePropertyName(String)
     * @see Utils#escapePropertyName(String)
     */
    @NotNull
    Set<String> getPropertyNames() {
        return data
                .keySet()
                .stream()
                .filter(Utils::isPropertyName)
                .collect(toSet());
    }

    /**
     * @return the {@link #REVISIONS} stored on this document.
     */
    @NotNull
    SortedMap<Revision, String> getLocalRevisions() {
        return getLocalMap(REVISIONS);
    }

    @NotNull
    SortedMap<Revision, String> getLocalCommitRoot() {
        return getLocalMap(COMMIT_ROOT);
    }

    @NotNull
    SortedMap<Revision, String> getLocalDeleted() {
        return getLocalMap(DELETED);
    }

    @NotNull
    SortedMap<Revision, String> getStalePrev() {
        return getLocalMap(STALE_PREV);
    }

    /**
     * Returns the branch commit entries on this document
     * ({@link #BRANCH_COMMITS}). This method does not consider previous
     * documents, but only returns the entries on this document.
     */
    @NotNull
    public Set<Revision> getLocalBranchCommits() {
        return getLocalMap(BRANCH_COMMITS).keySet();
    }

    /**
     * Resolves the commit value for the change with the given revision on this
     * document. If necessary, this method will lookup the commit value on the
     * referenced commit root document.
     *
     * @param revision the revision of a change on this document.
     * @return the commit value associated with the change.
     */
    @Nullable
    String resolveCommitValue(Revision revision) {
        NodeDocument commitRoot = getCommitRoot(revision);
        if (commitRoot == null) {
            return null;
        }
        return commitRoot.getCommitValue(revision);
    }

    /**
     * Returns the sweep revisions on this document as a {@link RevisionVector}.
     * This method will return an empty {@link RevisionVector} if this document
     * doesn't have any sweep revisions set.
     *
     * @return the sweep revisions as a {@link RevisionVector}.
     */
    @NotNull
    RevisionVector getSweepRevisions() {
        return new RevisionVector(transform(getLocalMap(SWEEP_REV).values(),
                s -> Revision.fromString(s)));
    }

    //-------------------------< UpdateOp modifiers >---------------------------

    public static void setChildrenFlag(@NotNull UpdateOp op,
                                       boolean hasChildNode) {
        requireNonNull(op).set(CHILDREN_FLAG, hasChildNode);
    }

    public static void setModified(@NotNull UpdateOp op,
                                   @NotNull Revision revision) {
        requireNonNull(op).max(MODIFIED_IN_SECS, getModifiedInSecs(requireNonNull(revision).getTimestamp()));
    }

    public static void setRevision(@NotNull UpdateOp op,
                                   @NotNull Revision revision,
                                   @NotNull String commitValue) {
        requireNonNull(op).setMapEntry(REVISIONS,
                requireNonNull(revision), requireNonNull(commitValue));
    }

    public static void unsetRevision(@NotNull UpdateOp op,
                                     @NotNull Revision revision) {
        requireNonNull(op).unsetMapEntry(REVISIONS, requireNonNull(revision));
    }

    public static boolean isRevisionsEntry(String name) {
        return REVISIONS.equals(name);
    }

    public static boolean isCommitRootEntry(String name) {
        return COMMIT_ROOT.equals(name);
    }

    public static boolean isDeletedEntry(String name) {
        return DELETED.equals(name);
    }

    public static boolean isLastRevEntry(String name) {
        return LAST_REV.equals(name);
    }

    public static void removeRevision(@NotNull UpdateOp op,
                                      @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(REVISIONS, requireNonNull(revision));
    }

    /**
     * Add a collision marker for the given {@code revision}.
     *
     * @param op the update operation.
     * @param revision the commit for which a collision was detected.
     * @param other the revision for the commit, which detected the collision.
     */
    public static void addCollision(@NotNull UpdateOp op,
                                    @NotNull Revision revision,
                                    @NotNull Revision other) {
        requireNonNull(op).setMapEntry(COLLISIONS, requireNonNull(revision),
                other.toString());
    }

    public static void removeCollision(@NotNull UpdateOp op,
                                       @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(COLLISIONS, requireNonNull(revision));
    }

    public static void setLastRev(@NotNull UpdateOp op,
                                  @NotNull Revision revision) {
        requireNonNull(op).setMapEntry(LAST_REV,
                new Revision(0, 0, revision.getClusterId()),
                revision.toString());
    }

    public static void setCommitRoot(@NotNull UpdateOp op,
                                     @NotNull Revision revision,
                                     int commitRootDepth) {
        requireNonNull(op).setMapEntry(COMMIT_ROOT, requireNonNull(revision),
                String.valueOf(commitRootDepth));
    }

    public static void removeCommitRoot(@NotNull UpdateOp op,
                                        @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(COMMIT_ROOT, revision);
    }

    public static void unsetCommitRoot(@NotNull UpdateOp op,
                                       @NotNull Revision revision) {
        requireNonNull(op).unsetMapEntry(COMMIT_ROOT, revision);
    }

    public static void setDeleted(@NotNull UpdateOp op,
                                  @NotNull Revision revision,
                                  boolean deleted) {
        if(deleted) {
            //DELETED_ONCE would be set upon every delete.
            //possibly we can avoid that
            setDeletedOnce(op);
        }
        requireNonNull(op).setMapEntry(DELETED, requireNonNull(revision), String.valueOf(deleted));
    }

    public static void setDeletedOnce(@NotNull UpdateOp op) {
        requireNonNull(op).set(DELETED_ONCE, Boolean.TRUE);
    }

    public static void removeDeleted(@NotNull UpdateOp op,
                                     @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(DELETED, revision);
    }

    public static void setPrevious(@NotNull UpdateOp op,
                                   @NotNull Range range) {
        requireNonNull(op).setMapEntry(PREVIOUS, requireNonNull(range).high,
                range.getLowValue());
    }

    public static void removePrevious(@NotNull UpdateOp op,
                                      @NotNull Range range) {
        removePrevious(op, requireNonNull(range).high);
    }

    public static void removePrevious(@NotNull UpdateOp op,
                                      @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(PREVIOUS, requireNonNull(revision));
    }

    public static void setStalePrevious(@NotNull UpdateOp op,
                                        @NotNull Revision revision,
                                        int height) {
        requireNonNull(op).setMapEntry(STALE_PREV,
                requireNonNull(revision), String.valueOf(height));
    }

    public static void removeStalePrevious(@NotNull UpdateOp op,
                                           @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(STALE_PREV, requireNonNull(revision));
    }

    public static void setHasBinary(@NotNull UpdateOp op) {
        requireNonNull(op).set(HAS_BINARY_FLAG, HAS_BINARY_VAL);
    }

    public static void setBranchCommit(@NotNull UpdateOp op,
                                       @NotNull Revision revision) {
        requireNonNull(op).setMapEntry(BRANCH_COMMITS,
                revision, String.valueOf(true));
    }

    public static void removeBranchCommit(@NotNull UpdateOp op,
                                          @NotNull Revision revision) {
        requireNonNull(op).removeMapEntry(BRANCH_COMMITS, revision);
    }

    public static void setSweepRevision(@NotNull UpdateOp op,
                                        @NotNull Revision revision) {
        requireNonNull(op).setMapEntry(SWEEP_REV,
                new Revision(0, 0, revision.getClusterId()),
                revision.toString());
    }

    public static void hasLastRev(@NotNull UpdateOp op,
                                  @NotNull Revision revision) {
        requireNonNull(op).equals(LAST_REV,
                new Revision(0, 0, revision.getClusterId()),
                revision.toString());
    }

    //----------------------------< internal >----------------------------------

    private void previousDocumentNotFound(String prevId, Revision rev) {
        final boolean logSilence = LOG_SILENCER.silence(prevId);
        if (!logSilence) {
            LOG.warn("Document with previous revisions not found: " + prevId
                    + LogSilencer.SILENCING_POSTFIX);
        } else {
            LOG.debug("Document with previous revisions not found: {}", prevId);
        }
        // main document may be stale, evict it from the cache if it is
        // older than one minute. We don't want to invalidate a document
        // too frequently if the document structure is really broken.
        Path path = getMainPath();
        String id = Utils.getIdFromPath(path);
        NodeDocument doc = store.getIfCached(NODES, id);
        long now = Revision.getCurrentTimestamp();
        while (doc != null
                && doc.getCreated() + TimeUnit.MINUTES.toMillis(1) < now) {
            if (!logSilence) {
                LOG.info("Invalidated cached document {} -{}", id, LogSilencer.SILENCING_POSTFIX);
            } else {
                LOG.debug("Invalidated cached document {}", id);
            }
            store.invalidateCache(NODES, id);
            // also invalidate intermediate docs if there are any matching
            Iterable<Range> ranges = doc.getPreviousRanges().values();
            doc = null;
            for (Range range : ranges) {
                if (range.includes(rev)) {
                    id = Utils.getPreviousIdFor(path, range.high, range.height);
                    doc = store.getIfCached(NODES, id);
                    break;
                }
            }
        }
    }

    private LastRevs createLastRevs(@NotNull RevisionVector readRevision,
                                    @NotNull RevisionContext context,
                                    @Nullable Branch branch,
                                    @Nullable Revision pendingLastRev) {
        LastRevs lastRevs = new LastRevs(getLastRev(), readRevision, branch);
        // overlay with unsaved last modified from this instance
        lastRevs.update(pendingLastRev);
        // collect clusterIds
        SortedSet<Revision> mostRecentChanges = new TreeSet<>(REVERSE);
        mostRecentChanges.addAll(getLocalRevisions().keySet());
        mostRecentChanges.addAll(getLocalCommitRoot().keySet());
        Set<Integer> clusterIds = new HashSet<>();
        for (Revision r : getLocalRevisions().keySet()) {
            clusterIds.add(r.getClusterId());
        }
        for (Revision r : getLocalCommitRoot().keySet()) {
            clusterIds.add(r.getClusterId());
        }
        for (Revision r : mostRecentChanges) {
            if (!clusterIds.contains(r.getClusterId())) {
                // already found most recent change from this cluster node
                continue;
            }
            String commitValue = context.getCommitValue(r, this);
            if (commitValue == null) {
                continue;
            }
            // resolve revision
            Revision commitRev = resolveCommitRevision(r, commitValue);
            if (Utils.isCommitted(commitValue)) {
                lastRevs.update(commitRev);
                clusterIds.remove(r.getClusterId());
            } else if (branch != null) {
                Revision branchRev = commitRev.asBranchRevision();
                if (branch.containsCommit(branchRev)) {
                    lastRevs.updateBranch(branchRev);
                    clusterIds.remove(r.getClusterId());
                }
            }
        }
        return lastRevs;
    }

    /**
     * Returns {@code true} if the given {@code revision} is more recent or
     * equal to the committed revision in {@code valueMap}. This method assumes
     * the given {@code revision} is committed.
     *
     * @param valueMap the value map sorted most recent first.
     * @param revision a committed revision.
     * @param context the revision context.
     * @return if {@code revision} is the most recent committed revision in the
     *          {@code valueMap}.
     */
    private boolean isMostRecentCommitted(SortedMap<Revision, String> valueMap,
                                          Revision revision,
                                          RevisionContext context) {
        if (valueMap.isEmpty()) {
            return true;
        }
        // shortcut when revision is the first key
        Revision first = valueMap.firstKey();
        if (first.compareRevisionTimeThenClusterId(revision) <= 0) {
            return true;
        }
        // need to check commit status
        for (Revision r : valueMap.keySet()) {
            String cv = context.getCommitValue(r, this);
            if (Utils.isCommitted(cv)) {
                Revision c = resolveCommitRevision(r, cv);
                return c.compareRevisionTimeThenClusterId(revision) <= 0;
            }
        }
        // no committed revision found in valueMap
        return true;
    }

    /**
     * Returns the commit root document for the given revision. This may either
     * be this document or another one.
     *
     * @param rev a revision.
     * @return the commit root or <code>null</code> if there is none.
     */
    @Nullable
    private NodeDocument getCommitRoot(@NotNull Revision rev) {
        // check local revisions and commitRoot first
        if (getLocalRevisions().containsKey(rev)) {
            return this;
        }
        Path commitRootPath;
        String depth = getLocalCommitRoot().get(rev);
        if (depth != null) {
            commitRootPath = getPathAtDepth(depth);
        } else {
            // fall back to complete check, including previous documents
            if (containsRevision(rev)) {
                return this;
            }
            commitRootPath = getCommitRootPath(rev);
            if (commitRootPath == null) {
                // may happen for a commit root document, which hasn't been
                // updated with the commit revision yet
                return null;
            }
        }
        // get root of commit
        return store.find(Collection.NODES, Utils.getIdFromPath(commitRootPath));
    }

    /**
     * Returns the path at the given {@code depth} based on the path of this
     * document.
     *
     * @param depth the depth as a string.
     * @return the path.
     * @throws NumberFormatException if {@code depth} cannot be parsed as an
     *              integer.
     */
    @NotNull
    private Path getPathAtDepth(@NotNull String depth) {
        if (requireNonNull(depth).equals("0")) {
            return Path.ROOT;
        }
        Path p = getPath();
        return p.getAncestor(p.getDepth() - Integer.parseInt(depth));
    }

    /**
     * Returns the commit root depth for the given revision. This method also
     * takes previous documents into account.
     *
     * @param revision get the commit root depth for this revision.
     * @return the depth or <code>null</code> if there is no commit root entry
     *         for the given revision on this document or previous documents.
     */
    @Nullable
    private String getCommitRootDepth(@NotNull Revision revision) {
        // check local map first
        Map<Revision, String> local = getLocalCommitRoot();
        String depth = local.get(revision);
        if (depth == null) {
            // check previous
            for (NodeDocument prev : getPreviousDocs(COMMIT_ROOT, revision)) {
                depth = prev.getCommitRootDepth(revision);
                if (depth != null) {
                    break;
                }
            }
        }
        return depth;
    }

    /**
     * Returns <code>true</code> if the given revision
     * {@link Utils#isCommitted(String)} in the revisions map (including
     * revisions split off to previous documents) and is visible from the
     * <code>readRevision</code>. This includes branch commits if the read
     * revision is on the same branch and is equal or newer than the revision
     * to check.
     *
     * @param revision  the revision to check.
     * @param commitValue the commit value of the revision to check.
     * @param readRevision the read revision.
     * @return <code>true</code> if the revision is visible, otherwise
     *         <code>false</code>.
     */
    private boolean isVisible(@NotNull RevisionContext context,
                              @NotNull Revision revision,
                              @NotNull String commitValue,
                              @NotNull RevisionVector readRevision) {
        if (Utils.isCommitted(commitValue)) {
            Branch b = context.getBranches().getBranch(readRevision);
            if (b == null) {
                // readRevision is not from a branch commit, though it may
                // still be a branch revision when it references the base
                // of a new branch that has not yet been created. In that
                // case the branch revision is equivalent with the trunk
                // revision.

                // resolve commit revision
                revision = resolveCommitRevision(revision, commitValue);
                // compare resolved revision as is
                return !readRevision.isRevisionNewer(revision);
            } else {
                // read revision is on a branch and the change is committed
                // get the base revision of the branch and check
                // if change is visible from there
                RevisionVector baseRev = b.getBase(readRevision.getBranchRevision());
                revision = resolveCommitRevision(revision, commitValue);
                return !baseRev.isRevisionNewer(revision);
            }
        } else {
            // branch commit (not merged)
            // read as RevisionVector, even though this should be
            // a Revision only. See OAK-4840
            RevisionVector branchCommit = RevisionVector.fromString(commitValue);
            if (branchCommit.getBranchRevision().getClusterId() != context.getClusterId()) {
                // this is an unmerged branch commit from another cluster node,
                // hence never visible to us
                return false;
            } else {
                // unmerged branch change with local clusterId
                Branch b = context.getBranches().getBranch(readRevision);
                if (b == null) {
                    // reading on trunk never sees changes on an unmerged branch
                    return false;
                } else if (b.containsCommit(revision)) {
                    // read revision is on the same branch as the
                    // unmerged branch changes -> compare revisions as is
                    return !readRevision.isRevisionNewer(revision);
                } else {
                    // read revision is on a different branch than the
                    // unmerged branch changes -> never visible
                    return false;
                }
            }
        }
    }

    /**
     * Returns the commit value for the given <code>revision</code>.
     *
     * @param revision a revision.
     * @return the commit value or <code>null</code> if the revision is unknown.
     */
    @Nullable
    private String getCommitValue(Revision revision) {
        String value = getLocalRevisions().get(revision);
        if (value == null) {
            // check previous
            for (NodeDocument prev : getPreviousDocs(REVISIONS, revision)) {
                value = prev.getCommitValue(revision);
                if (value != null) {
                    break;
                }
            }
        }
        return value;
    }

    /**
     * Check if there may be more recent values in a previous document and thus a
     * complete map check is required.
     *
     * @param localValue value as resolved from local value map
     * @param local      local value map
     * @param context    the revision context
     * @return false if it is most recent, true otherwise
     */
    private boolean requiresCompleteMapCheck(@Nullable Value localValue,
            @NotNull SortedMap<Revision, String> local,
            @NotNull RevisionContext context) {
        if (localValue != null
                && !getPreviousRanges().isEmpty()
                && !isMostRecentCommitted(local, localValue.revision, context)) {
            // not reading the most recent value, we may need to
            // consider previous documents as well
            for (Revision prev : getPreviousRanges().keySet()) {
                if (prev.compareRevisionTimeThenClusterId(localValue.revision) > 0) {
                    // a previous document has more recent changes
                    // than localValue.revision
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get the latest property value smaller or equal the readRevision revision.
     *
     * @param valueMap the sorted revision-value map
     * @param readRevision the maximum revision
     * @param validRevisions map of revision to commit value considered valid
     *                       against the given readRevision.
     * @param lastRevs to keep track of the most recent modification.
     * @return the latest value from the {@code readRevision} point of view.
     */
    @Nullable
    private Value getLatestValue(@NotNull RevisionContext context,
                                 @NotNull Iterable<Map.Entry<Revision, String>> valueMap,
                                 @NotNull RevisionVector readRevision,
                                 @NotNull Map<Revision, String> validRevisions,
                                 @NotNull LastRevs lastRevs) {
        for (Map.Entry<Revision, String> entry : valueMap) {
            Revision propRev = entry.getKey();
            String commitValue = validRevisions.get(propRev);
            if (commitValue == null) {
                commitValue = context.getCommitValue(propRev, this);
            }
            if (commitValue == null) {
                continue;
            }
            // resolve revision
            Revision commitRev = resolveCommitRevision(propRev, commitValue);
            if (Utils.isCommitted(commitValue)) {
                lastRevs.update(commitRev);
            } else {
                // branch commit
                lastRevs.updateBranch(commitRev.asBranchRevision());
            }

            if (isValidRevision(context, propRev, commitValue, readRevision, validRevisions)) {
                return new Value(commitRev, entry);
            }
        }
        return null;
    }

    @NotNull
    public Path getPath() {
        return Path.fromString(getPathString());
    }

    @NotNull
    private String getPathString() {
        String p = (String) get(PATH);
        if (p != null) {
            return p;
        }
        return Utils.getPathFromId(getId());
    }

    @NotNull
    Map<Revision, String> getDeleted() {
        return ValueMap.create(this, DELETED);
    }
    
    public String asString() {
        JsopWriter json = new JsopBuilder();
        toJson(json, data);
        return json.toString();
    }
    
    @SuppressWarnings("unchecked")
    private static void toJson(JsopWriter json, Map<?, Object> map) {
        for (Entry<?, Object>e : map.entrySet()) {
            json.key(e.getKey().toString());
            Object value = e.getValue();
            if (value == null) {
                json.value(null);
            } else if (value instanceof Boolean) {
                json.value((Boolean) value);
            } else if (value instanceof Long) {
                json.value((Long) value);
            } else if (value instanceof Integer) {
                json.value((Integer) value);
            } else if (value instanceof Map) {
                json.object();
                toJson(json, (Map<Object, Object>) value);
                json.endObject();
            } else if (value instanceof Revision) {
                json.value(value.toString());
            } else {
                json.value((String) value);
            }        
        }
    }
    
    public static NodeDocument fromString(DocumentStore store, String s) {
        JsopTokenizer json = new JsopTokenizer(s);
        NodeDocument doc = new NodeDocument(store);
        while (true) {
            if (json.matches(JsopReader.END)) {
                break;
            }
            String k = json.readString();
            json.read(':');
            if (json.matches(JsopReader.END)) {
                break;
            }
            doc.put(k, fromJson(json));
            json.matches(',');
        }
        doc.seal();
        return doc;
    }
    
    private static Object fromJson(JsopTokenizer json) {
        switch (json.read()) {
        case JsopReader.NULL:
            return null;
        case JsopReader.TRUE:
            return true;
        case JsopReader.FALSE:
            return false;
        case JsopReader.NUMBER:
            return Long.parseLong(json.getToken());
        case JsopReader.STRING:
            return json.getToken();
        case '{':
            TreeMap<Revision, Object> map = new TreeMap<Revision, Object>(REVERSE);
            while (true) {
                if (json.matches('}')) {
                    break;
                }
                String k = json.readString();
                json.read(':');
                map.put(Revision.fromString(k), fromJson(json));
                json.matches(',');
            }
            return map;
        }
        throw new IllegalArgumentException(json.readRawValue());
    }

    /**
     * A property value / revision combination.
     */
    private static final class Value {

        final Revision revision;
        /**
         * valueEntry contains both the underlying (commit) revision and
         * the (String) value of a property. valueEntry is never null.
         * valueEntry.getValue() being {@code null}
         * indicates the property was removed.
         */
        final Map.Entry<Revision, String> valueEntry;

        Value(@NotNull Revision mergeRevision, @NotNull Map.Entry<Revision, String> valueEntry) {
            this.revision = requireNonNull(mergeRevision);
            this.valueEntry = valueEntry;
        }
    }

    private static final class ValueComparator implements
            Comparator<Entry<Revision, String>> {

        static final Comparator<Entry<Revision, String>> INSTANCE = new ValueComparator();

        static final Comparator<Entry<Revision, String>> REVERSE = Collections.reverseOrder(INSTANCE);

        private static final Ordering<String> STRING_ORDERING = Ordering.natural().nullsFirst();

        @Override
        public int compare(Entry<Revision, String> o1,
                           Entry<Revision, String> o2) {
            int cmp = StableRevisionComparator.INSTANCE.compare(o1.getKey(), o2.getKey());
            if (cmp != 0) {
                return cmp;
            }
            return STRING_ORDERING.compare(o1.getValue(), o2.getValue());
        }
    }
}
