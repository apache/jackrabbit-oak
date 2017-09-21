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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator.REVERSE;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isRevisionNewer;

/**
 * A document storing data about a node.
 */
public final class NodeDocument extends Document implements CachedNodeDocument{

    /**
     * Marker document, which indicates the document does not exist.
     */
    public static final NodeDocument NULL = new NodeDocument(new MemoryDocumentStore());

    static {
        NULL.seal();
    }

    static final Logger LOG = LoggerFactory.getLogger(NodeDocument.class);

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
    static final int DOC_SIZE_THRESHOLD = 256 * 1024;

    /**
     * Only split off at least this number of revisions.
     */
    static final int NUM_REVS_THRESHOLD = 100;

    /**
     * The split ratio. Only split data to an old document when at least
     * 30% of the data can be moved.
     */
    static final float SPLIT_RATIO = 0.3f;

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
    private static final String DELETED = "_deleted";

    /**
     * Flag indicating that whether this node was ever deleted.
     * Its just used as a hint. If set to true then it indicates that
     * node was once deleted.
     *
     * <p>Note that a true value does not mean that node should be considered
     * deleted as it might have been resurrected in later revision</p>
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


    /**
     * Properties to ignore when a document is split.
     */
    static final Set<String> IGNORE_ON_SPLIT = ImmutableSet.of(
            ID, MOD_COUNT, MODIFIED_IN_SECS, PREVIOUS, LAST_REV, CHILDREN_FLAG,
            HAS_BINARY_FLAG, PATH, DELETED_ONCE, COLLISIONS);

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
    private final AtomicLong lastCheckTime = new AtomicLong(System.currentTimeMillis());

    private final long creationTime;

    NodeDocument(@Nonnull DocumentStore store) {
        this(store, System.currentTimeMillis());
    }

    /**
     * Required for serialization
     *
     * @param store the document store.
     * @param creationTime time at which it was created. Would be different from current time
     *                     in case of being resurrected from a serialized for
     */
    public NodeDocument(@Nonnull DocumentStore store, long creationTime) {
        this.store = checkNotNull(store);
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
    @Nonnull
    public Map<Revision, String> getValueMap(@Nonnull String key) {
        if (IGNORE_ON_SPLIT.contains(key)) {
            return Collections.emptyMap();
        } else {
            return ValueMap.create(this, key);
        }
    }

    /**
     * @return the system time this object was created.
     */
    @Override
    public long getCreated() {
        return creationTime;
    }

    /**
     * See also {@link #MODIFIED_IN_SECS}.
     *
     * @return the time in seconds this document was last modified with five
     *          seconds precision. Returns {@code null} if none is set.
     */
    @CheckForNull
    public Long getModified() {
        return (Long) get(MODIFIED_IN_SECS);
    }

    /**
     * Returns <tt>true</tt> if this node possibly has children.
     * If false then that indicates that there are no child
     *
     * @return <tt>true</tt> if this node has children
     */
    public boolean hasChildren() {
        Boolean childrenFlag = (Boolean) get(CHILDREN_FLAG);
        return childrenFlag != null && childrenFlag;
    }

    /**
     * Returns <tt>true</tt> if this document was ever deleted in past.
     */
    public boolean wasDeletedOnce() {
        Boolean deletedOnceFlag = (Boolean) get(DELETED_ONCE);
        return deletedOnceFlag != null && deletedOnceFlag;
    }

    /**
     * Checks if this document has been modified after the given lastModifiedTime
     *
     * @param lastModifiedTime time to compare against in millis
     * @return <tt>true</tt> if this document was modified after the given
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
     * @return <tt>true</tt> if timestamp of maximum revision stored in this document
     * is less than than the passed revision timestamp
     */
    public boolean hasAllRevisionLessThan(long maxRevisionTime){
        Long maxRevTimeStamp = (Long) get(SD_MAX_REV_TIME_IN_SECS);
        return maxRevTimeStamp != null && maxRevTimeStamp < TimeUnit.MILLISECONDS.toSeconds(maxRevisionTime);
    }

    /**
     * Determines if this document is a split document
     *
     * @return <tt>true</tt> if this document is a split document
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
    @Override
    public void markUpToDate(long checkTime) {
        lastCheckTime.set(checkTime);
    }

    /**
     * Returns true if the document has already been checked for consistency
     * in current cycle.
     *
     * @param lastCheckTime time at which current cycle started
     * @return if the document was checked
     */
    @Override
    public boolean isUpToDate(long lastCheckTime) {
        return lastCheckTime <= this.lastCheckTime.get();
    }

    /**
     * Returns the last time when this object was checked for consistency.
     *
     * @return the last check time
     */
    @Override
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
    @Nonnull
    public String getMainPath() {
        String p = getPath();
        if (p.startsWith("p")) {
            p = PathUtils.getAncestorPath(p, 2);
            if (p.length() == 1) {
                return "/";
            } else {
                return p.substring(1);
            }
        } else {
            return p;
        }
    }

    /**
     * @return a map of the last known revision for each clusterId.
     */
    @Nonnull
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
     * Returns <code>true</code> if the given <code>revision</code> is marked
     * committed.
     *
     * @param revision the revision.
     * @return <code>true</code> if committed; <code>false</code> otherwise.
     */
    public boolean isCommitted(@Nonnull Revision revision) {
        NodeDocument commitRootDoc = getCommitRoot(checkNotNull(revision));
        if (commitRootDoc == null) {
            return false;
        }
        String value = commitRootDoc.getLocalRevisions().get(revision);
        if (value != null) {
            return Utils.isCommitted(value);
        }
        // check previous docs
        for (NodeDocument prev : commitRootDoc.getPreviousDocs(REVISIONS, revision)) {
            if (prev.containsRevision(revision)) {
                return prev.isCommitted(revision);
            }
        }
        return false;
    }

    /**
     * Returns the commit revision for the change with the given revision.
     *
     * @param revision the revision of a change.
     * @return the commit revision of the change or {@code null} if the change
     *          is not committed or unknown.
     */
    @CheckForNull
    public Revision getCommitRevision(@Nonnull Revision revision) {
        NodeDocument commitRoot = getCommitRoot(checkNotNull(revision));
        if (commitRoot == null) {
            return null;
        }
        String value = commitRoot.getCommitValue(revision);
        if (Utils.isCommitted(value)) {
            return Utils.resolveCommitRevision(revision, value);
        }
        return null;
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
     * are the {@link #REVISIONS} entries where {@link Utils#isCommitted(String)}
     * returns false.
     *
     * <p>
     *     <bold>Note</bold> - This method should only be invoked upon startup
     *     as then only we can safely assume that these revisions would not be
     *     committed
     * </p>
     *
     * @param context the revision context.
     * @return count of the revision entries purged
     */
    int purgeUncommittedRevisions(RevisionContext context) {
        // only look at revisions in this document.
        // uncommitted revisions are not split off
        Map<Revision, String> valueMap = getLocalRevisions();
        UpdateOp op = new UpdateOp(getId(), false);
        int purgeCount = 0;
        for (Map.Entry<Revision, String> commit : valueMap.entrySet()) {
            if (!Utils.isCommitted(commit.getValue())) {
                Revision r = commit.getKey();
                if (r.getClusterId() == context.getClusterId()) {
                    purgeCount++;
                    op.removeMapEntry(REVISIONS, r);
                }
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.NODES, op);
        }
        return purgeCount;
    }

    /**
     * Purge collision markers with the local clusterId on this document. Use
     * only on start when there are no ongoing or pending commits.
     *
     * @param context the revision context.
     * @return the number of removed collision markers.
     */
    int purgeCollisionMarkers(RevisionContext context) {
        Map<Revision, String> valueMap = getLocalMap(COLLISIONS);
        UpdateOp op = new UpdateOp(getId(), false);
        int purgeCount = 0;
        for (Map.Entry<Revision, String> commit : valueMap.entrySet()) {
            Revision r = commit.getKey();
            if (r.getClusterId() == context.getClusterId()) {
                purgeCount++;
                removeCollision(op, r);
            }
        }

        if (op.hasChanges()) {
            store.findAndUpdate(Collection.NODES, op);
        }
        return purgeCount;
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
    public String getCommitRootPath(Revision revision) {
        String depth = getCommitRootDepth(revision);
        if (depth != null) {
            return getPathAtDepth(depth);
        }
        return null;
    }

    /**
     * Get the revision of the latest change made to this node.
     *
     * @param context the revision context
     * @param baseRev the base revision of the current change.
     * @param changeRev the revision of the current change
     * @param branch the branch associated with the current change or
     *              {@code null} if {@code changeRev} is not a branch commit.
     * @param collisions concurrent changes preceding <code>changeRev</code>.
     * @return the revision, or null if deleted
     */
    @CheckForNull
    Revision getNewestRevision(final RevisionContext context,
                               final Revision baseRev,
                               final Revision changeRev,
                               final Branch branch,
                               final Set<Revision> collisions) {
        checkArgument(!baseRev.isBranch() || branch != null,
                "Branch must be non-null if baseRev is a branch revision");
        final Map<Revision, String> validRevisions = Maps.newHashMap();
        Predicate<Revision> predicate = new Predicate<Revision>() {
            @Override
            public boolean apply(Revision input) {
                if (input.equals(changeRev)) {
                    return false;
                }
                if (isValidRevision(context, input, null, changeRev, validRevisions)) {
                    return true;
                }
                collisions.add(input);
                return false;
            }
        };

        Revision newestRev = null;
        int localClusterId = context.getClusterId();
        // check local commits first
        SortedMap<Revision, String> revisions = getLocalRevisions();
        SortedMap<Revision, String> commitRoots = getLocalCommitRoot();
        // go through all local commits and consider all
        // changes from another cluster node not yet visible from
        // the base version as collision
        for (Revision r : concat(revisions.keySet(), commitRoots.keySet())) {
            if (r.getClusterId() != localClusterId
                    && isRevisionNewer(context, r, baseRev)) {
                collisions.add(r);
            }
        }
        Iterator<Revision> it = filter(Iterables.mergeSorted(
                ImmutableList.of(revisions.keySet(), commitRoots.keySet()),
                revisions.comparator()), predicate).iterator();
        if (it.hasNext()) {
            newestRev = it.next();
        } else {
            // check full history (only needed in rare cases)
            if (LOG.isDebugEnabled()) {
                LOG.debug("getNewestRevision() with changeRev {} on {}, " +
                                "_revisions {}, _commitRoot {}",
                        changeRev, getId(), getLocalRevisions(), getLocalCommitRoot());
            }
            it = filter(getAllChanges(), predicate).iterator();
            if (it.hasNext()) {
                newestRev = it.next();
            }
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
    boolean isValidRevision(@Nonnull RevisionContext context,
                            @Nonnull Revision rev,
                            @Nullable String commitValue,
                            @Nonnull Revision readRevision,
                            @Nonnull Map<Revision, String> validRevisions) {
        if (validRevisions.containsKey(rev)) {
            return true;
        }
        NodeDocument doc = getCommitRoot(rev);
        if (doc == null) {
            return false;
        }
        if (doc.isCommitted(context, rev, commitValue, readRevision)) {
            validRevisions.put(rev, commitValue);
            return true;
        }
        return false;
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
    @CheckForNull
    public DocumentNodeState getNodeAtRevision(@Nonnull DocumentNodeStore nodeStore,
                                               @Nonnull Revision readRevision,
                                               @Nullable Revision lastModified) {
        Map<Revision, String> validRevisions = Maps.newHashMap();
        Revision min = getLiveRevision(nodeStore, readRevision, validRevisions);
        if (min == null) {
            // deleted
            return null;
        }
        String path = getPath();
        DocumentNodeState n = new DocumentNodeState(nodeStore, path, readRevision, hasChildren());
        Revision lastRevision = min;
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
            Value value = getLatestValue(nodeStore, local,
                    min, readRevision, validRevisions);

            // check if there may be more recent values in a previous document
            if (!getPreviousRanges().isEmpty()) {
                if (!isMostRecentCommitted(nodeStore, local, value.revision)) {
                    // not reading the most recent value, we may need to
                    // consider previous documents as well
                    Revision newestPrev = getPreviousRanges().firstKey();
                    if (isRevisionNewer(nodeStore, newestPrev, value.revision)) {
                        // a previous document has more recent changes
                        // than value.revision
                        value = null;
                    }
                }
            }

            if (value == null && !getPreviousRanges().isEmpty()) {
                // check complete revision history
                value = getLatestValue(nodeStore, getValueMap(key),
                        min, readRevision, validRevisions);
            }
            String propertyName = Utils.unescapePropertyName(key);
            String v = value != null ? value.value : null;
            n.setProperty(propertyName, v);
            // keep track of when this node was last modified
            if (value != null && isRevisionNewer(nodeStore, value.revision, lastRevision)) {
                lastRevision = value.revision;
            }
        }

        // lastRevision now points to the revision when this node was
        // last modified directly. but it may also have been 'modified'
        // by an operation on a descendant node, which is tracked in
        // _lastRev.

        // when was this node last modified?
        Branch branch = nodeStore.getBranches().getBranch(readRevision);
        Map<Integer, Revision> lastRevs = Maps.newHashMap(getLastRev());
        // overlay with unsaved last modified from this instance
        if (lastModified != null) {
            lastRevs.put(nodeStore.getClusterId(), lastModified);
        }
        Revision branchBase = null;
        if (branch != null) {
            branchBase = branch.getBase(readRevision);
        }
        for (Revision r : lastRevs.values()) {
            // ignore if newer than readRevision
            if (isRevisionNewer(nodeStore, r, readRevision)) {
                // the node has a _lastRev which is newer than readRevision
                // this means we don't know when this node was
                // modified by an operation on a descendant node between
                // current lastRevision and readRevision. therefore we have
                // to stay on the safe side and use readRevision
                lastRevision = readRevision;
                continue;
            } else if (branchBase != null && isRevisionNewer(nodeStore, r, branchBase)) {
                // readRevision is on a branch and the node has a
                // _lastRev which is newer than the base of the branch
                // we cannot use this _lastRev because it is not visible
                // from this branch. highest possible revision of visible
                // changes is the base of the branch
                r = branchBase;
            }
            if (revisionAreAmbiguous(nodeStore, r, lastRevision)) {
                // _lastRev entries from multiple cluster nodes are ambiguous
                // use readRevision to make sure read is consistent
                lastRevision = readRevision;
            } else if (isRevisionNewer(nodeStore, r, lastRevision)) {
                lastRevision = r;
            }
        }
        if (branch != null) {
            // read from a branch
            // -> possibly overlay with unsaved last revs from branch
            Revision r = branch.getUnsavedLastRevision(path, readRevision);
            if (r != null) {
                lastRevision = r.asBranchRevision();
            }
        }
        n.setLastRevision(lastRevision);
        return n;
    }

    /**
     * Get the earliest (oldest) revision where the node was alive at or before
     * the provided revision, if the node was alive at the given revision.
     *
     * @param context the revision context
     * @param maxRev the maximum revision to return
     * @param validRevisions the map of revisions to commit value already
     *                       checked against maxRev and considered valid.
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    @CheckForNull
    public Revision getLiveRevision(RevisionContext context, Revision maxRev,
                                    Map<Revision, String> validRevisions) {
        // check local deleted map first
        Value value = getLatestValue(context, getLocalDeleted(),
                null, maxRev, validRevisions);
        if (value.value == null && !getPreviousRanges().isEmpty()) {
            // need to check complete map
            value = getLatestValue(context, getDeleted(),
                    null, maxRev, validRevisions);
        }

        return "false".equals(value.value) ? value.revision : null;
    }

    /**
     * Returns <code>true</code> if the given operation is conflicting with this
     * document.
     *
     * @param op the update operation.
     * @param baseRevision the base revision for the update operation.
     * @param commitRevision the commit revision of the update operation.
     * @param context the revision context.
     * @param enableConcurrentAddRemove feature flag for OAK-2673.
     * @return <code>true</code> if conflicting, <code>false</code> otherwise.
     */
    boolean isConflicting(@Nonnull UpdateOp op,
                          @Nonnull Revision baseRevision,
                          @Nonnull Revision commitRevision,
                          @Nonnull RevisionContext context,
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

            if (isRevisionNewer(context, entry.getKey(), baseRevision)) {
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
            for (Revision rev : getChanges(name, baseRevision, context)) {
                if (rev.equals(commitRevision)) {
                    continue;
                }
                if (isRevisionNewer(context, rev, baseRevision)) {
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
        String path = getPath();
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
     * @return the split operations.
     */
    @Nonnull
    public Iterable<UpdateOp> split(@Nonnull RevisionContext context,
                                    @Nonnull Revision head) {
        return SplitOperations.forDocument(this, context, head, NUM_REVS_THRESHOLD);
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
    @Nonnull
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
    @Nonnull
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
    @Nonnull
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
    @Nonnull
    Iterable<NodeDocument> getPreviousDocs(@Nonnull final String property,
                                           @Nullable final Revision revision) {
        if (getPreviousRanges().isEmpty()) {
            return Collections.emptyList();
        }
        if (revision == null) {
            return new PropertyHistory(this, property);
        } else {
            final String mainPath = getMainPath();
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
                    LOG.warn("Document with previous revisions not found: " + prevId);
                }
            }

            // didn't find entry -> scan through remaining head ranges
            return filter(transform(getPreviousRanges().headMap(revision).entrySet(),
                    new Function<Map.Entry<Revision, Range>, NodeDocument>() {
                @Override
                public NodeDocument apply(Map.Entry<Revision, Range> input) {
                    if (input.getValue().includes(revision)) {
                       return getPreviousDoc(input.getKey(), input.getValue());
                    }
                    return null;
                }
            }), new Predicate<NodeDocument>() {
                @Override
                public boolean apply(@Nullable NodeDocument input) {
                    return input != null && input.getValueMap(property).containsKey(revision);
                }
            });
        }
    }

    NodeDocument getPreviousDocument(String prevId){
        //Use the maxAge variant such that in case of Mongo call for
        //previous doc are directed towards replicas first
        LOG.trace("get previous document {}", prevId);
        return store.find(Collection.NODES, prevId, Integer.MAX_VALUE);
    }

    @Nonnull
    Iterator<NodeDocument> getAllPreviousDocs() {
        if (getPreviousRanges().isEmpty()) {
            return Iterators.emptyIterator();
        }
        //Currently this method would fire one query per previous doc
        //If that poses a problem we can try to find all prev doc by relying
        //on property that all prevDoc id would starts <depth+2>:p/path/to/node
        return new AbstractIterator<NodeDocument>(){
            private Queue<Map.Entry<Revision, Range>> previousRanges =
                    Queues.newArrayDeque(getPreviousRanges().entrySet());
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
    @Nonnull
    Iterator<NodeDocument> getPreviousDocLeaves() {
        if (getPreviousRanges().isEmpty()) {
            return Iterators.emptyIterator();
        }
        // create a mutable copy
        final NavigableMap<Revision, Range> ranges = Maps.newTreeMap(getPreviousRanges());
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

    @CheckForNull
    private NodeDocument getPreviousDoc(Revision rev, Range range){
        int h = range.height;
        String prevId = Utils.getPreviousIdFor(getMainPath(), rev, h);
        NodeDocument prev = getPreviousDocument(prevId);
        if (prev != null) {
            return prev;
        } else {
            LOG.warn("Document with previous revisions not found: " + prevId);
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
        final SortedSet<Revision> stack = Sets.newTreeSet(REVERSE);
        // initialize with local revisions and commitRoot entries
        stack.addAll(getLocalCommitRoot().keySet());
        stack.addAll(getLocalRevisions().keySet());
        if (getPreviousRanges().isEmpty()) {
            return stack;
        }
        return new Iterable<Revision>() {
            @Override
            public Iterator<Revision> iterator() {
                final Iterator<NodeDocument> previousDocs = getPreviousDocLeaves();
                return new AbstractIterator<Revision>() {
                    private NodeDocument nextDoc;
                    private Revision nextRevision;
                    @Override
                    protected Revision computeNext() {
                        if (stack.isEmpty()) {
                            return endOfData();
                        }
                        Revision next = stack.first();
                        stack.remove(next);
                        fillStackIfNeeded();
                        return next;
                    }

                    private void fillStackIfNeeded() {
                        for (;;) {
                            fetchNextDoc();

                            // no more changes to compare with
                            if (nextDoc == null) {
                                return;
                            }

                            // check if current top revision is still newer than
                            // most recent revision of next document
                            if (!stack.isEmpty()) {
                                Revision top = stack.first();
                                if (top.compareRevisionTimeThenClusterId(nextRevision) > 0) {
                                    return;
                                }
                            }

                            // if we get here, we need to pull in changes
                            // from nextDoc
                            Iterables.addAll(stack, nextDoc.getAllChanges());
                            nextDoc = null;
                            nextRevision = null;
                        }
                    }

                    /**
                     * Fetch the next document if {@code nextDoc} is
                     * {@code null} and there are more documents.
                     */
                    private void fetchNextDoc() {
                        for (;;) {
                            if (nextDoc != null) {
                                break;
                            }
                            if (!previousDocs.hasNext()) {
                                // no more previous docs
                                break;
                            }
                            nextDoc = previousDocs.next();
                            Iterator<Revision> changes = nextDoc.getAllChanges().iterator();
                            if (changes.hasNext()) {
                                nextRevision = changes.next();
                                break;
                            } else {
                                // empty document, try next
                                nextDoc = null;
                            }
                        }
                    }
                };
            }
        };
    }

    /**
     * Returns all changes for the given property back to {@code min} revision
     * (exclusive). The revisions include committed as well as uncommitted
     * changes.
     *
     * @param property the name of the property.
     * @param min the lower bound revision (exclusive).
     * @param context the revision context.
     * @return changes back to {@code min} revision.
     */
    @Nonnull
    Iterable<Revision> getChanges(@Nonnull final String property,
                                  @Nonnull final Revision min,
                                  @Nonnull final RevisionContext context) {
        return new Iterable<Revision>() {
            @Override
            public Iterator<Revision> iterator() {
                final Set<Revision> changes = getValueMap(property).keySet();
                final Set<Integer> clusterIds = Sets.newHashSet();
                for (Revision r : getLocalMap(property).keySet()) {
                    clusterIds.add(r.getClusterId());
                }
                for (Range r : getPreviousRanges().values()) {
                    if (isRevisionNewer(context, r.high, min)) {
                        clusterIds.add(r.high.getClusterId());
                    }
                }
                final Iterator<Revision> unfiltered = changes.iterator();
                return new AbstractIterator<Revision>() {
                    @Override
                    protected Revision computeNext() {
                        while (unfiltered.hasNext()) {
                            Revision next = unfiltered.next();
                            if (isRevisionNewer(context, next, min)) {
                                return next;
                            } else {
                                // further revisions with this clusterId
                                // are older than min revision
                                clusterIds.remove(next.getClusterId());
                                // no more revisions to check
                                if (clusterIds.isEmpty()) {
                                    return endOfData();
                                }
                            }
                        }
                        return endOfData();
                    }
                };
            }
        };

    }

    /**
     * Returns the local value map for the given key.
     *
     * @param key the key.
     * @return local value map.
     */
    @Nonnull
    SortedMap<Revision, String> getLocalMap(String key) {
        @SuppressWarnings("unchecked")
        SortedMap<Revision, String> map = (SortedMap<Revision, String>) data.get(key);
        if (map == null) {
            map = ValueMap.EMPTY;
        }
        return map;
    }

    /**
     * @return the {@link #REVISIONS} stored on this document.
     */
    @Nonnull
    SortedMap<Revision, String> getLocalRevisions() {
        return getLocalMap(REVISIONS);
    }

    @Nonnull
    SortedMap<Revision, String> getLocalCommitRoot() {
        return getLocalMap(COMMIT_ROOT);
    }

    @Nonnull
    SortedMap<Revision, String> getLocalDeleted() {
        return getLocalMap(DELETED);
    }

    @Nonnull
    SortedMap<Revision, String> getStalePrev() {
        return getLocalMap(STALE_PREV);
    }

    //-------------------------< UpdateOp modifiers >---------------------------

    public static void setChildrenFlag(@Nonnull UpdateOp op,
                                       boolean hasChildNode) {
        checkNotNull(op).set(CHILDREN_FLAG, hasChildNode);
    }

    public static void setModified(@Nonnull UpdateOp op,
                                   @Nonnull Revision revision) {
        checkNotNull(op).max(MODIFIED_IN_SECS, getModifiedInSecs(checkNotNull(revision).getTimestamp()));
    }

    public static void setRevision(@Nonnull UpdateOp op,
                                   @Nonnull Revision revision,
                                   @Nonnull String commitValue) {
        checkNotNull(op).setMapEntry(REVISIONS,
                checkNotNull(revision), checkNotNull(commitValue));
    }

    public static void unsetRevision(@Nonnull UpdateOp op,
                                     @Nonnull Revision revision) {
        checkNotNull(op).unsetMapEntry(REVISIONS, checkNotNull(revision));
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

    public static void removeRevision(@Nonnull UpdateOp op,
                                      @Nonnull Revision revision) {
        checkNotNull(op).removeMapEntry(REVISIONS, checkNotNull(revision));
    }

    public static void addCollision(@Nonnull UpdateOp op,
                                    @Nonnull Revision revision) {
        checkNotNull(op).setMapEntry(COLLISIONS, checkNotNull(revision),
                String.valueOf(true));
    }

    public static void removeCollision(@Nonnull UpdateOp op,
                                       @Nonnull Revision revision) {
        checkNotNull(op).removeMapEntry(COLLISIONS, checkNotNull(revision));
    }

    public static void setLastRev(@Nonnull UpdateOp op,
                                  @Nonnull Revision revision) {
        checkNotNull(op).setMapEntry(LAST_REV,
                new Revision(0, 0, revision.getClusterId()),
                revision.toString());
    }

    public static boolean hasLastRev(@Nonnull UpdateOp op, int clusterId) {
        return checkNotNull(op).getChanges().containsKey(
                new Key(LAST_REV, new Revision(0, 0, clusterId)));
    }

    public static void unsetLastRev(@Nonnull UpdateOp op, int clusterId) {
        checkNotNull(op).unsetMapEntry(LAST_REV,
                new Revision(0, 0, clusterId));
    }

    public static void setCommitRoot(@Nonnull UpdateOp op,
                                     @Nonnull Revision revision,
                                     int commitRootDepth) {
        checkNotNull(op).setMapEntry(COMMIT_ROOT, checkNotNull(revision),
                String.valueOf(commitRootDepth));
    }

    public static void removeCommitRoot(@Nonnull UpdateOp op,
                                        @Nonnull Revision revision) {
        checkNotNull(op).removeMapEntry(COMMIT_ROOT, revision);
    }

    public static void setDeleted(@Nonnull UpdateOp op,
                                  @Nonnull Revision revision,
                                  boolean deleted) {
        if(deleted) {
            //DELETED_ONCE would be set upon every delete.
            //possibly we can avoid that
            checkNotNull(op).set(DELETED_ONCE, Boolean.TRUE);
        }
        checkNotNull(op).setMapEntry(DELETED, checkNotNull(revision),
                String.valueOf(deleted));
    }

    public static void removeDeleted(@Nonnull UpdateOp op,
                                     @Nonnull Revision revision) {
        checkNotNull(op).removeMapEntry(DELETED, revision);
    }

    public static void setPrevious(@Nonnull UpdateOp op,
                                   @Nonnull Range range) {
        checkNotNull(op).setMapEntry(PREVIOUS, checkNotNull(range).high,
                range.getLowValue());
    }

    public static void removePrevious(@Nonnull UpdateOp op,
                                      @Nonnull Range range) {
        removePrevious(op, checkNotNull(range).high);
    }

    public static void removePrevious(@Nonnull UpdateOp op,
                                      @Nonnull Revision revision) {
        checkNotNull(op).removeMapEntry(PREVIOUS, checkNotNull(revision));
    }

    public static void setStalePrevious(@Nonnull UpdateOp op,
                                        @Nonnull Revision revision,
                                        int height) {
        checkNotNull(op).setMapEntry(STALE_PREV,
                checkNotNull(revision), String.valueOf(height));
    }

    public static void removeStalePrevious(@Nonnull UpdateOp op,
                                           @Nonnull Revision revision) {
        checkNotNull(op).removeMapEntry(STALE_PREV, checkNotNull(revision));
    }

    public static void setHasBinary(@Nonnull UpdateOp op) {
        checkNotNull(op).set(HAS_BINARY_FLAG, HAS_BINARY_VAL);
    }

    //----------------------------< internal >----------------------------------

    /**
     * Returns {@code true} if the given {@code revision} is more recent or
     * equal to the committed revision in {@code valueMap}. This method assumes
     * the given {@code revision} is committed.
     *
     * @param context the revision context.
     * @param valueMap the value map sorted most recent first.
     * @param revision a committed revision.
     * @return if {@code revision} is the most recent committed revision in the
     *          {@code valueMap}.
     */
    private boolean isMostRecentCommitted(RevisionContext context,
                                          SortedMap<Revision, String> valueMap,
                                          Revision revision) {
        if (valueMap.isEmpty()) {
            return true;
        }
        // shortcut when revision is the first key
        Revision first = valueMap.firstKey();
        if (!isRevisionNewer(context, first, revision)) {
            return true;
        }
        // need to check commit status
        for (Revision r : valueMap.keySet()) {
            Revision c = getCommitRevision(r);
            if (c != null) {
                return !isRevisionNewer(context, c, revision);
            }
        }
        // no committed revision found in valueMap
        return true;
    }

    /**
     * Returns {@code true} if the two revisions are ambiguous. That is, they
     * are from different cluster nodes and the comparison of the two revision
     * depends on the seen at revision and is different when just comparing the
     * timestamps of the revisions.
     *
     * @param context the revision context.
     * @param r1 the first revision.
     * @param r2 the second revision.
     * @return {@code true} if ambiguous, {@code false} otherwise.
     */
    static boolean revisionAreAmbiguous(@Nonnull RevisionContext context,
                                        @Nonnull Revision r1,
                                        @Nonnull Revision r2) {
        if (r1.getClusterId() == r2.getClusterId()) {
            return false;
        }
        int c1 = context.getRevisionComparator().compare(r1, r2);
        int c2 = r1.compareTo(r2);
        if (c1 == 0) {
            return c2 == 0;
        } else if (c1 < 0) {
            return c2 >= 0;
        } else {
            return c2 <= 0;
        }
    }

    /**
     * Returns the commit root document for the given revision. This may either
     * be this document or another one.
     *
     * @param rev a revision.
     * @return the commit root or <code>null</code> if there is none.
     */
    @CheckForNull
    private NodeDocument getCommitRoot(@Nonnull Revision rev) {
        // check local revisions and commitRoot first
        if (getLocalRevisions().containsKey(rev)) {
            return this;
        }
        String commitRootPath;
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
    @Nonnull
    private String getPathAtDepth(@Nonnull String depth) {
        if (checkNotNull(depth).equals("0")) {
            return "/";
        }
        String p = getPath();
        return PathUtils.getAncestorPath(p, PathUtils.getDepth(p) - Integer.parseInt(depth));
    }

    /**
     * Returns the commit root depth for the given revision. This method also
     * takes previous documents into account.
     *
     * @param revision get the commit root depth for this revision.
     * @return the depth or <code>null</code> if there is no commit root entry
     *         for the given revision on this document or previous documents.
     */
    @CheckForNull
    private String getCommitRootDepth(@Nonnull Revision revision) {
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
     * <code>readRevision</code>.
     *
     * @param revision  the revision to check.
     * @param commitValue the commit value of the revision to check or
     *                    <code>null</code> if unknown.
     * @param readRevision the read revision.
     * @return <code>true</code> if the revision is committed, otherwise
     *         <code>false</code>.
     */
    private boolean isCommitted(@Nonnull RevisionContext context,
                                @Nonnull Revision revision,
                                @Nullable String commitValue,
                                @Nonnull Revision readRevision) {
        if (commitValue == null) {
            commitValue = getCommitValue(revision);
        }
        if (commitValue == null) {
            return false;
        }
        if (Utils.isCommitted(commitValue)) {
            if (context.getBranches().getBranch(readRevision) == null
                    && !readRevision.isBranch()) {
                // resolve commit revision
                revision = Utils.resolveCommitRevision(revision, commitValue);
                // readRevision is not from a branch
                // compare resolved revision as is
                return !isRevisionNewer(context, revision, readRevision);
            } else {
                // on same merged branch?
                if (commitValue.equals(getCommitValue(readRevision.asTrunkRevision()))) {
                    // compare unresolved revision
                    return !isRevisionNewer(context, revision, readRevision);
                }
            }
        } else {
            // branch commit (not merged)
            if (Revision.fromString(commitValue).getClusterId() != context.getClusterId()) {
                // this is an unmerged branch commit from another cluster node,
                // hence never visible to us
                return false;
            }
        }
        return includeRevision(context, Utils.resolveCommitRevision(revision, commitValue), readRevision);
    }

    /**
     * Returns the commit value for the given <code>revision</code>.
     *
     * @param revision a revision.
     * @return the commit value or <code>null</code> if the revision is unknown.
     */
    @CheckForNull
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
     * and smaller or equal the readRevision revision. The returned value will
     * provide the revision when the value was set between the {@code min} and
     * {@code readRevision}. The returned value will have a {@code null} value
     * contained if there is no valid change within the given range. In this
     * case the associated revision is {@code min} or {@code readRevision} if
     * no {@code min} is provided.
     *
     * @param valueMap the sorted revision-value map
     * @param min the minimum revision (null meaning unlimited)
     * @param readRevision the maximum revision
     * @param validRevisions map of revision to commit value considered valid
     *                       against the given readRevision.
     * @return the latest value from the {@code readRevision} point of view.
     */
    @Nonnull
    private Value getLatestValue(@Nonnull RevisionContext context,
                                 @Nonnull Map<Revision, String> valueMap,
                                 @Nullable Revision min,
                                 @Nonnull Revision readRevision,
                                 @Nonnull Map<Revision, String> validRevisions) {
        for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
            Revision propRev = entry.getKey();
            // ignore revisions newer than readRevision
            // -> these are not visible anyway
            if (isRevisionNewer(context, propRev, readRevision)) {
                continue;
            }
            String commitValue = validRevisions.get(propRev);
            if (commitValue == null) {
                // resolve revision
                NodeDocument commitRoot = getCommitRoot(propRev);
                if (commitRoot == null) {
                    continue;
                }
                commitValue = commitRoot.getCommitValue(propRev);
                if (commitValue == null) {
                    continue;
                }
            }
            if (min != null && isRevisionNewer(context, min,
                    Utils.resolveCommitRevision(propRev, commitValue))) {
                continue;
            }
            if (isValidRevision(context, propRev, commitValue, readRevision, validRevisions)) {
                // TODO: need to check older revisions as well?
                return new Value(
                        Utils.resolveCommitRevision(propRev, commitValue),
                        entry.getValue());
            }
        }

        Revision r = min != null ? min : readRevision;
        return new Value(r, null);
    }

    @Override
    public String getPath() {
        String p = (String) get(PATH);
        if (p != null) {
            return p;
        }
        return Utils.getPathFromId(getId());
    }

    @Nonnull
    private Map<Revision, String> getDeleted() {
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
     * The list of children for a node. The list might be complete or not, in
     * which case it only represents a block of children.
     */
    public static final class Children implements CacheValue, Cloneable {

        /**
         * The child node names, ordered as stored in DocumentStore.
         */
        ArrayList<String> childNames = new ArrayList<String>();

        /**
         * Whether the list is complete (in which case there are no other
         * children) or not.
         */
        boolean isComplete;

        @Override
        public int getMemory() {
            int size = 114;
            for (String name : childNames) {
                size += name.length() * 2 + 56;
            }
            return size;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Children clone() {
            try {
                Children clone = (Children) super.clone();
                clone.childNames = (ArrayList<String>) childNames.clone();
                return clone;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException();
            }
        }

        public String asString() {
            JsopWriter json = new JsopBuilder();
            if (isComplete) {
                json.key("isComplete").value(true);
            }
            if (childNames.size() > 0) {
                json.key("children").array();
                for (String c : childNames) {
                    json.value(c);
                }
                json.endArray();
            }
            return json.toString();            
        }
        
        public static Children fromString(String s) {
            JsopTokenizer json = new JsopTokenizer(s);
            Children children = new Children();
            while (true) {
                if (json.matches(JsopReader.END)) {
                    break;
                }
                String k = json.readString();
                json.read(':');
                if ("isComplete".equals(k)) {
                    children.isComplete = json.read() == JsopReader.TRUE;
                } else if ("children".equals(k)) {
                    json.read('[');
                    while (true) {
                        if (json.matches(']')) {
                            break;
                        }
                        String value = json.readString();
                        children.childNames.add(value);
                        json.matches(',');
                    }
                }
                if (json.matches(JsopReader.END)) {
                    break;
                }
                json.read(',');
            }
            return children;            
        }
        
    }

    /**
     * A property value / revision combination.
     */
    private static final class Value {

        final Revision revision;
        /**
         * The value of a property at the given revision. A {@code null} value
         * indicates the property was removed.
         */
        final String value;

        Value(@Nonnull Revision revision, @Nullable String value) {
            this.revision = checkNotNull(revision);
            this.value = value;
        }
    }
}
