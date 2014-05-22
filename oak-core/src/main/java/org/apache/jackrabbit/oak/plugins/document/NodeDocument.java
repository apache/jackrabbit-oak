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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.disjoint;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;

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
    static final String COLLISIONS = "_collisions";

    /**
     * The modified time in seconds (5 second resolution).
     */
    public static final String MODIFIED_IN_SECS = "_modified";

    private static final NavigableMap<Revision, Range> EMPTY_RANGE_MAP =
            Maps.unmodifiableNavigableMap(new TreeMap<Revision, Range>());

    /**
     * The list of revision to root commit depth mappings to find out if a
     * revision is actually committed. Depth 0 means the commit is in the root node,
     * depth 1 means one node below the root, and so on.
     */
    private static final String COMMIT_ROOT = "_commitRoot";

    /**
     * The number of previous documents (documents that contain old revisions of
     * this node). This property is only set if multiple documents per node
     * exist. This is the case when a node is updated very often in a short
     * time, such that the document gets very big.
     * <p>
     * Key: high revision
     * <p>
     * Value: low revision
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
    private static final String REVISIONS = "_revisions";

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
     * A document which is created from splitting a main document can be classified
     * into multiple types depending on the content i.e. weather it contains
     * REVISIONS, COMMIT_ROOT, property history etc
     */
    public static enum SplitDocType {
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
         */
        DEFAULT_NO_CHILD(20),
        /**
         * A split document which does not contain REVISIONS history
         */
        PROP_COMMIT_ONLY(30),
        /**
         * Its an intermediate split document which only contains version ranges
         * and does not contain any other attributes
         */
        INTERMEDIATE(40)
        ;

        final int type;

        private SplitDocType(int type){
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
    private static final Set<String> IGNORE_ON_SPLIT = ImmutableSet.of(
            ID, MOD_COUNT, MODIFIED_IN_SECS, PREVIOUS, LAST_REV, CHILDREN_FLAG,
            HAS_BINARY_FLAG, PATH, DELETED_ONCE, COLLISIONS);

    public static final long HAS_BINARY_VAL = 1;

    final DocumentStore store;

    /**
     * Parsed and sorted set of previous revisions.
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
    public SplitDocType getSplitDocType(){
        return SplitDocType.valueOf((Integer) get(SD_TYPE));
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
        if(flag == null){
            return false;
        }
        return flag.intValue() == HAS_BINARY_VAL;
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
        Map<Revision, String> valueMap = getLocalRevisions();
        SortedMap<Revision, Revision> revisions =
                new TreeMap<Revision, Revision>(context.getRevisionComparator());
        for (Map.Entry<Revision, String> commit : valueMap.entrySet()) {
            if (!Utils.isCommitted(commit.getValue())) {
                Revision r = commit.getKey();
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
    public String getCommitRootPath(Revision revision) {
        String depth = getCommitRootDepth(revision);
        if (depth != null) {
            if (depth.equals("0")) {
                return "/";
            }
            String p = getPath();
            return PathUtils.getAncestorPath(p,
                    PathUtils.getDepth(p) - Integer.parseInt(depth));
        }
        return null;
    }

    /**
     * Get the revision of the latest change made to this node.
     *
     * @param context the revision context
     * @param changeRev the revision of the current change
     * @param handler the conflict handler, which is called for concurrent changes
     *                preceding <code>changeRev</code>.
     * @return the revision, or null if deleted
     */
    @CheckForNull
    public Revision getNewestRevision(final RevisionContext context,
                                      final Revision changeRev,
                                      final CollisionHandler handler) {
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
                handler.concurrentModification(input);
                return false;
            }
        };

        Revision newestRev = null;
        // check local commits first
        SortedMap<Revision, String> revisions = getLocalRevisions();
        SortedMap<Revision, String> commitRoots = getLocalCommitRoot();
        Iterator<Revision> it = filter(Iterables.mergeSorted(
                Arrays.asList(revisions.keySet(), commitRoots.keySet()),
                revisions.comparator()), predicate).iterator();
        if (it.hasNext()) {
            newestRev = it.next();
        } else {
            // check full history (only needed in rare cases)
            it = filter(Iterables.mergeSorted(
                    Arrays.asList(
                            getValueMap(REVISIONS).keySet(),
                            getValueMap(COMMIT_ROOT).keySet()),
                    revisions.comparator()), predicate).iterator();
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
            // first check local map, which contains most recent values
            Value value = getLatestValue(nodeStore, getLocalMap(key),
                    min, readRevision, validRevisions);
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
            if (isRevisionNewer(nodeStore, r, lastRevision)) {
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
        if (value == null && !getPreviousRanges().isEmpty()) {
            // need to check complete map
            value = getLatestValue(context, getDeleted(),
                    null, maxRev, validRevisions);
        }
        return value != null && value.value.equals("false") ? value.revision : null;
    }

    /**
     * Returns <code>true</code> if the given operation is conflicting with this
     * document.
     *
     * @param op the update operation.
     * @param baseRevision the base revision for the update operation.
     * @param commitRevision the commit revision of the update operation.
     * @param context the revision context.
     * @return <code>true</code> if conflicting, <code>false</code> otherwise.
     */
    boolean isConflicting(@Nonnull UpdateOp op,
                                 @Nonnull Revision baseRevision,
                                 @Nonnull Revision commitRevision,
                                 @Nonnull RevisionContext context) {
        // did existence of node change after baseRevision?
        // only check local deleted map, which contains the most
        // recent values
        Map<Revision, String> deleted = getLocalDeleted();
        for (Map.Entry<Revision, String> entry : deleted.entrySet()) {
            if (entry.getKey().equals(commitRevision)) {
                continue;
            }
            if (isRevisionNewer(context, entry.getKey(), baseRevision)) {
                return true;
            }
        }

        for (Map.Entry<Key, Operation> entry : op.getChanges().entrySet()) {
            if (entry.getValue().type != Operation.Type.SET_MAP_ENTRY) {
                continue;
            }
            String name = entry.getKey().getName();
            if (DELETED.equals(name)) {
                // existence of node changed, this always conflicts with
                // any other concurrent change
                return true;
            }
            if (!Utils.isPropertyName(name)) {
                continue;
            }
            // was this property touched after baseRevision?
            for (Revision rev : getValueMap(name).keySet()) {
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
     * Returns update operations to split this document. The implementation may
     * decide to not return any operations if no splitting is required.
     *
     * @param context the revision context.
     * @return the split operations.
     */
    @Nonnull
    public Iterable<UpdateOp> split(@Nonnull RevisionContext context) {
        SortedMap<Revision, Range> previous = getPreviousRanges();
        // only consider if there are enough commits,
        // unless document is really big
        if (getLocalRevisions().size() + getLocalCommitRoot().size() <= NUM_REVS_THRESHOLD
                && getMemory() < DOC_SIZE_THRESHOLD
                && previous.size() < PREV_SPLIT_FACTOR) {
            return Collections.emptyList();
        }
        String path = getPath();
        String id = getId();
        if (id == null) {
            throw new IllegalStateException("document does not have an id: " + this);
        }
        // collect ranges and create a histogram of the height
        Map<Integer, List<Range>> prevHisto = Maps.newHashMap();
        for (Map.Entry<Revision, Range> entry : previous.entrySet()) {
            Revision rev = entry.getKey();
            if (rev.getClusterId() != context.getClusterId()) {
                continue;
            }
            Range r = entry.getValue();
            List<Range> list = prevHisto.get(r.getHeight());
            if (list == null) {
                list = new ArrayList<Range>();
                prevHisto.put(r.getHeight(), list);
            }
            list.add(r);
        }
        Map<String, NavigableMap<Revision, String>> splitValues
                = new HashMap<String, NavigableMap<Revision, String>>();
        for (String property : data.keySet()) {
            if (IGNORE_ON_SPLIT.contains(property)) {
                continue;
            }
            NavigableMap<Revision, String> splitMap
                    = new TreeMap<Revision, String>(context.getRevisionComparator());
            splitValues.put(property, splitMap);
            Map<Revision, String> valueMap = getLocalMap(property);
            // collect committed changes of this cluster node after the
            // most recent previous split revision
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();
                if (rev.getClusterId() != context.getClusterId()) {
                    continue;
                }
                if (isCommitted(rev)) {
                    splitMap.put(rev, entry.getValue());
                }
            }
        }

        List<UpdateOp> splitOps = Lists.newArrayList();
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
        UpdateOp main = null;
        if (high != null && low != null
                && (numValues >= NUM_REVS_THRESHOLD
                    || getMemory() > DOC_SIZE_THRESHOLD)) {
            // enough revisions to split off
            // move to another document
            main = new UpdateOp(id, false);
            setPrevious(main, new Range(high, low, 0));
            String oldPath = Utils.getPreviousPathFor(path, high, 0);
            UpdateOp old = new UpdateOp(Utils.getIdFromPath(oldPath), true);
            old.set(ID, old.getId());
            if (Utils.isLongPath(oldPath)) {
                old.set(PATH, oldPath);
            }
            for (String property : splitValues.keySet()) {
                NavigableMap<Revision, String> splitMap = splitValues.get(property);
                for (Map.Entry<Revision, String> entry : splitMap.entrySet()) {
                    Revision r = entry.getKey();
                    main.removeMapEntry(property, r);
                    old.setMapEntry(property, r, entry.getValue());
                }
            }
            // check size of old document
            NodeDocument oldDoc = new NodeDocument(store);
            UpdateUtils.applyChanges(oldDoc, old, context.getRevisionComparator());
            setSplitDocProps(this, oldDoc, old, high);
            // only split if enough of the data can be moved to old document
            if (oldDoc.getMemory() > getMemory() * SPLIT_RATIO
                    || numValues >= NUM_REVS_THRESHOLD) {
                splitOps.add(old);
            } else {
                main = null;
            }
        }

        // check if we need to create intermediate previous documents
        for (Map.Entry<Integer, List<Range>> entry : prevHisto.entrySet()) {
            if (entry.getValue().size() >= PREV_SPLIT_FACTOR) {
                if (main == null) {
                    main = new UpdateOp(id, false);
                }
                // calculate range new range
                Revision h = null;
                Revision l = null;
                for (Range r : entry.getValue()) {
                    if (h == null || isRevisionNewer(context, r.high, h)) {
                        h = r.high;
                    }
                    if (l == null || isRevisionNewer(context, l, r.low)) {
                        l = r.low;
                    }
                    removePrevious(main, r);
                }
                if (h == null || l == null) {
                    throw new IllegalStateException();
                }
                String prevPath = Utils.getPreviousPathFor(path, h, entry.getKey() + 1);
                String prevId = Utils.getIdFromPath(prevPath);
                UpdateOp intermediate = new UpdateOp(prevId, true);
                intermediate.set(ID, prevId);
                if (Utils.isLongPath(prevPath)) {
                    intermediate.set(PATH, prevPath);
                }
                setPrevious(main, new Range(h, l, entry.getKey() + 1));
                for (Range r : entry.getValue()) {
                    setPrevious(intermediate, r);
                }
                setIntermediateDocProps(intermediate, h);
                splitOps.add(intermediate);
            }
        }

        // main document must be updated last
        if (main != null && !splitOps.isEmpty()) {
            splitOps.add(main);
        }

        return splitOps;
    }

    /**
     * Returns previous revision ranges for this document. The revision keys are
     * sorted descending, newest first!
     *
     * @return the previous ranges for this document.
     */
    @Nonnull
    NavigableMap<Revision, Range> getPreviousRanges() {
        if (previous == null) {
            Map<Revision, String> map = getLocalMap(PREVIOUS);
            if (map.isEmpty()) {
                previous = EMPTY_RANGE_MAP;
            } else {
                NavigableMap<Revision, Range> transformed = new TreeMap<Revision, Range>(
                        StableRevisionComparator.REVERSE);
                for (Map.Entry<Revision, String> entry : map.entrySet()) {
                    Range r = Range.fromEntry(entry.getKey(), entry.getValue());
                    transformed.put(r.high, r);
                }
                previous = Maps.unmodifiableNavigableMap(transformed);
            }
        }
        return previous;
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
            return new PropertyHistory(store, this, property);
        } else {
            final String mainPath = getMainPath();
            // first try to lookup revision directly
            Map.Entry<Revision, Range> entry = getPreviousRanges().floorEntry(revision);
            if (entry != null) {
                Revision r = entry.getKey();
                int h = entry.getValue().height;
                String prevId = Utils.getPreviousIdFor(mainPath, r, h);
                NodeDocument prev = store.find(Collection.NODES, prevId);
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

    private NodeDocument getPreviousDoc(Revision rev, Range range){
        int h = range.height;
        String prevId = Utils.getPreviousIdFor(getMainPath(), rev, h);
        //TODO Use the maxAge variant such that in case of Mongo call for
        //previous doc are directed towards replicas first
        NodeDocument prev = store.find(Collection.NODES, prevId);
        if (prev != null) {
            return prev;
        } else {
            LOG.warn("Document with previous revisions not found: " + prevId);
        }
        return null;
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

    //-------------------------< UpdateOp modifiers >---------------------------

    public static void setChildrenFlag(@Nonnull UpdateOp op,
                                       boolean hasChildNode) {
        checkNotNull(op).set(CHILDREN_FLAG, hasChildNode);
    }

    public static void setModified(@Nonnull UpdateOp op,
                                   @Nonnull Revision revision) {
        checkNotNull(op).set(MODIFIED_IN_SECS, Commit.getModifiedInSecs(checkNotNull(revision).getTimestamp()));
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
        checkNotNull(op).removeMapEntry(PREVIOUS, checkNotNull(range).high);
    }

    public static void setHasBinary(@Nonnull UpdateOp op) {
        checkNotNull(op).set(HAS_BINARY_FLAG, HAS_BINARY_VAL);
    }

    //----------------------------< internal modifiers >------------------------

    private static void setSplitDocType(@Nonnull UpdateOp op,
                                        @Nonnull SplitDocType type) {
        checkNotNull(op).set(SD_TYPE, type.type);
    }

    private static void setSplitDocMaxRev(@Nonnull UpdateOp op,
                                          @Nonnull Revision maxRev) {
        checkNotNull(op).set(SD_MAX_REV_TIME_IN_SECS, Commit.getModifiedInSecs(maxRev.getTimestamp()));
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
        String commitRootPath = getCommitRootPath(rev);
        if (commitRootPath == null) {
            // may happen for a commit root document, which hasn't been
            // updated with the commit revision yet
            return null;
        }
        // get root of commit
        return store.find(Collection.NODES, Utils.getIdFromPath(commitRootPath));
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
     * Set various split document related flag/properties
     *
     * @param mainDoc main document from which split document is being created
     * @param old updateOp of the old document created via split
     * @param oldDoc old document created via split
     * @param maxRev max revision stored in the split document oldDoc
     */
    private static void setSplitDocProps(NodeDocument mainDoc, NodeDocument oldDoc,
                                         UpdateOp old, Revision maxRev) {
        setSplitDocMaxRev(old, maxRev);

        SplitDocType type = SplitDocType.DEFAULT;
        if(!mainDoc.hasChildren() && !referencesOldDocAfterSplit(mainDoc, oldDoc)){
            type = SplitDocType.DEFAULT_NO_CHILD;
        } else if (oldDoc.getLocalRevisions().isEmpty()){
            type = SplitDocType.PROP_COMMIT_ONLY;
        }

        //Copy over the hasBinary flag
        if(mainDoc.hasBinary()){
            setHasBinary(old);
        }

        setSplitDocType(old,type);
    }

    /**
     * Checks if the main document has changes referencing {@code oldDoc} after
     * the split.
     *
     * @param mainDoc the main document before the split.
     * @param oldDoc  the old document created by the split.
     * @return {@code true} if the main document contains references to the
     *         old document after the split; {@code false} otherwise.
     */
    private static boolean referencesOldDocAfterSplit(NodeDocument mainDoc,
                                                      NodeDocument oldDoc) {
        Set<Revision> revs = oldDoc.getLocalRevisions().keySet();
        for (String property : mainDoc.data.keySet()) {
            if (IGNORE_ON_SPLIT.contains(property)) {
                continue;
            }
            Set<Revision> changes = Sets.newHashSet(mainDoc.getLocalMap(property).keySet());
            changes.removeAll(oldDoc.getLocalMap(property).keySet());
            if (!disjoint(changes, revs)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Set various properties for intermediate split document
     *
     * @param intermediate updateOp of the intermediate doc getting created
     * @param maxRev max revision stored in the intermediate
     */
    private static void setIntermediateDocProps(UpdateOp intermediate, Revision maxRev) {
        setSplitDocMaxRev(intermediate, maxRev);
        setSplitDocType(intermediate,SplitDocType.INTERMEDIATE);
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
        if (revision.equalsIgnoreBranch(readRevision)) {
            return true;
        }
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
     * and smaller or equal the readRevision revision.
     *
     * @param valueMap the sorted revision-value map
     * @param min the minimum revision (null meaning unlimited)
     * @param readRevision the maximum revision
     * @param validRevisions map of revision to commit value considered valid
     *                       against the given readRevision.
     * @return the value, or null if not found
     */
    @CheckForNull
    private Value getLatestValue(@Nonnull RevisionContext context,
                                 @Nonnull Map<Revision, String> valueMap,
                                 @Nullable Revision min,
                                 @Nonnull Revision readRevision,
                                 @Nonnull Map<Revision, String> validRevisions) {
        String value = null;
        Revision latestRev = null;
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
                latestRev = Utils.resolveCommitRevision(propRev, commitValue);
                value = entry.getValue();
                break;
            }
        }
        return value != null ? new Value(value, latestRev) : null;
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

    /**
     * The list of children for a node. The list might be complete or not, in
     * which case it only represents a block of children.
     */
    static final class Children implements CacheValue, Cloneable {

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
    }

    /**
     * A property value / revision combination.
     */
    private static final class Value {

        final String value;
        final Revision revision;

        Value(@Nonnull String value, @Nonnull Revision revision) {
            this.value = checkNotNull(value);
            this.revision = checkNotNull(revision);
        }
    }
}
