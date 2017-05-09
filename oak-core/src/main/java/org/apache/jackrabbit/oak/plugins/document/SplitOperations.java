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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.filter;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COMMIT_ROOT;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.DOC_SIZE_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.REVISIONS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isCommitRootEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isRevisionsEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.removePrevious;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setHasBinary;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setPrevious;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PROPERTY_OR_DELETED;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getPreviousIdFor;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;

/**
 * Utility class to create document split operations.
 */
class SplitOperations {

    private static final Logger LOG = LoggerFactory.getLogger(SplitOperations.class);
    private static final int GARBAGE_LIMIT = Integer.getInteger("oak.documentMK.garbage.limit", 1000);
    private static final Predicate<Long> BINARY_FOR_SPLIT_THRESHOLD = new Predicate<Long>() {
        @Override
        public boolean apply(Long input) {
            // only force trigger split for binaries bigger than 4k
            return input > 4096;
        }
    };
    private static final DocumentStore STORE = new MemoryDocumentStore();

    private final NodeDocument doc;
    private final String path;
    private final String id;
    private final Revision headRevision;
    private final RevisionContext context;
    private final Function<String, Long> binarySize;
    private final int numRevsThreshold;
    private Revision high;
    private Revision low;
    private int numValues;
    private boolean hasBinaryToSplit;
    private Supplier<Boolean> nodeExistsAtHeadRevision;
    private Map<String, NavigableMap<Revision, String>> committedChanges;
    private Set<Revision> changes;
    private Map<String, Set<Revision>> garbage;
    private int garbageCount = 0;
    private Set<Revision> mostRecentRevs;
    private Set<Revision> splitRevs;
    private List<UpdateOp> splitOps;
    private UpdateOp main;

    private SplitOperations(@Nonnull final NodeDocument doc,
                            @Nonnull final RevisionContext context,
                            @Nonnull final RevisionVector headRev,
                            @Nonnull final Function<String, Long> binarySize,
                            int numRevsThreshold) {
        this.doc = checkNotNull(doc);
        this.context = checkNotNull(context);
        this.binarySize = checkNotNull(binarySize);
        this.path = doc.getPath();
        this.id = doc.getId();
        this.headRevision = checkNotNull(headRev).getRevision(context.getClusterId());
        this.numRevsThreshold = numRevsThreshold;
        this.nodeExistsAtHeadRevision = Suppliers.memoize(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                return doc.getLiveRevision(context, headRev,
                        Maps.<Revision, String>newHashMap(),
                        new LastRevs(headRev)) != null;
            }
        });
    }

    /**
     * Creates a list of update operations in case the given document requires
     * a split. A caller must explicitly pass a head revision even though it
     * is available through the {@link RevisionContext}. The given head revision
     * must reflect a head state before {@code doc} was retrieved from the
     * document store. This is important in order to maintain consistency.
     * See OAK-3081 for details.
     *
     * @param doc a main document.
     * @param context the revision context.
     * @param headRevision the head revision before the document was retrieved
     *                     from the document store.
     * @param binarySize a function that returns the binary size of the given
     *                   JSON property value String.
     * @param numRevsThreshold only split off at least this number of revisions.
     * @return list of update operations. An empty list indicates the document
     *          does not require a split.
     * @throws IllegalArgumentException if the given document is a split
     *                                  document.
     */
    @Nonnull
    static List<UpdateOp> forDocument(@Nonnull NodeDocument doc,
                                      @Nonnull RevisionContext context,
                                      @Nonnull RevisionVector headRevision,
                                      @Nonnull Function<String, Long> binarySize,
                                      int numRevsThreshold) {
        if (doc.isSplitDocument()) {
            throw new IllegalArgumentException(
                    "Not a main document: " + doc.getId());
        }
        return new SplitOperations(doc, context, headRevision,
                binarySize, numRevsThreshold).create();

    }

    private List<UpdateOp> create() {
        if (!considerSplit()) {
            return Collections.emptyList();
        }
        splitOps = Lists.newArrayList();
        mostRecentRevs = Sets.newHashSet();
        splitRevs = Sets.newHashSet();
        garbage = Maps.newHashMap();
        changes = Sets.newHashSet();
        committedChanges = Maps.newHashMap();
        
        collectLocalChanges(committedChanges, changes);

        // revisions of the most recent committed changes on this document
        // these are kept in the main document. _revisions and _commitRoot
        // entries with these revisions are retained in the main document
        populateSplitRevs();

        // collect _revisions and _commitRoot entries for split document
        collectRevisionsAndCommitRoot();

        // create split ops out of the split values
        main = createSplitOps();

        // create intermediate docs if needed
        createIntermediateDocs();

        // remove stale references to previous docs
        disconnectStalePrevDocs();
        
        // remove garbage
        removeGarbage();

        // main document must be updated last
        if (main != null) {
            splitOps.add(main);
        }

        return splitOps;
    }

    private boolean considerSplit() {
        SortedMap<Revision, Range> previous = doc.getPreviousRanges();
        // only consider if there are enough commits,
        // unless document is really big
        return doc.getLocalRevisions().size() + doc.getLocalCommitRoot().size() > numRevsThreshold
                || doc.getMemory() >= DOC_SIZE_THRESHOLD
                || previous.size() >= PREV_SPLIT_FACTOR
                || !doc.getStalePrev().isEmpty()
                || doc.hasBinary();
    }

    /**
     * Populate the {@link #splitRevs} with the revisions of the committed
     * changes that will be moved to a previous document. For each property,
     * all but the most recent change will be moved.
     */
    private void populateSplitRevs() {
        for (NavigableMap<Revision, String> splitMap : committedChanges.values()) {
            // keep the most recent changes in the main document
            if (!splitMap.isEmpty()) {
                Revision r = splitMap.lastKey();
                splitMap.remove(r);
                splitRevs.addAll(splitMap.keySet());
                hasBinaryToSplit |= hasBinaryPropertyForSplit(splitMap.values())
                        && nodeExistsAtHeadRevision.get();
                mostRecentRevs.add(r);
            }
            if (splitMap.isEmpty()) {
                continue;
            }
            // remember highest / lowest revision
            trackHigh(splitMap.lastKey());
            trackLow(splitMap.firstKey());
            numValues += splitMap.size();
        }
    }

    private boolean hasBinaryPropertyForSplit(Iterable<String> values) {
        return doc.hasBinary() && any(transform(values, binarySize), BINARY_FOR_SPLIT_THRESHOLD);
    }

    /**
     * Collect _revisions and _commitRoot entries that can be moved to a
     * previous document.
     */
    private void collectRevisionsAndCommitRoot() {
        NavigableMap<Revision, String> revisions =
                new TreeMap<Revision, String>(StableRevisionComparator.INSTANCE);
        for (Map.Entry<Revision, String> entry : doc.getLocalRevisions().entrySet()) {
            if (splitRevs.contains(entry.getKey())) {
                revisions.put(entry.getKey(), entry.getValue());
                numValues++;
            } else {
                // move _revisions entries that act as commit root without
                // local changes
                if (context.getClusterId() != entry.getKey().getClusterId()) {
                    // only consider local changes
                    continue;
                }
                if (isCommitted(context.getCommitValue(entry.getKey(), doc))
                        && !mostRecentRevs.contains(entry.getKey())) {
                    // this is a commit root for changes in other documents
                    revisions.put(entry.getKey(), entry.getValue());
                    numValues++;
                    trackHigh(entry.getKey());
                    trackLow(entry.getKey());
                }
            }
        }
        committedChanges.put(REVISIONS, revisions);
        NavigableMap<Revision, String> commitRoot =
                new TreeMap<Revision, String>(StableRevisionComparator.INSTANCE);
        boolean mostRecent = true;
        for (Map.Entry<Revision, String> entry : doc.getLocalCommitRoot().entrySet()) {
            Revision r = entry.getKey();
            if (splitRevs.contains(r)) {
                commitRoot.put(r, entry.getValue());
                numValues++;
            } else if (r.getClusterId() == context.getClusterId() 
                    && !changes.contains(r)) {
                // OAK-2528: _commitRoot entry without associated change
                // consider all but most recent as garbage (OAK-3333, OAK-4050)
                if (mostRecent && isCommitted(context.getCommitValue(r, doc))) {
                    mostRecent = false;
                } else if (isGarbage(r)) {
                    addGarbage(r, COMMIT_ROOT);
                }
            }
        }
        committedChanges.put(COMMIT_ROOT, commitRoot);
    }

    /**
     * Creates {@link UpdateOp}s for intermediate documents if necessary.
     */
    private void createIntermediateDocs() {
        // collect ranges and create a histogram of the height
        Map<Integer, List<Range>> prevHisto = getPreviousDocsHistogram();
        // check if we need to create intermediate previous documents
        for (Map.Entry<Integer, List<Range>> entry : prevHisto.entrySet()) {
            if (entry.getValue().size() >= PREV_SPLIT_FACTOR) {
                if (main == null) {
                    main = new UpdateOp(id, false);
                }
                // calculate range
                Revision h = null;
                Revision l = null;
                for (Range r : entry.getValue()) {
                    if (h == null || r.high.compareRevisionTime(h) > 0) {
                        h = r.high;
                    }
                    if (l == null || l.compareRevisionTime(r.low) > 0) {
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
                if (Utils.isLongPath(prevPath)) {
                    intermediate.set(NodeDocument.PATH, prevPath);
                }
                setPrevious(main, new Range(h, l, entry.getKey() + 1));
                for (Range r : entry.getValue()) {
                    setPrevious(intermediate, r);
                }
                setIntermediateDocProps(intermediate, h);
                splitOps.add(intermediate);
            }
        }
    }

    /**
     * Creates split {@link UpdateOp} if there is enough data to split off. The
     * {@link UpdateOp} for the new previous document is placed into the list of
     * {@link #splitOps}. The {@link UpdateOp} for the main document is not
     * added to the list but rather returned.
     *
     * @return the UpdateOp for the main document or {@code null} if there is
     *          not enough data to split.
     */
    @CheckForNull
    private UpdateOp createSplitOps() {
        UpdateOp main = null;
        // check if we have enough data to split off
        if (high != null && low != null
                && (numValues >= numRevsThreshold
                || doc.getMemory() > DOC_SIZE_THRESHOLD
                || hasBinaryToSplit)) {
            // enough changes to split off
            // move to another document
            main = new UpdateOp(id, false);
            setPrevious(main, new Range(high, low, 0));
            String oldPath = Utils.getPreviousPathFor(path, high, 0);
            UpdateOp old = new UpdateOp(Utils.getIdFromPath(oldPath), true);
            if (Utils.isLongPath(oldPath)) {
                old.set(NodeDocument.PATH, oldPath);
            }
            for (String property : committedChanges.keySet()) {
                NavigableMap<Revision, String> splitMap = committedChanges.get(property);
                for (Map.Entry<Revision, String> entry : splitMap.entrySet()) {
                    Revision r = entry.getKey();
                    if (isRevisionsEntry(property) || isCommitRootEntry(property)) {
                        // only remove from main document if it is not
                        // referenced anymore from from most recent changes
                        if (!mostRecentRevs.contains(r)) {
                            main.removeMapEntry(property, r);
                            NodeDocument.removeBranchCommit(main, r);
                        }
                    } else {
                        main.removeMapEntry(property, r);
                    }
                    old.setMapEntry(property, r, entry.getValue());
                    if (doc.getLocalBranchCommits().contains(r)) {
                        NodeDocument.setBranchCommit(old, r);
                    }
                }
            }
            // check size of old document
            NodeDocument oldDoc = new NodeDocument(STORE);
            UpdateUtils.applyChanges(oldDoc, old);
            setSplitDocProps(doc, oldDoc, old, high);
            splitOps.add(old);

            if (numValues < numRevsThreshold) {
                String reason;
                if (hasBinaryToSplit) {
                    reason = "binary";
                } else {
                    reason = "size";
                }
                LOG.debug("Force splitting {} ({})", id, reason);
            }
        }
        return main;
    }

    /**
     * Returns a histogram of the height of the previous documents referenced
     * by this document. This only includes direct references and not indirectly
     * referenced previous documents through intermediate previous docs.
     *
     * @return histogram of the height of the previous documents.
     */
    private Map<Integer, List<Range>> getPreviousDocsHistogram() {
        Map<Integer, List<Range>> prevHisto = Maps.newHashMap();
        for (Map.Entry<Revision, Range> entry : doc.getPreviousRanges().entrySet()) {
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
        return prevHisto;
    }

    /**
     * Collects all local property changes committed by the current
     * cluster node.
     *
     * @param committedLocally local changes committed by the current cluster node.
     * @param changes all revisions of local changes (committed and uncommitted).
     */
    private void collectLocalChanges(
            Map<String, NavigableMap<Revision, String>> committedLocally,
            Set<Revision> changes) {
        for (String property : filter(doc.keySet(), PROPERTY_OR_DELETED)) {
            NavigableMap<Revision, String> splitMap
                    = new TreeMap<Revision, String>(StableRevisionComparator.INSTANCE);
            committedLocally.put(property, splitMap);
            Map<Revision, String> valueMap = doc.getLocalMap(property);
            // collect committed changes of this cluster node
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();
                if (rev.getClusterId() != context.getClusterId()) {
                    continue;
                }
                changes.add(rev);
                if (isCommitted(context.getCommitValue(rev, doc))) {
                    splitMap.put(rev, entry.getValue());
                } else if (isGarbage(rev)) {
                    addGarbage(rev, property);
                }
            }
        }
    }
    
    private boolean isGarbage(Revision rev) {
        // use headRevision as passed in the constructor instead
        // of the head revision from the RevisionContext. see OAK-3081
        if (headRevision.compareRevisionTime(rev) <= 0) {
            // this may be an in-progress commit
            return false;
        }
        // garbage if not part of an active branch
        return context.getBranches().getBranchCommit(rev) == null;
    }
    
    private void addGarbage(Revision rev, String property) {
        if (garbageCount > GARBAGE_LIMIT) {
            return;
        }
        Set<Revision> revisions = garbage.get(property);
        if (revisions == null) {
            revisions = Sets.newHashSet();
            garbage.put(property, revisions);
        }
        if (revisions.add(rev)) {
            garbageCount++;
        }
    }

    private void disconnectStalePrevDocs() {
        NavigableMap<Revision, Range> ranges = doc.getPreviousRanges(true);
        for (Map.Entry<Revision, String> entry : doc.getStalePrev().entrySet()) {
            Revision r = entry.getKey();
            if (r.getClusterId() != context.getClusterId()) {
                // only process revisions of this cluster node
                continue;
            }
            if (main == null) {
                main = new UpdateOp(id, false);
            }
            NodeDocument.removeStalePrevious(main, r);

            if (ranges.containsKey(r)
                    && entry.getValue().equals(String.valueOf(ranges.get(r).height))) {
                NodeDocument.removePrevious(main, r);
            } else {
                // reference was moved to an intermediate doc
                // while the last GC was running
                // -> need to locate intermediate doc and disconnect from there
                int height = Integer.parseInt(entry.getValue());
                NodeDocument intermediate = doc.findPrevReferencingDoc(r, height);
                if (intermediate == null) {
                    LOG.warn("Split document {} not referenced anymore. Main document is {}",
                            getPreviousIdFor(doc.getPath(), r, height), id);
                } else {
                    UpdateOp op = new UpdateOp(intermediate.getId(), false);
                    NodeDocument.removePrevious(op, r);
                    splitOps.add(op);
                }
            }

        }
    }
    
    private void removeGarbage() {
        if (garbage.isEmpty()) {
            return;
        } else if (main == null) {
            main = new UpdateOp(id, false);
        }
        for (Map.Entry<String, Set<Revision>> entry : garbage.entrySet()) {
            for (Revision r : entry.getValue()) {
                main.removeMapEntry(entry.getKey(), r);
                if (PROPERTY_OR_DELETED.apply(entry.getKey())) {
                    NodeDocument.removeCommitRoot(main, r);
                    NodeDocument.removeRevision(main, r);
                    NodeDocument.removeBranchCommit(main, r);
                }
            }
        }
    }

    private void trackHigh(Revision r) {
        if (high == null || r.compareRevisionTime(high) > 0) {
            high = r;
        }
    }

    private void trackLow(Revision r) {
        if (low == null || low.compareRevisionTime(r) > 0) {
            low = r;
        }
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
        if (!mainDoc.hasChildren()) {
            type = SplitDocType.DEFAULT_LEAF;
        } else if (oldDoc.getLocalRevisions().isEmpty()) {
            type = SplitDocType.COMMIT_ROOT_ONLY;
        } else if (oldDoc.getLocalBranchCommits().isEmpty()) {
            type = SplitDocType.DEFAULT_NO_BRANCH;
        }

        // Copy over the hasBinary flag
        if (mainDoc.hasBinary()) {
            setHasBinary(old);
        }

        setSplitDocType(old, type);
    }

    /**
     * Set various properties for intermediate split document
     *
     * @param intermediate updateOp of the intermediate doc getting created
     * @param maxRev max revision stored in the intermediate
     */
    private static void setIntermediateDocProps(UpdateOp intermediate, Revision maxRev) {
        setSplitDocMaxRev(intermediate, maxRev);
        setSplitDocType(intermediate, SplitDocType.INTERMEDIATE);
    }

    //----------------------------< internal modifiers >------------------------

    private static void setSplitDocType(@Nonnull UpdateOp op,
                                        @Nonnull SplitDocType type) {
        checkNotNull(op).set(NodeDocument.SD_TYPE, type.type);
    }

    private static void setSplitDocMaxRev(@Nonnull UpdateOp op,
                                          @Nonnull Revision maxRev) {
        checkNotNull(op).set(NodeDocument.SD_MAX_REV_TIME_IN_SECS, NodeDocument.getModifiedInSecs(maxRev.getTimestamp()));
    }

}
