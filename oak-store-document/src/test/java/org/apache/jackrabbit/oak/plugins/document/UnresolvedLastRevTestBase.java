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

import java.util.SortedMap;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for tests where the most recent change of a cluster node on a
 * document cannot be resolved by another cluster node (OAK-12557).
 * <p>
 * The scenario mirrors a Lucene index {@code :data} node:
 * <ul>
 *     <li>the writer (clusterId 1) is the async indexer. Every commit updates
 *     the {@code dirListing} property of {@code :data} and {@code /:async},
 *     hence the commit root is the root document and {@code :data} never
 *     gets a {@code _lastRev} entry from the writer,</li>
 *     <li>the reader (clusterId 2) is another cluster node, e.g. the one
 *     running FullGC,</li>
 *     <li>the writer adds a segment file node, keeps updating {@code :data},
 *     the reader catches up, the writer commits once more and right after
 *     splits {@code :data}. The split moves all but the most recent change
 *     of the writer to a previous document,</li>
 *     <li>the reader has not yet seen the root document with the commit
 *     entry of the most recent change.</li>
 * </ul>
 */
public abstract class UnresolvedLastRevTestBase {

    static final String DATA = "/oak:index/lucene/:data";
    static final String SEGMENT = DATA + "/_292j4.cfe";
    static final String DIR_LISTING = "dirListing";
    static final String DATA_ID = Utils.getIdFromPath(DATA);
    static final String SEGMENT_ID = Utils.getIdFromPath(SEGMENT);

    private static final int WRITER_UPDATES = 10;
    private static final int SPLIT_THRESHOLD = 5;

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    protected Clock clock;
    protected MemoryDocumentStore store;
    protected StaleCommitRootDocumentStore readerStore;
    protected DocumentNodeStore writer;
    protected DocumentNodeStore reader;

    private int listingCounter;

    @Before
    public void setUpClock() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
    }

    @After
    public void resetClock() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    void createNodeStores(Feature readerLegacyFeature) {
        store = new MemoryDocumentStore();
        readerStore = new StaleCommitRootDocumentStore(store);
        writer = newBuilder(1).setDocumentStore(store).getNodeStore();
        reader = newBuilder(2).setDocumentStore(readerStore)
                .setLegacyUnresolvedLastRevFeature(readerLegacyFeature).getNodeStore();
    }

    private DocumentMK.Builder newBuilder(int clusterId) {
        return builderProvider.newBuilder().clock(clock).setClusterId(clusterId)
                .setAsyncDelay(0).setLeaseCheckMode(LeaseCheckMode.DISABLED);
    }

    /**
     * Creates {@code :data} with the given node store, then lets the writer
     * add {@link #SEGMENT} and update {@code :data} a couple of times. The
     * reader has seen all these changes when this method returns.
     */
    void prepare(DocumentNodeStore creator) throws Exception {
        prepare(creator, b -> {});
    }

    void prepare(DocumentNodeStore creator, Consumer<NodeBuilder> segmentCommit) throws Exception {
        commit(creator, b -> child(b, DATA).setProperty(DIR_LISTING, "[]"));
        syncAll();
        writerCommit(b -> {
            child(b, SEGMENT).setProperty("blobSize", 1024L);
            segmentCommit.accept(b);
        });
        clock.waitUntil(clock.getTime() + 60_000);
        for (int i = 0; i < WRITER_UPDATES; i++) {
            writerCommit(b -> {});
        }
        syncAll();
    }

    /**
     * The writer commits one more change to {@code :data}, which the reader
     * does not see yet, and splits {@code :data} right after.
     *
     * @return the revision of the change not yet seen by the reader.
     */
    Revision commitAndSplitUnseenByReader() throws Exception {
        readerStore.freezeCommitRoot();
        writerCommit(b -> {});
        NodeDocument doc = store.find(NODES, DATA_ID);
        Revision unseen = doc.getLocalMap(DIR_LISTING).firstKey();
        for (UpdateOp op : SplitOperations.forDocument(doc, writer,
                writer.getHeadRevision(), s -> 0L, SPLIT_THRESHOLD)) {
            store.createOrUpdate(NODES, op);
        }
        doc = store.find(NODES, DATA_ID);
        // precondition: the only local dirListing change of the writer is
        // the one the reader cannot resolve yet
        SortedMap<Revision, String> listing = doc.getLocalMap(DIR_LISTING);
        assertEquals(unseen, listing.firstKey());
        assertEquals(1, listing.keySet().stream()
                .filter(r -> r.getClusterId() == writer.getClusterId()).count());
        assertFalse(doc.getPreviousRanges().isEmpty());
        NodeDocument staleRoot = readerStore.find(NODES, Utils.getIdFromPath(PathUtils.ROOT_PATH));
        assertNull(staleRoot.getLocalRevisions().get(unseen));
        assertTrue(Utils.isCommitted(writer.getCommitValue(unseen, doc)));
        return unseen;
    }

    /**
     * The reader catches up with the writer.
     */
    void readerCatchesUp() {
        readerStore.unfreezeCommitRoot();
        syncAll();
    }

    void syncAll() {
        writer.runBackgroundOperations();
        reader.runBackgroundOperations();
        writer.runBackgroundOperations();
        reader.runBackgroundOperations();
    }

    void writerCommit(Consumer<NodeBuilder> changes) throws Exception {
        int n = ++listingCounter;
        commit(writer, b -> {
            changes.accept(b);
            child(b, DATA).setProperty(DIR_LISTING, "[" + n + "]");
            b.child(":async").setProperty("lucene", "r" + n);
        });
    }

    static void commit(DocumentNodeStore ns, Consumer<NodeBuilder> changes) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        changes.accept(builder);
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    static NodeBuilder child(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }
        return builder;
    }

    /**
     * Reads the node at the given path with the reader, without any node
     * states cached from previous reads.
     */
    NodeState readerState(String path) {
        reader.getNodeCache().invalidateAll();
        reader.getNodeChildrenCache().invalidateAll();
        NodeState state = reader.getRoot();
        for (String name : PathUtils.elements(path)) {
            state = state.getChildNode(name);
        }
        return state;
    }
}
