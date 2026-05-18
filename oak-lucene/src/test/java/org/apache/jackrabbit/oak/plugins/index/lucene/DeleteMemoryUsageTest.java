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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.lang.management.ManagementFactory;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference-only reproducer for OAK-12193. DISABLED in CI because it allocates
 * several GB, produces a multi-hundred-MB heap dump, and takes about a minute even
 * with SegmentNodeStore. Intended for manual before/after comparison when tuning
 * the filtered-delete logic in {@link FulltextIndexEditor#childNodeDeleted}; the
 * fast, CI-friendly behavioral coverage lives in {@link FilteredDeleteTest}.
 *
 * <p>How to run manually (remove the {@code @Ignore} or pass {@code -Dtest=DeleteMemoryUsageTest
 * -DexcludedGroups=}, then):
 * <pre>
 *   mvn test -pl oak-lucene -Dtest=DeleteMemoryUsageTest -Dtest.opts.memory="-Xmx2g"
 * </pre>
 *
 * <p>Setup: 10 Lucene indexes declaring {@code nt:file} are registered; a /content
 * tree with 2 levels x FAN_OUT children of type {@code nt:unstructured} is populated
 * (none of the content matches any index's declaringNodeTypes, so the indexes are
 * initialized but empty). Every grandchild is then deleted individually so the
 * editor sees each as its own top-level {@code childNodeDeleted}, and a final async
 * cycle is run. A peak-memory sampler triggers a heap dump at 70% of max heap.
 *
 * <p>Uses SegmentNodeStore on a temporary folder so populate does not hold content
 * in RAM; this leaves headroom to observe the delete-heavy cycle's contribution.
 *
 * <p>Expected on current trunk (bug present): delete-heavy cycle ~52 s, peak heap
 * ~1500 MB at {@code -Xmx2g}, {@code updates} counter ~64 M. With the OAK-12193 fix
 * enabled (default): ~7 s, peak heap ~950 MB, {@code updates} ~1500. No assertions;
 * the test logs the observed numbers for comparison.
 */
@Ignore("OAK-12193 reference-only reproducer; run manually. Uses several GB and writes a heap dump.")
public class DeleteMemoryUsageTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteMemoryUsageTest.class);

    /** Fan-out per level. Tree has 2 levels: total nodes under /content = FAN_OUT + FAN_OUT^2. */
    private static final int FAN_OUT = 2530;

    /** Number of Lucene indexes registered, each with a deliberately mismatched declaringNodeType. */
    private static final int NUM_INDEXES = 10;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private MemoryBlobStore blobStore;
    private Root root;
    private AsyncIndexUpdate asyncIndexUpdate;
    private NodeStore nodeStore;
    private FileStore fileStore;

    @Before
    public void before() throws Exception {
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
    }

    @After
    public void after() {
        if (fileStore != null) {
            fileStore.close();
        }
    }

    protected ContentRepository createRepository() throws Exception {
        blobStore = new MemoryBlobStore();
        blobStore.setBlockSizeMin(48);

        File segDir = temporaryFolder.newFolder("segment");
        fileStore = FileStoreBuilder.fileStoreBuilder(segDir)
                .withBlobStore(blobStore)
                .withSegmentCacheSize(64)
                .build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore,
                CompositeIndexEditorProvider.compose(
                        luceneIndexEditorProvider,
                        new NodeCounterEditorProvider()));
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(luceneIndexEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    @Test
    public void deleteHeavyAsyncCycleMemoryUsage() throws Exception {
        // Allow toggling via system property for manual OAK-12193 before/after verification.
        if (Boolean.getBoolean("oak.oak12193.disable")) {
            FulltextIndexEditor.FT_OAK_12193_DISABLE.set(true);
            log("OAK-12193 fix DISABLED for this run (legacy behavior)");
        } else {
            FulltextIndexEditor.FT_OAK_12193_DISABLE.set(false);
        }
        for (int i = 0; i < NUM_INDEXES; i++) {
            createIndex("idx" + i);
        }
        root.commit();
        log("Registered " + NUM_INDEXES + " Lucene indexes (declaringNodeTypes=nt:file, no data matches)");

        Tree content = root.getTree("/").addChild("content");
        content.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        root.commit();

        int totalNodes = 1;
        for (int i = 0; i < FAN_OUT; i++) {
            Tree child = root.getTree("/content").addChild("child" + i);
            child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            totalNodes++;
            for (int j = 0; j < FAN_OUT; j++) {
                Tree grandchild = child.addChild("child" + j);
                grandchild.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                totalNodes++;
            }
            root.commit();
            // Run async indexer periodically so MemoryNodeStore doesn't hold many revisions at once.
            if (i % 20 == 19) {
                asyncIndexUpdate.run();
            }
        }
        asyncIndexUpdate.run();
        log("Populated /content with " + totalNodes + " nodes (FAN_OUT=" + FAN_OUT + ")");
        logMemory("after populate + initial async cycles");

        root.refresh();
        // Delete each grandchild individually so the Lucene editor sees each as its own
        // top-level childNodeDeleted (triggering one deleteDocuments call per index per leaf).
        // Deleting the parent /content/childI as a subtree would only be one call per index.
        int deleted = 0;
        for (int i = 0; i < FAN_OUT; i++) {
            for (int j = 0; j < FAN_OUT; j++) {
                root.getTree("/content/child" + i + "/child" + j).remove();
                deleted++;
                if (deleted % 1000 == 0) {
                    root.commit();
                }
            }
        }
        root.commit();
        log("Deleted " + deleted + " grandchildren individually; running delete-heavy async cycle");

        logMemory("before delete-heavy async cycle");
        // Trigger a heap dump once memory exceeds this threshold during the cycle —
        // captures the state with buffered deletes still live (before writer close flushes them).
        long dumpThreshold = (long) (Runtime.getRuntime().maxMemory() * 0.7);
        File dumpFile = new File("target/DeleteMemoryUsageTest-peak.hprof");
        if (dumpFile.exists()) dumpFile.delete();
        PeakMemorySampler sampler = new PeakMemorySampler(dumpThreshold, dumpFile);
        sampler.start();
        long t0 = System.currentTimeMillis();
        asyncIndexUpdate.run();
        long elapsed = System.currentTimeMillis() - t0;
        sampler.stop();
        log("delete-heavy cycle: elapsed=" + elapsed + "ms, peak heap used=" + sampler.getPeakUsedMB() + "MB");
        log("async stats: " + asyncIndexUpdate.getIndexStats());
        logMemory("after delete-heavy async cycle");
        if (sampler.dumpWritten()) {
            log("peak heap dumped to " + dumpFile.getAbsolutePath());
        } else {
            log("threshold " + (dumpThreshold / 1024 / 1024) + "MB not reached; no dump written");
        }
        log("Delete-heavy async cycle completed without OOM.");
    }

    private static final class PeakMemorySampler {
        private volatile boolean running;
        private volatile long peakUsed;
        private volatile boolean dumpWritten;
        private Thread thread;
        private final long dumpThresholdBytes;
        private final File dumpFile;

        PeakMemorySampler() {
            this(-1, null);
        }

        PeakMemorySampler(long dumpThresholdBytes, File dumpFile) {
            this.dumpThresholdBytes = dumpThresholdBytes;
            this.dumpFile = dumpFile;
        }

        void start() {
            running = true;
            peakUsed = 0;
            dumpWritten = false;
            thread = new Thread(() -> {
                while (running) {
                    Runtime rt = Runtime.getRuntime();
                    long used = rt.totalMemory() - rt.freeMemory();
                    if (used > peakUsed) peakUsed = used;
                    if (!dumpWritten && dumpFile != null && used > dumpThresholdBytes) {
                        try {
                            HotSpotDiagnosticMXBean mx = ManagementFactory.newPlatformMXBeanProxy(
                                    ManagementFactory.getPlatformMBeanServer(),
                                    "com.sun.management:type=HotSpotDiagnostic",
                                    HotSpotDiagnosticMXBean.class);
                            mx.dumpHeap(dumpFile.getAbsolutePath(), true);
                            dumpWritten = true;
                        } catch (Exception e) {
                            // best-effort; give up
                            dumpWritten = true;
                        }
                    }
                    try { Thread.sleep(20); } catch (InterruptedException e) { return; }
                }
            }, "peak-memory-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            running = false;
            try { thread.join(5000); } catch (InterruptedException e) { /* ignore */ }
        }

        long getPeakUsedMB() {
            return peakUsed / (1024 * 1024);
        }

        boolean dumpWritten() {
            return dumpWritten;
        }
    }

    private void createIndex(String name) throws Exception {
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.async("async");
        idxb.includedPaths("/content");
        idxb.indexRule("nt:file")
                .property("jcr:title").propertyIndex();
        idxb.build(root.getTree("/oak:index").addChild(name));
    }

    private static void logMemory(String phase) {
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        log(String.format("heap [%s]: used=%dMB total=%dMB max=%dMB",
                phase,
                used / (1024 * 1024),
                rt.totalMemory() / (1024 * 1024),
                rt.maxMemory() / (1024 * 1024)));
    }

    private static void log(String msg) {
        System.out.println("[DeleteMemoryUsageTest] " + msg);
        LOG.info(msg);
    }
}
