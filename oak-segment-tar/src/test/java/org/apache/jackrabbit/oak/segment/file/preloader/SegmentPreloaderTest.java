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
package org.apache.jackrabbit.oak.segment.file.preloader;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentGraph;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCachePreloadingConfiguration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SegmentPreloaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void testDecorationSkippedForWrongArguments() throws IOException {
        Supplier<TarFiles> tarFiles = () -> null; // never called
        PersistentCache delegate = new MemoryTestCache();
        PersistentCache decorated = SegmentPreloader.decorate(delegate, PersistentCachePreloadingConfiguration.withConcurrency(0), tarFiles);
        assertSame(delegate, decorated);
    }

    @Test
    public void testPreloading() throws IOException, InvalidFileStoreVersionException, CommitFailedException, InterruptedException {
        SegmentNodeStorePersistence persistence = new TarPersistence(folder.getRoot());
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot())
                .withCustomPersistence(persistence)
                .build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();

            generateContent(builder, 4, 8);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        MemoryTestCache underlyingCache = new MemoryTestCache();
        TarFiles tarFiles = createReadOnlyTarFiles(folder.getRoot(), persistence);

        SegmentPreloader preloadingCache = (SegmentPreloader)SegmentPreloader.decorate(underlyingCache,
                PersistentCachePreloadingConfiguration.withConcurrency(8).withPrefetchDepth(2), () -> tarFiles);

        SegmentArchiveManager archiveManager = persistence.createArchiveManager(false, false, null, null, null);
        assertEquals(List.of("data00000a.tar"), archiveManager.listArchives());
        try (@Nullable SegmentArchiveReader reader = archiveManager.open("data00000a.tar");
             JournalFileReader journalFileReader = persistence.getJournalFile().openJournalReader()) {
            assertNotNull(reader);

            String line = journalFileReader.readLine();
            String[] parts = line.split(":");
            UUID root = UUID.fromString(parts[0]);

            SegmentGraph graph = reader.getGraph();
            Set<UUID> referencedSegments = collectReferencedSegments(root, graph, 2);
            for (UUID segment : referencedSegments) {
                assertFalse(underlyingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
                assertFalse(preloadingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
            }

            preloadingCache.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(),
                    () -> tarFiles.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits()));

            // wait for preloading to complete
            while (preloadingCache.hasInProgressTasks()) {
                TimeUnit.MILLISECONDS.sleep(50);
            }

            for (UUID segment : referencedSegments) {
                assertTrue("Segment missing in underlying cache: " + segment,
                        underlyingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
                assertTrue("Segment missing in preloading cache: " + segment,
                        preloadingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
            }
            assertEquals(referencedSegments.size(), underlyingCache.segments.size());

            UUID nextToLoad = null;
            Set<UUID> uuids = null;
            for (UUID referencedSegment : referencedSegments) {
                uuids = collectReferencedSegments(referencedSegment, graph, 2);
                uuids.removeAll(referencedSegments);
                if (!uuids.isEmpty()) {
                    nextToLoad = referencedSegment;
                }
            }

            assertNotNull(uuids);
            assertNotNull(nextToLoad);

            final UUID next = nextToLoad;
            preloadingCache.readSegment(next.getMostSignificantBits(), next.getLeastSignificantBits(),
                    () -> tarFiles.readSegment(next.getMostSignificantBits(), next.getLeastSignificantBits()));

            // wait for preloading to complete
            while (preloadingCache.hasInProgressTasks()) {
                TimeUnit.MILLISECONDS.sleep(50);
            }

            preloadingCache.close();

            for (UUID segment : uuids) {
                assertTrue("Segment missing in underlying cache: " + segment,
                        underlyingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
                assertTrue("Segment missing in preloading cache: " + segment,
                        preloadingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
            }
        }
    }

    private TarFiles createReadOnlyTarFiles(File directory, SegmentNodeStorePersistence persistence) throws IOException {
        return TarFiles.builder()
                .withDirectory(directory)
                .withPersistence(persistence)
                .withReadOnly()
                .withIOMonitor(new IOMonitorAdapter())
                .withRemoteStoreMonitor(new RemoteStoreMonitorAdapter())
                .withTarRecovery((uuid, data, entryRecovery) -> {
                    throw new UnsupportedOperationException();
                })
                .build();
    }

    private static Set<UUID> collectReferencedSegments(UUID root, SegmentGraph graph, int depth) throws IOException {
        Set<UUID> uuids = new LinkedHashSet<>();
        uuids.add(root);
        if (depth > 0) {
            for (UUID edge : graph.getEdges(root)) {
                uuids.addAll(collectReferencedSegments(edge, graph, depth - 1));
            }
        }
        return uuids;
    }

    private void generateContent(NodeBuilder builder, int childNodes, int depth) {
        RandomUtils r = RandomUtils.insecure();
        RandomStringUtils random = RandomStringUtils.insecure();
        for (int i = 0; i < childNodes; i++) {
            NodeBuilder child = builder.child(random.nextAlphabetic(40, 60));
            child.setProperty("jcr:primaryType", random.nextAlphabetic(4));
            child.setProperty(random.nextAlphabetic(30, 40), r.randomBoolean() ? random.nextAlphabetic(100, 150) : r.randomLong());
            if (depth > 1) {
                generateContent(child, childNodes, depth - 1);
            }
        }
    }

    private static class MemoryTestCache implements PersistentCache {

        Map<Long, Buffer> segments = new ConcurrentHashMap<>();

        @Override
        public @Nullable Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) {
            return segments.computeIfAbsent(lsb, i -> {
                try {
                    return loader.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public boolean containsSegment(long msb, long lsb) {
            return segments.containsKey(lsb);
        }

        @Override
        public void writeSegment(long msb, long lsb, Buffer buffer) {
            segments.put(lsb, buffer);
        }

        @Override
        public void cleanUp() {
            segments.clear();
        }
    }

}
