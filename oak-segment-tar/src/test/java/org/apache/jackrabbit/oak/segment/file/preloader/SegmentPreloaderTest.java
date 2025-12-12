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
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.preloader.SegmentPreloader.DispatchTask;
import org.apache.jackrabbit.oak.segment.file.preloader.SegmentPreloader.PreloadTask;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCachePreloadingConfiguration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SegmentPreloaderTest {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentPreloaderTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void testDecorationSkippedForWrongArguments() {
        Supplier<TarFiles> tarFiles = () -> null; // never called
        PersistentCache delegate = new MemoryTestCache();
        PersistentCache decorated = SegmentPreloader.decorate(delegate, PersistentCachePreloadingConfiguration.withConcurrency(0), tarFiles);
        assertSame(delegate, decorated);
    }

    @Test
    public void viaFileStoreBuilder() throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot())
                .build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();

            generateContent(builder, 4, 4);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        MemoryTestCache persistentCache = new MemoryTestCache();
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot())
                .withPersistentCache(persistentCache)
                .withPersistentCachePreloading(PersistentCachePreloadingConfiguration.withConcurrency(4).withMaxPreloadDepth(1))
                .build()) {
            SegmentId root = fileStore.getRevisions().getPersistedHead().getSegmentId();
            Segment segment = root.getSegment();

            int expectedCacheSize = 1 + segment.getReferencedSegmentIdCount();
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(expectedCacheSize, persistentCache.segments.size()));

            assertTrue(persistentCache.containsSegment(root.getMostSignificantBits(), root.getLeastSignificantBits()));
        }
    }

    @Test
    public void testPreloading() throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        SegmentNodeStorePersistence persistence = new TarPersistence(folder.getRoot());
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot())
                .withMaxFileSize(4)
                .withMemoryMapping(false)
                .withCustomPersistence(persistence)
                .build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();

            generateContent(builder, 4, 8);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        MemoryTestCache underlyingCache = new MemoryTestCache();

        try (TarFiles tarFiles = createReadOnlyTarFiles(folder.getRoot(), persistence);
             SegmentPreloader preloadingCache = (SegmentPreloader)SegmentPreloader.decorate(underlyingCache,
                     PersistentCachePreloadingConfiguration.withConcurrency(8).withMaxPreloadDepth(2), () -> tarFiles);
             JournalFileReader journalFileReader = persistence.getJournalFile().openJournalReader()) {

            UUID root = getRootUUID(journalFileReader);

            assertTrue(tarFiles.getIndices().size() > 2);
            Map<UUID, Set<UUID>> graph = computeFullGraph(tarFiles);

            Set<UUID> referencedSegments = collectReferencedSegments(root, graph, 1);
            for (UUID segment : referencedSegments) {
                assertFalse(underlyingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
                assertFalse(preloadingCache.containsSegment(segment.getMostSignificantBits(), segment.getLeastSignificantBits()));
            }

            preloadingCache.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(),
                    () -> tarFiles.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits()));
            assertReferencedSegmentsLoaded(referencedSegments, underlyingCache, preloadingCache);

            UUID nextToLoad = null;
            Set<UUID> uuids = null;
            for (UUID referencedSegment : referencedSegments) {
                uuids = collectReferencedSegments(referencedSegment, graph, 2);
                uuids.removeIf(uuid -> underlyingCache.containsSegment(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                if (!uuids.isEmpty()) {
                    nextToLoad = referencedSegment;
                }
            }

            assertNotNull(nextToLoad);

            final UUID next = nextToLoad;
            preloadingCache.readSegment(next.getMostSignificantBits(), next.getLeastSignificantBits(),
                    () -> tarFiles.readSegment(next.getMostSignificantBits(), next.getLeastSignificantBits()));
            LOG.info("Next loaded segment: {}", next);
            assertReferencedSegmentsLoaded(uuids, underlyingCache, preloadingCache);
        }
    }

    @Test
    public void testDispatchTaskEquals() throws IOException {
        withSegmentPreloader(preloader -> {
            UUID uuid = UUID.randomUUID();
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();

            DispatchTask task1 = preloader.createDispatchTask(msb, lsb, 1);
            assertEquals(task1, task1);

            DispatchTask task2 = preloader.createDispatchTask(msb, lsb, 1);
            assertEquals(task1, task2);

            DispatchTask task3 = preloader.createDispatchTask(msb, lsb, 0);
            assertNotEquals(task1, task3);

            DispatchTask task4 = preloader.createDispatchTask(msb, lsb + 1, 1);
            assertNotEquals(task1, task4);

            DispatchTask task5 = preloader.createDispatchTask(msb + 1, lsb, 1);
            assertNotEquals(task1, task5);

            assertNotEquals(task1, new Object());
        });
    }

    @Test
    public void testDispatchTaskArgumentValidation() throws IOException {
        withSegmentPreloader(preloader -> {
            UUID uuid = UUID.randomUUID();
            assertThrows(IllegalArgumentException.class, () -> preloader.createDispatchTask(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), 3));
        });
    }

    @Test
    public void testDispatchTaskToString() throws IOException {
        withSegmentPreloader(preloader -> {
            UUID uuid = UUID.randomUUID();
            assertEquals(
                    "DispatchTask{segmentId=" + uuid + ", depth=1}",
                    preloader.createDispatchTask(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), 1).toString());
        });
    }

    @Test
    public void testDispatchTaskCompareTo() throws IOException {
        withSegmentPreloader(preloader -> {
            UUID uuid = UUID.randomUUID();
            DispatchTask task1 = preloader.createDispatchTask(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), 2);
            DispatchTask task2 = preloader.createDispatchTask(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), 1);
            DispatchTask task3 = preloader.createDispatchTask(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), 2);
            List<DispatchTask> tasks = new ArrayList<>();
            tasks.add(task1);
            tasks.add(task2);
            tasks.add(task3);
            Collections.sort(tasks);
            assertEquals(List.of(task2, task3, task1), tasks);
        });
    }

    @Test
    public void testPreloadTaskEquals() throws IOException {
        withSegmentPreloader(preloader -> {
            UUID uuid = UUID.randomUUID();
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();

            PreloadTask task1 = preloader.createPreloadTask(msb, lsb, 1);
            assertEquals(task1, task1);

            PreloadTask task2 = preloader.createPreloadTask(msb, lsb, 1);
            assertEquals(task1, task2);

            PreloadTask task3 = preloader.createPreloadTask(msb, lsb, 0);
            assertEquals(task1, task3); // depth is not considered for equality

            PreloadTask task4 = preloader.createPreloadTask(msb, lsb + 1, 1);
            assertNotEquals(task1, task4);

            PreloadTask task5 = preloader.createPreloadTask(msb + 1, lsb, 1);
            assertNotEquals(task1, task5);

            assertNotEquals(task1, new Object());

        });
    }

    @Test
    public void testPreloadTaskToString() throws IOException {
        withSegmentPreloader(preloader -> {
            UUID uuid = UUID.randomUUID();
            assertEquals("PreloadTask{segmentId=" + uuid + ", depth=1}",
                    preloader.createPreloadTask(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), 1).toString());
        });
    }

    private void withSegmentPreloader(Consumer<SegmentPreloader> withPreloader) throws IOException {
        MemoryTestCache cache = new MemoryTestCache();
        PersistentCachePreloadingConfiguration config =
                PersistentCachePreloadingConfiguration.withConcurrency(2).withMaxPreloadDepth(2);
        try (TarFiles tarFiles = createReadOnlyTarFiles(folder.getRoot(), new TarPersistence(folder.getRoot()));
             SegmentPreloader preloader = (SegmentPreloader) SegmentPreloader.decorate(cache, config, () -> tarFiles)) {
            withPreloader.accept(preloader);
        }
    }

    private static @NotNull UUID getRootUUID(JournalFileReader journalFileReader) throws IOException {
        String line = journalFileReader.readLine();
        String[] parts = line.split(":");
        return UUID.fromString(parts[0]);
    }

    private void assertReferencedSegmentsLoaded(Set<UUID> referencedSegments, MemoryTestCache underlyingCache, SegmentPreloader preloadingCache) {
        Set<UUID> segments = new HashSet<>(referencedSegments);
        int timeoutSec = 10;
        Awaitility
                .await()
                .atMost(timeoutSec, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    segments.removeIf(uuid ->
                            underlyingCache.containsSegment(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits())
                            && preloadingCache.containsSegment(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
                    assertEquals("Not all referenced segments have been preloaded within " + timeoutSec + " seconds",
                            Set.of(), segments);
                });


    }

    private static Map<UUID, Set<UUID>> computeFullGraph(TarFiles tarFiles) throws IOException {
        Map<UUID, Set<UUID>> fullGraph = new HashMap<>();
        for (String archiveName : tarFiles.getIndices().keySet()) {
            Map<UUID, Set<UUID>> graph = tarFiles.getGraph(archiveName);
            fullGraph.putAll(graph);
        }
        return fullGraph;
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

    private static Set<UUID> collectReferencedSegments(UUID root, Map<UUID, Set<UUID>> graph, int depth) {
        Set<UUID> uuids = new LinkedHashSet<>();
        uuids.add(root);
        if (depth > 0) {
            for (UUID edge : graph.get(root)) {
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

        @Override
        public @Nullable AbstractCacheStats getCacheStats() {
            return null;
        }
    }

}
