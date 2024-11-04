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
package org.apache.jackrabbit.oak.segment.file.tar;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndex;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TarReader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TarReader.class);

    static TarReader open(String file, SegmentArchiveManager archiveManager) throws IOException {
        TarReader reader = openFirstFileWithValidIndex(singletonList(file), archiveManager);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open tar file " + file);
        }
    }

    /**
     * Creates a {@link TarReader} instance for reading content from a tar file.
     * If there exist multiple generations of the same tar file, they are all
     * passed to this method. The latest generation with a valid tar index
     * (which is a good indication of general validity of the file) is opened
     * and the other generations are removed to clean things up. If none of the
     * generations has a valid index, then something must have gone wrong and
     * we'll try recover as much content as we can from the existing tar
     * generations.
     *
     * @param files         The generations of the same TAR file.
     * @param recovery      Strategy for recovering a damaged TAR file.
     * @return An instance of {@link TarReader}.
     */
    static TarReader open(Map<Character, String> files, TarRecovery recovery, SegmentArchiveManager archiveManager) throws IOException {
        SortedMap<Character, String> sorted = new TreeMap<>();
        sorted.putAll(files);

        List<String> list = new ArrayList<>(sorted.values());
        Collections.reverse(list);

        TarReader reader = openFirstFileWithValidIndex(list, archiveManager);
        if (reader != null) {
            return reader;
        }

        // no generation has a valid index, so recover as much as we can
        log.warn("Could not find a valid tar index in {}, recovering...", list);
        LinkedHashMap<UUID, byte[]> entries = new LinkedHashMap<>();
        for (String file : sorted.values()) {
            collectFileEntries(file, entries, true, archiveManager);
        }

        // regenerate the first generation based on the recovered data
        String file = sorted.values().iterator().next();
        generateTarFile(entries, file, recovery, archiveManager);

        reader = openFirstFileWithValidIndex(singletonList(file), archiveManager);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open recovered tar file " + file);
        }
    }

    @FunctionalInterface
    private interface OpenStrategy {
        SegmentArchiveReader open(SegmentArchiveManager segmentArchiveManager, String archiveName) throws IOException;
    }

    static TarReader openRO(Map<Character, String> files, TarRecovery recovery, SegmentArchiveManager archiveManager) throws IOException {
        // for readonly store only try the latest generation of a given
        // tar file to prevent any rollback or rewrite
        String file = files.get(Collections.max(files.keySet()));

        OpenStrategy recoverAndOpen = (segmentArchiveManager, archiveName) -> {
            log.info("Could not find a valid tar index in {}, recovering read-only", archiveName);
            // collecting the entries (without touching the original file) and
            // writing them into an artificial tar file '.ro.bak'
            LinkedHashMap<UUID, byte[]> entries = new LinkedHashMap<>();
            collectFileEntries(archiveName, entries, false, segmentArchiveManager);
            String bakFile = findAvailGen(archiveName, ".ro.bak", segmentArchiveManager);
            generateTarFile(entries, bakFile, recovery, segmentArchiveManager);
            return segmentArchiveManager.open(bakFile);
        };

        for (OpenStrategy openStrategy : new OpenStrategy[]{SegmentArchiveManager::open, SegmentArchiveManager::forceOpen, recoverAndOpen}) {
            TarReader reader = openFirstFileWithValidIndex(singletonList(file), archiveManager, openStrategy);
            if (reader != null) {
                return reader;
            }
        }
        throw new IOException("Failed to open tar file " + file);
    }

    /**
     * Collects all entries from the given file and optionally backs-up the
     * file, by renaming it to a ".bak" extension
     *
     * @param file    The TAR file.
     * @param entries The map where the recovered entries will be collected
     *                into.
     * @param backup  If {@code true}, performs a backup of the TAR file.
     */
    private static void collectFileEntries(String file, LinkedHashMap<UUID, byte[]> entries, boolean backup, SegmentArchiveManager archiveManager) throws IOException {
        log.info("Recovering segments from tar file {}", file);
        try {
            archiveManager.recoverEntries(file, entries);
        } catch (IOException e) {
            log.warn("Could not read tar file {}, skipping...", file, e);
        }

        if (backup) {
            backupSafely(archiveManager, file, entries.keySet());
        }
    }

    /**
     * Regenerates a tar file from a list of entries.
     *
     * @param entries   Map of entries to recover. The entries will be recovered
     *                  in the iteration order of this {@link LinkedHashMap}.
     * @param file      The output file that will contain the recovered
     *                  entries.
     * @param recovery  The recovery strategy to execute.
     */
    private static void generateTarFile(LinkedHashMap<UUID, byte[]> entries, String file, TarRecovery recovery, SegmentArchiveManager archiveManager) throws IOException {
        log.info("Regenerating tar file {}", file);

        try (TarWriter writer = new TarWriter(archiveManager, file)) {
            Map<SegmentId, Segment> segmentMap = new HashMap<>(entries.size());

            for (Entry<UUID, byte[]> entry : entries.entrySet()) {
                try {
                    recovery.recoverEntry(entry.getKey(), entry.getValue(), new EntryRecovery() {
                        @Override
                        public void recoverEntry(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
                            writer.writeEntry(msb, lsb, data, offset, size, generation);
                        }

                        @Override
                        public void recoverGraphEdge(UUID from, UUID to) {
                            writer.addGraphEdge(from, to);
                        }

                        @Override
                        public void recoverBinaryReference(GCGeneration generation, UUID segmentId, String reference) {
                            writer.addBinaryReference(generation, segmentId, reference);
                        }

                        public Segment getSegment(SegmentId id) {
                            return segmentMap.get(id);
                        }

                        public void addSegment(Segment segment) {
                            segmentMap.put(segment.getSegmentId(), segment);
                        }

                        @Override
                        public Map<SegmentId, Segment> getRecoveredSegments() {
                            return segmentMap;
                        }
                    });
                } catch (IOException e) {
                    throw new IOException(String.format("Unable to recover entry %s for file %s", entry.getKey(), file), e);
                }
            }
        }
    }

    /**
     * Backup this tar file for manual inspection. Something went wrong earlier
     * so we want to prevent the data from being accidentally removed or
     * overwritten.
     *
     * @param file File to backup.
     * @param recoveredEntries
     */
    private static void backupSafely(SegmentArchiveManager archiveManager, String file, Set<UUID> recoveredEntries) throws IOException {
        String backup = findAvailGen(file, ".bak", archiveManager);
        log.info("Backing up {} to {}", file, backup);

        archiveManager.backup(file, backup, recoveredEntries);
    }

    /**
     * Fine next available generation number so that a generated file doesn't
     * overwrite another existing file.
     *
     * @param name The file to backup.
     * @param ext  The extension of the backed up file.
     */
    private static String findAvailGen(String name, String ext, SegmentArchiveManager archiveManager) {
        String backup = name + ext;
        for (int i = 2; archiveManager.exists(backup); i++) {
            backup = name + "." + i + ext;
        }
        return backup;
    }

    private static TarReader openFirstFileWithValidIndex(List<String> archives, SegmentArchiveManager archiveManager) {
        return openFirstFileWithValidIndex(archives, archiveManager, SegmentArchiveManager::open);
    }

    private static TarReader openFirstFileWithValidIndex(List<String> archives, SegmentArchiveManager archiveManager, OpenStrategy openStrategy) {
        for (String name : archives) {
            try {
                SegmentArchiveReader reader = openStrategy.open(archiveManager, name);
                if (reader != null) {
                    for (String other : archives) {
                        if (!Objects.equals(other, name)) {
                            log.info("Removing unused tar file {}", other);
                            archiveManager.delete(other);
                        }
                    }
                    return new TarReader(archiveManager, reader);
                }
            } catch (IOException e) {
                log.warn("Could not read tar file {}, skipping...", name, e);
            }
        }

        return null;
    }

    private final SegmentArchiveManager archiveManager;

    private final SegmentArchiveReader archive;

    private final Set<UUID> segmentUUIDs;

    private volatile boolean hasGraph;

    private TarReader(SegmentArchiveManager archiveManager, SegmentArchiveReader archive) {
        this.archiveManager = archiveManager;
        this.archive = archive;
        this.segmentUUIDs = archive.listSegments()
                .stream()
                .map(e -> new UUID(e.getMsb(), e.getLsb()))
                .collect(Collectors.toSet());
    }

    long size() {
        return archive.length();
    }

    /**
     * Reads and returns the identifier of every segment included in the index
     * of this TAR file.
     *
     * @return An instance of {@link Set}.
     */
    Set<UUID> getUUIDs() {
        return segmentUUIDs;
    }

    /**
     * Check if the requested entry exists in this TAR file.
     *
     * @param msb The most significant bits of the entry identifier.
     * @param lsb The least significant bits of the entry identifier.
     * @return {@code true} if the entry exists in this TAR file, {@code false}
     * otherwise.
     */
    boolean containsEntry(long msb, long lsb) {
        return archive.containsSegment(msb, lsb);
    }

    /**
     * If the given segment is in this file, get the byte buffer that allows
     * reading it.
     * <p>
     * Whether or not this will read from the file depends on whether memory
     * mapped files are used or not.
     *
     * @param msb the most significant bits of the segment id
     * @param lsb the least significant bits of the segment id
     * @return the byte buffer, or null if not in this file.
     */
    Buffer readEntry(long msb, long lsb) throws IOException {
        return archive.readSegment(msb, lsb);
    }

    /**
     * Read the entries in this TAR file.
     *
     * @return An array of {@link IndexEntry}.
     */
    @NotNull
    SegmentArchiveEntry[] getEntries() {
        List<SegmentArchiveEntry> entryList = archive.listSegments();
        return entryList.toArray(new SegmentArchiveEntry[entryList.size()]);
    }

    /**
     * Read the references of an entry in this TAR file.
     *
     * @param id    The identifier of the entry.
     * @param graph The content of the graph of this TAR file.
     * @return The references of the provided TAR entry.
     */
    @NotNull
    private static List<UUID> getReferences(UUID id, Map<UUID, List<UUID>> graph) {
        List<UUID> references = graph.get(id);

        if (references == null) {
            return Collections.emptyList();
        }

        return references;
    }

    /**
     * Collect the references of those BLOBs that are reachable from the entries
     * in this TAR file.
     * <p>
     * The user-provided {@link Predicate} determines if entries belonging to a
     * specific generation should be inspected for binary references of not.
     * Given a generation number as input, if the predicate returns {@code
     * true}, entries from that generation will be skipped. If the predicate
     * returns {@code false}, entries from that generation will be inspected for
     * references.
     * <p>
     * The provided {@link Consumer} is callback object that will be invoked for
     * every reference found in the inspected entries.
     *
     * @param collector      An instance of {@link Consumer}.
     * @param skipGeneration An instance of {@link Predicate}.
     */
    void collectBlobReferences(@NotNull Consumer<String> collector, Predicate<GCGeneration> skipGeneration) {
        BinaryReferencesIndex references = getBinaryReferences();

        if (references == null) {
            return;
        }

        references.forEach((generation, full, compacted, segment, reference) -> {
            if (skipGeneration.test(newGCGeneration(generation, full, compacted))) {
                return;
            }
            collector.accept(reference);
        });
    }

    /**
     * Mark entries that can be reclaimed.
     * <p>
     * A data segment is reclaimable iff its generation is in the {@code
     * reclaimGeneration} predicate. A bulk segment is reclaimable if it is not
     * in {@code bulkRefs} or if it is transitively reachable through a non
     * reclaimable data segment.
     * <p>
     * The algorithm implemented by this method uses a couple of supporting data
     * structures.
     * <p>
     * The first of the supporting data structures is the set of bulk segments
     * to keep. When this method is invoked, this set initially contains the set
     * of bulk segments that are currently in use. The algorithm removes a
     * reference from this set if the corresponding bulk segment is not
     * referenced (either directly or transitively) from a marked data segment.
     * The algorithm adds a reference to this set if a marked data segment is
     * references the corresponding bulk segment. When this method returns, the
     * references in this set represent bulk segments that are currently in use
     * and should not be removed.
     * <p>
     * The second of the supporting data structures is the set of segments to
     * reclaim. This set contains references to bulk and data segments. A
     * reference to a bulk segment is added if the bulk segment is not
     * referenced (either directly or transitively) by marked data segment. A
     * reference to a data segment is added if the user-provided predicate
     * returns {@code true} for that segment. When this method returns, this set
     * contains segments that are not marked and can be removed.
     *
     * @param references  The set of bulk segments to keep.
     * @param reclaimable The set of segments to remove.
     * @param context     An instance of {@link CleanupContext}.
     */
    void mark(Set<UUID> references, Set<UUID> reclaimable, CleanupContext context) throws IOException {
        Map<UUID, List<UUID>> graph = getGraph();
        SegmentArchiveEntry[] entries = getEntries();
        for (int i = entries.length - 1; i >= 0; i--) {
            // A bulk segments is *always* written before any data segment referencing it.
            // Backward iteration ensures we see all references to bulk segments before
            // we see the bulk segment itself. Therefore we can remove a bulk reference
            // from the bulkRefs set once we encounter it, which save us some memory and
            // CPU on subsequent look-ups.
            SegmentArchiveEntry entry = entries[i];
            UUID id = new UUID(entry.getMsb(), entry.getLsb());
            GCGeneration generation = GCGeneration.newGCGeneration(entry);
            if (context.shouldReclaim(id, generation, references.remove(id))) {
                reclaimable.add(id);
            } else {
                for (UUID refId : getReferences(id, graph)) {
                    if (context.shouldFollow(id, refId)) {
                        references.add(refId);
                    }
                }
            }
        }
    }

    /**
     * Try to remove every segment contained in a user-provided set.
     * <p>
     * This method might refuse to remove the segments under the following
     * circumstances.
     * <p>
     * First, if this TAR files does not contain any of the segments that are
     * supposed to be removed. In this case, the method returns {@code null}.
     * <p>
     * Second, if this method contains some of the segments that are supposed to
     * be removed, but the reclaimable space is be less than 1/4 of the current
     * size of the TAR file. In this case, this method returns this {@link
     * TarReader}.
     * <p>
     * Third, if this TAR file is in the highest generation possible ('z') and
     * thus a new generation for this TAR file can't be created. In this case,
     * the method returns this {@link TarReader}.
     * <p>
     * Fourth, if a new TAR file has been created but it is unreadable for
     * unknown reasons. In this case, this method returns this {@link
     * TarReader}.
     * <p>
     * If none of the above conditions apply, this method returns a new {@link
     * TarReader} instance tha points to a TAR file that doesn't contain the
     * removed segments. The returned {@link TarReader} will belong to the next
     * generation of this {@link TarReader}. In this case, the {@code reclaimed}
     * set will be updated to contain the identifiers of the segments that were
     * removed from this TAR file.
     *
     * @param reclaim   Set of segment sto reclaim.
     * @param reclaimed Set of reclaimed segments. It will be update if this TAR
     *                  file is rewritten.
     * @return Either this {@link TarReader}, or a new instance of {@link
     * TarReader}, or {@code null}.
     */
    TarReader sweep(@NotNull Set<UUID> reclaim, @NotNull Set<UUID> reclaimed) throws IOException {
        String name = archive.getName();
        log.debug("Cleaning up {}", name);

        Set<UUID> cleaned = new HashSet<>();
        int afterSize = 0;
        int beforeSize = 0;
        int afterCount = 0;

        SegmentArchiveEntry[] entries = getEntries();
        for (int i = 0; i < entries.length; i++) {
            SegmentArchiveEntry entry = entries[i];
            beforeSize += archive.getEntrySize(entry.getLength());
            UUID id = new UUID(entry.getMsb(), entry.getLsb());
            if (reclaim.contains(id)) {
                cleaned.add(id);
                entries[i] = null;
            } else {
                afterSize += archive.getEntrySize(entry.getLength());
                afterCount += 1;
            }
        }

        if (afterCount == 0) {
            log.debug("None of the entries of {} are referenceable.", name);
            return null;
        }
        if (afterSize >= beforeSize * 3 / 4 && hasGraph()) {
            // the space savings are not worth it at less than 25%,
            // unless this tar file lacks a pre-compiled segment graph
            // in which case we'll always generate a new tar file with
            // the graph to speed up future garbage collection runs.
            log.debug("Not enough space savings. ({}/{}). Skipping clean up of {}",
                    archive.length() - afterSize, archive.length(), name);
            return this;
        }
        if (!hasGraph()) {
            log.warn("Recovering {}, which is missing its graph.", name);
        }

        int pos = name.length() - "a.tar".length();
        char generation = name.charAt(pos);
        if (generation == 'z') {
            log.debug("No garbage collection after reaching generation z: {}", name);
            return this;
        }

        String newFile = name.substring(0, pos) + (char) (generation + 1) + ".tar";

        log.debug("Writing new generation {}", newFile);
        TarWriter writer = new TarWriter(archiveManager, newFile);
        for (SegmentArchiveEntry entry : entries) {
            if (entry != null) {
                long msb = entry.getMsb();
                long lsb = entry.getLsb();
                int size = entry.getLength();
                GCGeneration gen = GCGeneration.newGCGeneration(entry);
                byte[] data = new byte[size];
                archive.readSegment(msb, lsb).get(data);
                writer.writeEntry(msb, lsb, data, 0, size, gen);
            }
        }

        // Reconstruct the graph index for non-cleaned segments.

        Map<UUID, List<UUID>> graph = getGraph();

        for (Entry<UUID, List<UUID>> e : graph.entrySet()) {
            if (cleaned.contains(e.getKey())) {
                continue;
            }

            Set<UUID> vertices = new HashSet<>();

            for (UUID vertex : e.getValue()) {
                if (cleaned.contains(vertex)) {
                    continue;
                }

                vertices.add(vertex);
            }

            for (UUID vertex : vertices) {
                writer.addGraphEdge(e.getKey(), vertex);
            }
        }

        // Reconstruct the binary reference index for non-cleaned segments.

        BinaryReferencesIndex references = getBinaryReferences();

        if (references != null) {
            references.forEach((gen, full, compacted, id, reference) -> {
                if (cleaned.contains(id)) {
                    return;
                }
                writer.addBinaryReference(newGCGeneration(gen, full, compacted), id, reference);
            });
        }

        writer.close();

        TarReader reader = openFirstFileWithValidIndex(singletonList(newFile), archiveManager);
        if (reader != null) {
            reclaimed.addAll(cleaned);
            return reader;
        } else {
            log.warn("Failed to open cleaned up tar file {}", getFileName());
            return this;
        }
    }

    @Override
    public void close() throws IOException {
        archive.close();
    }

    /**
     * Loads and parses the optional pre-compiled graph entry from the given tar
     * file.
     *
     * @return The parsed graph, or {@code null} if one was not found.
     */
    Map<UUID, List<UUID>> getGraph() throws IOException {
        Buffer buffer = archive.getGraph();
        if (buffer == null) {
            return null;
        } else {
            return GraphLoader.parseGraph(buffer);
        }
    }

    private boolean hasGraph() {
        return archive.hasGraph();
    }

    /**
     * Read the index of binary references from this TAR file.
     * <p>
     * The index of binary references is a two-level map. The key to the first
     * level of the map is the generation. The key to the second level of the
     * map is the identifier of a data segment in this TAR file. The value of
     * the second-level map is the set of binary references contained in the
     * segment.
     *
     * @return An instance of {@link Map}.
     */
    BinaryReferencesIndex getBinaryReferences() {
        BinaryReferencesIndex index = null;
        try {

            Buffer binaryReferences = archive.getBinaryReferences();
            if (binaryReferences == null && archive.isRemote()) {

                // This can happen because segment files and binary references files are flushed one after another in
                // {@link TarWriter#flush}
                log.info("The remote archive directory {} still does not have file with binary references written.", archive.getName());
                return null;
            }
            index = BinaryReferencesIndexLoader.parseBinaryReferencesIndex(binaryReferences);
        } catch (InvalidBinaryReferencesIndexException | IOException e) {
            log.warn("Exception while loading binary reference", e);
        }
        return index;
    }

    /**
     * Return the path of this TAR file.
     *
     * @return An instance of {@link File}.
     */
    String getFileName() {
        return archive.getName();
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return getFileName();
    }

}
