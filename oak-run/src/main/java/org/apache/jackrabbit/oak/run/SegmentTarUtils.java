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

package org.apache.jackrabbit.oak.run;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.escape.Escapers.builder;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.isValidFileStoreOrFail;
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.newBasicReadOnlyBlobStore;
import static org.apache.jackrabbit.oak.segment.RecordId.fromString;
import static org.apache.jackrabbit.oak.segment.RecordType.NODE;
import static org.apache.jackrabbit.oak.segment.SegmentGraph.writeGCGraph;
import static org.apache.jackrabbit.oak.segment.SegmentGraph.writeSegmentGraph;
import static org.apache.jackrabbit.oak.segment.SegmentNodeStateHelper.getTemplateId;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker.checkConsistency;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.PropertyType;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordUsageAnalyser;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentVersion;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.tooling.RevisionHistory;
import org.apache.jackrabbit.oak.segment.file.tooling.RevisionHistory.HistoryElement;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

final class SegmentTarUtils {

    private static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    private static final int TAR_SEGMENT_CACHE_SIZE = Integer.getInteger("cache", 256);

    private static final int MAX_CHAR_DISPLAY = Integer.getInteger("max.char.display", 60);

    private SegmentTarUtils() {
        // Prevent instantiation
    }

    static NodeStore bootstrapNodeStore(String path, Closer closer) throws IOException {
        try {
            return SegmentNodeStoreBuilders.builder(bootstrapFileStore(path, closer)).build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    static BlobReferenceRetriever newBlobReferenceRetriever(String path, Closer closer) throws IOException {
        try {
            return new SegmentBlobReferenceRetriever(closer.register(openFileStore(path, false)));
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void backup(File source, File target) throws IOException {
        Closer closer = Closer.create();
        try {
            FileStore fs;
            if (FileStoreBackup.USE_FAKE_BLOBSTORE) {
                fs = openReadOnlyFileStore(source, newBasicReadOnlyBlobStore());
            } else {
                fs = openReadOnlyFileStore(source);
            }
            closer.register(fs);
            FileStoreBackup.backup(fs.getReader(), fs.getRevisions(), target);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    static void restore(File source, File target) throws IOException {
        try {
            FileStoreRestore.restore(source, target);
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void debug(String... args) throws IOException {
        File file = new File(args[0]);
        System.out.println("Debug " + file);
        FileStore store;
        try {
            store = openReadOnlyFileStore(file);
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        try {
            if (args.length == 1) {
                debugFileStore(store);
            } else {
                if (args[1].endsWith(".tar")) {
                    debugTarFile(store, args);
                } else {
                    debugSegment(store, args);
                }
            }
        } finally {
            store.close();
        }
    }

    static void graph(File path, boolean gcGraph, Date epoch, String regex, OutputStream out) throws Exception {
        System.out.println("Opening file store at " + path);
        ReadOnlyStore fileStore = openReadOnlyFileStore(path);
        if (gcGraph) {
            writeGCGraph(fileStore, out);
        } else {
            writeSegmentGraph(fileStore, out, epoch, regex);
        }
    }

    static void history(File directory, File journal, String path, int depth) throws IOException {
        Iterator<HistoryElement> history;
        try {
            history = new RevisionHistory(directory).getHistory(journal, path);
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        while (history.hasNext()) {
            RevisionHistory.HistoryElement historyElement = history.next();
            System.out.println(historyElement.toString(depth));
        }
    }

    static void check(File dir, String journalFileName, boolean fullTraversal, long debugLevel, long binLen) throws IOException {
        try {
            checkConsistency(dir, journalFileName, fullTraversal, debugLevel, binLen);
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void compact(File directory, boolean force) throws IOException {
        FileStore store;
        try {
            store = newFileStoreBuilder(directory.getAbsolutePath(),
                    force).withGCOptions(defaultGCOptions().setOffline()).build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        try {
            store.compact();
        } finally {
            store.close();
        }

        System.out.println("    -> cleaning up");
        try {
            store = newFileStoreBuilder(directory.getAbsolutePath(), force)
                    .withGCOptions(defaultGCOptions().setOffline()).build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        try {
            for (File file : store.cleanup()) {
                if (!file.exists() || file.delete()) {
                    System.out.println("    -> removed old file " + file.getName());
                } else {
                    System.out.println("    -> failed to remove old file " + file.getName());
                }
            }

            String head;
            File journal = new File(directory, "journal.log");
            JournalReader journalReader = new JournalReader(journal);
            try {
                head = journalReader.next() + " root " + System.currentTimeMillis() + "\n";
            } finally {
                journalReader.close();
            }

            RandomAccessFile journalFile = new RandomAccessFile(journal, "rw");
            try {
                System.out.println("    -> writing new " + journal.getName() + ": " + head);
                journalFile.setLength(0);
                journalFile.writeBytes(head);
                journalFile.getChannel().force(false);
            } finally {
                journalFile.close();
            }
        } finally {
            store.close();
        }
    }

    static void diff(File store, File out, boolean listOnly, String interval, boolean incremental, String path, boolean ignoreSNFEs) throws IOException {
        if (listOnly) {
            listRevs(store, out);
            return;
        }

        try {
            diff(store, interval, incremental, out, path, ignoreSNFEs);
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    private static FileStore bootstrapFileStore(String path, Closer closer) throws IOException, InvalidFileStoreVersionException {
        return closer.register(bootstrapFileStore(path));
    }

    private static FileStore bootstrapFileStore(String path) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(new File(path)).build();
    }

    private static void listRevs(File store, File out) throws IOException {
        System.out.println("Store " + store);
        System.out.println("Writing revisions to " + out);
        List<String> revs = readRevisions(store);
        if (revs.isEmpty()) {
            System.out.println("No revisions found.");
            return;
        }
        PrintWriter pw = new PrintWriter(out);
        try {
            for (String r : revs) {
                pw.println(r);
            }
        } finally {
            pw.close();
        }
    }

    private static List<String> readRevisions(File store) {
        File journal = new File(store, "journal.log");
        if (!journal.exists()) {
            return newArrayList();
        }

        List<String> revs = newArrayList();
        JournalReader journalReader = null;
        try {
            journalReader = new JournalReader(journal);
            try {
                revs = newArrayList(journalReader);
            } finally {
                journalReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (journalReader != null) {
                    journalReader.close();
                }
            } catch (IOException e) {
            }
        }
        return revs;
    }

    private static void diff(File dir, String interval, boolean incremental, File out, String filter, boolean ignoreSNFEs) throws IOException, InvalidFileStoreVersionException {
        System.out.println("Store " + dir);
        System.out.println("Writing diff to " + out);
        String[] tokens = interval.trim().split("\\.\\.");
        if (tokens.length != 2) {
            System.out.println("Error parsing revision interval '" + interval
                    + "'.");
            return;
        }
        ReadOnlyStore store = fileStoreBuilder(dir).withBlobStore(newBasicReadOnlyBlobStore()).buildReadOnly();
        RecordId idL = null;
        RecordId idR = null;
        try {
            if (tokens[0].equalsIgnoreCase("head")) {
                idL = store.getRevisions().getHead();
            } else {
                idL = fromString(store, tokens[0]);
            }
            if (tokens[1].equalsIgnoreCase("head")) {
                idR = store.getRevisions().getHead();
            } else {
                idR = fromString(store, tokens[1]);
            }
        } catch (IllegalArgumentException ex) {
            System.out.println("Error parsing revision interval '" + interval + "': " + ex.getMessage());
            ex.printStackTrace();
            return;
        }

        long start = System.currentTimeMillis();
        PrintWriter pw = new PrintWriter(out);
        try {
            if (incremental) {
                List<String> revs = readRevisions(dir);
                System.out.println("Generating diff between " + idL + " and " + idR + " incrementally. Found " + revs.size() + " revisions.");

                int s = revs.indexOf(idL.toString10());
                int e = revs.indexOf(idR.toString10());
                if (s == -1 || e == -1) {
                    System.out.println("Unable to match input revisions with FileStore.");
                    return;
                }
                List<String> revDiffs = revs.subList(Math.min(s, e), Math.max(s, e) + 1);
                if (s > e) {
                    // reverse list
                    revDiffs = reverse(revDiffs);
                }
                if (revDiffs.size() < 2) {
                    System.out.println("Nothing to diff: " + revDiffs);
                    return;
                }
                Iterator<String> revDiffsIt = revDiffs.iterator();
                RecordId idLt = fromString(store, revDiffsIt.next());
                while (revDiffsIt.hasNext()) {
                    RecordId idRt = fromString(store, revDiffsIt.next());
                    boolean good = diff(store, idLt, idRt, filter, pw);
                    idLt = idRt;
                    if (!good && !ignoreSNFEs) {
                        break;
                    }
                }
            } else {
                System.out.println("Generating diff between " + idL + " and " + idR);
                diff(store, idL, idR, filter, pw);
            }
        } finally {
            pw.close();
        }
        long dur = System.currentTimeMillis() - start;
        System.out.println("Finished in " + dur + " ms.");
    }

    private static boolean diff(ReadOnlyStore store, RecordId idL, RecordId idR, String filter, PrintWriter pw) throws IOException {
        pw.println("rev " + idL + ".." + idR);
        try {
            NodeState before = store.getReader().readNode(idL).getChildNode("root");
            NodeState after = store.getReader().readNode(idR).getChildNode("root");
            for (String name : elements(filter)) {
                before = before.getChildNode(name);
                after = after.getChildNode(name);
            }
            after.compareAgainstBaseState(before, new PrintingDiff(pw, filter));
            return true;
        } catch (SegmentNotFoundException ex) {
            System.out.println(ex.getMessage());
            pw.println("#SNFE " + ex.getSegmentId());
            return false;
        }
    }

    private static List<SegmentId> getReferencedIds(FileStore store, Segment segment) {
        List<SegmentId> segmentIds = new ArrayList<>();

        for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
            UUID uuid = segment.getReferencedSegmentId(i);
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();
            segmentIds.add(store.newSegmentId(msb, lsb));
        }

        return segmentIds;
    }

    private static void debugFileStore(FileStore store) {
        Map<SegmentId, List<SegmentId>> idmap = Maps.newHashMap();
        int dataCount = 0;
        long dataSize = 0;
        int bulkCount = 0;
        long bulkSize = 0;

        ((Logger) getLogger(SegmentTracker.class)).setLevel(Level.OFF);
        RecordUsageAnalyser analyser = new RecordUsageAnalyser(store.getReader());

        for (SegmentId id : store.getSegmentIds()) {
            if (id.isDataSegmentId()) {
                Segment segment = id.getSegment();
                dataCount++;
                dataSize += segment.size();
                idmap.put(id, getReferencedIds(store, segment));
                analyseSegment(segment, analyser);
            } else if (id.isBulkSegmentId()) {
                bulkCount++;
                bulkSize += id.getSegment().size();
                idmap.put(id, Collections.<SegmentId>emptyList());
            }
        }
        System.out.println("Total size:");
        System.out.format(
                "%s in %6d data segments%n",
                byteCountToDisplaySize(dataSize), dataCount);
        System.out.format(
                "%s in %6d bulk segments%n",
                byteCountToDisplaySize(bulkSize), bulkCount);
        System.out.println(analyser.toString());

        Set<SegmentId> garbage = newHashSet(idmap.keySet());
        Queue<SegmentId> queue = Queues.newArrayDeque();
        queue.add(store.getRevisions().getHead().getSegmentId());
        while (!queue.isEmpty()) {
            SegmentId id = queue.remove();
            if (garbage.remove(id)) {
                queue.addAll(idmap.get(id));
            }
        }
        dataCount = 0;
        dataSize = 0;
        bulkCount = 0;
        bulkSize = 0;
        for (SegmentId id : garbage) {
            if (id.isDataSegmentId()) {
                dataCount++;
                dataSize += id.getSegment().size();
            } else if (id.isBulkSegmentId()) {
                bulkCount++;
                bulkSize += id.getSegment().size();
            }
        }
        System.out.format("%nAvailable for garbage collection:%n");
        System.out.format("%s in %6d data segments%n",
                byteCountToDisplaySize(dataSize), dataCount);
        System.out.format("%s in %6d bulk segments%n",
                byteCountToDisplaySize(bulkSize), bulkCount);
    }

    private static void analyseSegment(Segment segment, RecordUsageAnalyser analyser) {
        for (int k = 0; k < segment.getRootCount(); k++) {
            if (segment.getRootType(k) == NODE) {
                RecordId nodeId = new RecordId(segment.getSegmentId(), segment.getRootOffset(k));
                try {
                    analyser.analyseNode(nodeId);
                } catch (Exception e) {
                    System.err.format("Error while processing node at %s", nodeId);
                    e.printStackTrace();
                }
            }
        }
    }

    private static void debugTarFile(FileStore store, String[] args) {
        File root = new File(args[0]);
        for (int i = 1; i < args.length; i++) {
            String f = args[i];
            if (!f.endsWith(".tar")) {
                System.out.println("skipping " + f);
                continue;
            }
            File tar = new File(root, f);
            if (!tar.exists()) {
                System.out.println("file doesn't exist, skipping " + f);
                continue;
            }
            System.out.println("Debug file " + tar + "(" + tar.length() + ")");
            Set<UUID> uuids = new HashSet<UUID>();
            boolean hasrefs = false;
            for (Map.Entry<String, Set<UUID>> e : store.getTarReaderIndex()
                    .entrySet()) {
                if (e.getKey().endsWith(f)) {
                    hasrefs = true;
                    uuids = e.getValue();
                }
            }
            if (hasrefs) {
                System.out.println("SegmentNodeState references to " + f);
                List<String> paths = new ArrayList<String>();
                filterNodeStates(uuids, paths, store.getHead(), "/");
                for (String p : paths) {
                    System.out.println("  " + p);
                }
            } else {
                System.out.println("No references to " + f);
            }

            try {
                Map<UUID, List<UUID>> graph = store.getTarGraph(f);
                System.out.println();
                System.out.println("Tar graph:");
                for (Map.Entry<UUID, List<UUID>> entry : graph.entrySet()) {
                    System.out.println("" + entry.getKey() + '=' + entry.getValue());
                }
            } catch (IOException e) {
                System.out.println("Error getting tar graph:");
            }

        }
    }

    private static void filterNodeStates(Set<UUID> uuids, List<String> paths, SegmentNodeState state, String path) {
        Set<String> localPaths = newTreeSet();
        for (PropertyState ps : state.getProperties()) {
            if (ps instanceof SegmentPropertyState) {
                SegmentPropertyState sps = (SegmentPropertyState) ps;
                RecordId recordId = sps.getRecordId();
                UUID id = recordId.getSegmentId().asUUID();
                if (uuids.contains(id)) {
                    if (ps.getType().tag() == PropertyType.STRING) {
                        String val = "";
                        if (ps.count() > 0) {
                            // only shows the first value, do we need more?
                            val = displayString(ps.getValue(Type.STRING, 0));
                        }
                        localPaths.add(path + ps.getName() + " = " + val
                                + " [SegmentPropertyState<" + ps.getType()
                                + ">@" + recordId + "]");
                    } else {
                        localPaths.add(path + ps + " [SegmentPropertyState<"
                                + ps.getType() + ">@" + recordId + "]");
                    }

                }
                if (ps.getType().tag() == PropertyType.BINARY) {
                    // look for extra segment references
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i);
                        for (SegmentId sbid : SegmentBlob.getBulkSegmentIds(b)) {
                            UUID bid = sbid.asUUID();
                            if (!bid.equals(id) && uuids.contains(bid)) {
                                localPaths.add(path + ps
                                        + " [SegmentPropertyState<"
                                        + ps.getType() + ">@" + recordId + "]");
                            }
                        }
                    }
                }
            }
        }

        RecordId stateId = state.getRecordId();
        if (uuids.contains(stateId.getSegmentId().asUUID())) {
            localPaths.add(path + " [SegmentNodeState@" + stateId + "]");
        }

        RecordId templateId = getTemplateId(state);
        if (uuids.contains(templateId.getSegmentId().asUUID())) {
            localPaths.add(path + "[Template@" + templateId + "]");
        }
        paths.addAll(localPaths);
        for (ChildNodeEntry ce : state.getChildNodeEntries()) {
            NodeState c = ce.getNodeState();
            if (c instanceof SegmentNodeState) {
                filterNodeStates(uuids, paths, (SegmentNodeState) c,
                        path + ce.getName() + "/");
            }
        }
    }

    private static String displayString(String value) {
        if (MAX_CHAR_DISPLAY > 0 && value.length() > MAX_CHAR_DISPLAY) {
            value = value.substring(0, MAX_CHAR_DISPLAY) + "... ("
                    + value.length() + " chars)";
        }
        String escaped = builder().setSafeRange(' ', '~')
                .addEscape('"', "\\\"").addEscape('\\', "\\\\").build()
                .escape(value);
        return '"' + escaped + '"';
    }

    private static void debugSegment(FileStore store, String[] args) {
        SegmentReader reader = store.getReader();
        Pattern pattern = Pattern
                .compile("([0-9a-f-]+)|(([0-9a-f-]+:[0-9a-f]+)(-([0-9a-f-]+:[0-9a-f]+))?)?(/.*)?");
        for (int i = 1; i < args.length; i++) {
            Matcher matcher = pattern.matcher(args[i]);
            if (!matcher.matches()) {
                System.err.println("Unknown argument: " + args[i]);
            } else if (matcher.group(1) != null) {
                UUID uuid = UUID.fromString(matcher.group(1));
                SegmentId id = store.newSegmentId(
                        uuid.getMostSignificantBits(),
                        uuid.getLeastSignificantBits());
                System.out.println(id.getSegment());
            } else {
                RecordId id1 = store.getRevisions().getHead();
                RecordId id2 = null;
                if (matcher.group(2) != null) {
                    id1 = fromString(store,
                            matcher.group(3));
                    if (matcher.group(4) != null) {
                        id2 = fromString(store,
                                matcher.group(5));
                    }
                }
                String path = "/";
                if (matcher.group(6) != null) {
                    path = matcher.group(6);
                }

                if (id2 == null) {
                    NodeState node = reader.readNode(id1);
                    System.out.println("/ (" + id1 + ") -> " + node);
                    for (String name : PathUtils.elements(path)) {
                        node = node.getChildNode(name);
                        RecordId nid = null;
                        if (node instanceof SegmentNodeState) {
                            nid = ((SegmentNodeState) node).getRecordId();
                        }
                        System.out.println("  " + name + " (" + nid + ") -> "
                                + node);
                    }
                } else {
                    NodeState node1 = reader.readNode(id1);
                    NodeState node2 = reader.readNode(id2);
                    for (String name : PathUtils.elements(path)) {
                        node1 = node1.getChildNode(name);
                        node2 = node2.getChildNode(name);
                    }
                    System.out.println(JsopBuilder.prettyPrint(JsopDiff
                            .diffToJsop(node1, node2)));
                }
            }
        }
    }

    private static FileStore openReadOnlyFileStore(File path, BlobStore blobStore) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(isValidFileStoreOrFail(path))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .withBlobStore(blobStore)
                .buildReadOnly();
    }

    private static ReadOnlyStore openReadOnlyFileStore(File path) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(isValidFileStoreOrFail(path))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .buildReadOnly();
    }

    private static FileStoreBuilder newFileStoreBuilder(String directory, boolean force) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(checkFileStoreVersionOrFail(directory, force))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED);
    }

    private static FileStore openFileStore(String directory, boolean force) throws IOException, InvalidFileStoreVersionException {
        return newFileStoreBuilder(directory, force).build();
    }

    private static File checkFileStoreVersionOrFail(String path, boolean force) throws IOException, InvalidFileStoreVersionException {
        File directory = new File(path);
        if (!directory.exists()) {
            return directory;
        }
        FileStore store = openReadOnlyFileStore(directory);
        try {
            SegmentVersion segmentVersion = getSegmentVersion(store);
            if (segmentVersion != LATEST_VERSION) {
                if (force) {
                    System.out.printf("Segment version mismatch. Found %s, expected %s. Forcing execution.\n", segmentVersion, LATEST_VERSION);
                } else {
                    throw new RuntimeException(String.format("Segment version mismatch. Found %s, expected %s. Aborting.", segmentVersion, LATEST_VERSION));
                }
            }
        } finally {
            store.close();
        }
        return directory;
    }

    private static SegmentVersion getSegmentVersion(FileStore fileStore) {
        return fileStore.getRevisions().getHead().getSegment().getSegmentVersion();
    }

}
