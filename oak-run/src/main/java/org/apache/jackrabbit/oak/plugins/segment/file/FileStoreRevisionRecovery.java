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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.difference;
import static java.util.Arrays.asList;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.sort;
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.readRevisions;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.RecordType;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentVersion;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;

public class FileStoreRevisionRecovery {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out
                    .println("java -jar oak-run-*.jar tarmkrecovery <path/to/repository> [--version-v10]");
            System.exit(0);
        }
        OptionParser parser = new OptionParser();
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"),
                "show help").forHelp();

        OptionSpec<File> storeO = parser.nonOptions(
                "Path to segment store (required)").ofType(File.class);
        OptionSpec<?> v10 = parser
                .accepts("version-v10", "Use V10 for reading");

        OptionSet options = parser.parse(args);

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        File store = storeO.value(options);
        if (store == null) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        SegmentVersion version;
        if (options.has(v10)) {
            version = SegmentVersion.V_10;
            System.out.println("Store(V10) " + store);
        } else {
            version = SegmentVersion.V_11;
            System.out.println("Store " + store);
        }

        SortedMap<String, UUID> candidates = candidateSegments(store);
        SortedMap<String, String> roots = extractRoots(store, candidates,
                version);

        for (Entry<String, String> r : roots.entrySet()) {
            System.out.println(r.getValue()); // + " @ " + r.getKey());
        }
    }

    private static SortedMap<String, String> extractRoots(File dir,
            SortedMap<String, UUID> candidates, final SegmentVersion version)
            throws IOException, InvalidFileStoreVersionException {

        ReadOnlyStore store = FileStore.builder(dir).withSegmentVersion(version).buildReadOnly();

        final SortedMap<String, String> roots = newTreeMap(reverseOrder());

        for (Entry<String, UUID> c : candidates.entrySet()) {
            UUID uid = c.getValue();
            SegmentId id = new SegmentId(store.getTracker(),
                    uid.getMostSignificantBits(), uid.getLeastSignificantBits());
            try {
                Segment s = store.readSegment(id);
                for (int r = 0; r < s.getRootCount(); r++) {
                    if (s.getRootType(r) == RecordType.NODE) {
                        int offset = s.getRootOffset(r);
                        RecordId nodeId = new RecordId(s.getSegmentId(), offset);
                        if (isRoot(nodeId)) {
                            roots.put(c.getKey() + "." + offset,
                                    nodeId.toString10());
                        }
                    }
                }
            } catch (SegmentNotFoundException ex) {
                System.out.println(ex.getMessage());
            }
        }
        return roots;
    }

    private static Set<String> ROOT_NAMES = ImmutableSet.of("root",
            "checkpoints");

    private static boolean isRoot(RecordId nodeId) {
        SegmentNodeState sns = new SegmentNodeState(nodeId);
        Set<String> childNames = ImmutableSet.copyOf(sns.getChildNodeNames());
        return sns.getPropertyCount() == 0 && childNames.size() == 2
                && difference(ROOT_NAMES, childNames).isEmpty();
    }

    private static SortedMap<String, UUID> candidateSegments(File store)
            throws IOException {

        List<String> revs = readRevisions(store);
        if (revs.isEmpty()) {
            System.out.println("No revisions found.");
            return newTreeMap();
        }
        String head = revs.iterator().next();
        System.out.println("Current head revision " + head);
        final UUID headSegment = extractSegmentId(head);

        List<String> tars = listTars(store);
        // <tar+offset, UUID>
        final SortedMap<String, UUID> candidates = newTreeMap(reverseOrder());

        for (final String tar : tars) {
            final AtomicLong threshold = new AtomicLong(-1);
            TarReader r = TarReader.open(new File(store, tar), true);

            // first identify the offset beyond which we need to include
            // segments
            r.accept(new TarEntryVisitor() {
                @Override
                public void visit(long msb, long lsb, File file, int offset,
                        int size) {
                    if (msb == headSegment.getMostSignificantBits()
                            && lsb == headSegment.getLeastSignificantBits()) {
                        threshold.set(offset);
                    }
                }
            });
            r.accept(new TarEntryVisitor() {
                @Override
                public void visit(long msb, long lsb, File file, int offset,
                        int size) {
                    if (offset >= threshold.get()
                            && SegmentId.isDataSegmentId(lsb)) {
                        candidates.put(tar + "." + offset, new UUID(msb, lsb));
                    }
                }
            });
            if (threshold.get() >= 0) {
                break;
            }
        }
        return candidates;
    }

    private static UUID extractSegmentId(String record) throws IOException {
        RecordId head = RecordId.fromString(new MemoryStore().getTracker(),
                record);
        return UUID.fromString(head.getSegmentId().toString());
    }

    private static List<String> listTars(File store) {
        List<String> files = asList(store.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".tar");
            }
        }));
        sort(files, reverseOrder());
        return files;
    }
}
