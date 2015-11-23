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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.reverse;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.readRevisions;
import static org.apache.jackrabbit.oak.plugins.segment.RecordId.fromString;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;

public class FileStoreDiff {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out
                    .println("java -jar oak-diff-*.jar <path/to/repository> [--list] [--diff=R0..R1] [--incremental] [--ignore-snfes] [--output=/path/to/output/file]");
            System.exit(0);
        }
        OptionParser parser = new OptionParser();
        OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"),
                "show help").forHelp();

        OptionSpec<File> storeO = parser.nonOptions(
                "Path to segment store (required)").ofType(File.class);
        OptionSpec<File> outO = parser
                .accepts("output", "Output file")
                .withRequiredArg()
                .ofType(File.class)
                .defaultsTo(
                        new File("diff_" + System.currentTimeMillis() + ".log"));
        OptionSpec<?> listOnlyO = parser.accepts("list",
                "Lists available revisions");
        OptionSpec<String> intervalO = parser
                .accepts(
                        "diff",
                        "Revision diff interval. Ex '--diff=R0..R1'. 'HEAD' can be used to reference the latest head revision, ie. '--diff=R0..HEAD'")
                .withRequiredArg().ofType(String.class);
        OptionSpec<?> incrementalO = parser
                .accepts("incremental",
                        "Runs diffs between each subsequent revisions in the provided interval");
        OptionSpec<String> pathO = parser
                .accepts("path", "Filter diff by given path").withRequiredArg()
                .ofType(String.class).defaultsTo("/");
        OptionSpec<?> isgnoreSNFEsO = parser
                .accepts(
                        "ignore-snfes",
                        "Ignores SegmentNotFoundExceptions and continues running the diff (experimental)");

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
        File out = outO.value(options);
        if (options.has(listOnlyO)) {
            listRevs(store, out);
        } else {
            diff(store, intervalO.value(options), options.has(incrementalO),
                    out, pathO.value(options), options.has(isgnoreSNFEsO));
        }
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

    private static void diff(File dir, String interval, boolean incremental,
            File out, String filter, boolean ignoreSNFEs) throws IOException {
        System.out.println("Store " + dir);
        System.out.println("Writing diff to " + out);
        String[] tokens = interval.trim().split("\\.\\.");
        if (tokens.length != 2) {
            System.out.println("Error parsing revision interval '" + interval
                    + "'.");
            return;
        }
        ReadOnlyStore store = new ReadOnlyStore(dir, new DummyBlobStore());
        RecordId idL = null;
        RecordId idR = null;
        try {
            if (tokens[0].equalsIgnoreCase("head")) {
                idL = store.getHead().getRecordId();
            } else {
                idL = fromString(store.getTracker(), tokens[0]);
            }
            if (tokens[1].equalsIgnoreCase("head")) {
                idR = store.getHead().getRecordId();
            } else {
                idR = fromString(store.getTracker(), tokens[1]);
            }
        } catch (IllegalArgumentException ex) {
            System.out.println("Error parsing revision interval '" + interval
                    + "': " + ex.getMessage());
            ex.printStackTrace();
            return;
        }

        long start = System.currentTimeMillis();
        PrintWriter pw = new PrintWriter(out);
        try {
            if (incremental) {
                List<String> revs = readRevisions(dir);
                System.out.println("Generating diff between " + idL + " and "
                        + idR + " incrementally. Found " + revs.size()
                        + " revisions.");

                int s = revs.indexOf(idL.toString10());
                int e = revs.indexOf(idR.toString10());
                if (s == -1 || e == -1) {
                    System.out
                            .println("Unable to match input revisions with FileStore.");
                    return;
                }
                List<String> revDiffs = revs.subList(Math.min(s, e),
                        Math.max(s, e) + 1);
                if (s > e) {
                    // reverse list
                    revDiffs = reverse(revDiffs);
                }
                if (revDiffs.size() < 2) {
                    System.out.println("Nothing to diff: " + revDiffs);
                    return;
                }
                Iterator<String> revDiffsIt = revDiffs.iterator();
                RecordId idLt = fromString(store.getTracker(),
                        revDiffsIt.next());
                while (revDiffsIt.hasNext()) {
                    RecordId idRt = fromString(store.getTracker(),
                            revDiffsIt.next());
                    boolean good = diff(store, idLt, idRt, filter, pw);
                    idLt = idRt;
                    if (!good && !ignoreSNFEs) {
                        break;
                    }
                }
            } else {
                System.out.println("Generating diff between " + idL + " and "
                        + idR);
                diff(store, idL, idR, filter, pw);
            }
        } finally {
            pw.close();
        }
        long dur = System.currentTimeMillis() - start;
        System.out.println("Finished in " + dur + " ms.");
    }

    private static boolean diff(ReadOnlyStore store, RecordId idL,
            RecordId idR, String filter, PrintWriter pw) throws IOException {
        pw.println("rev " + idL + ".." + idR);
        try {
            NodeState before = new SegmentNodeState(idL).getChildNode("root");
            NodeState after = new SegmentNodeState(idR).getChildNode("root");
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

    private static final class PrintingDiff implements NodeStateDiff {

        private final PrintWriter pw;
        private final String path;
        private final boolean skipProps;

        public PrintingDiff(PrintWriter pw, String path) {
            this(pw, path, false);
        }

        private PrintingDiff(PrintWriter pw, String path, boolean skipProps) {
            this.pw = pw;
            this.path = path;
            this.skipProps = skipProps;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (!skipProps) {
                pw.println("    + " + toString(after));
            }
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (!skipProps) {
                pw.println("    ^ " + before.getName());
                pw.println("      - " + toString(before));
                pw.println("      + " + toString(after));
            }
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (!skipProps) {
                pw.println("    - " + toString(before));
            }
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            String p = concat(path, name);
            pw.println("+ " + p);
            return after.compareAgainstBaseState(EMPTY_NODE, new PrintingDiff(
                    pw, p));
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before,
                NodeState after) {
            String p = concat(path, name);
            pw.println("^ " + p);
            return after.compareAgainstBaseState(before,
                    new PrintingDiff(pw, p));
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            String p = concat(path, name);
            pw.println("- " + p);
            return MISSING_NODE.compareAgainstBaseState(before,
                    new PrintingDiff(pw, p, true));
        }

        private static String toString(PropertyState ps) {
            StringBuilder val = new StringBuilder();
            if (ps.getType() == BINARY) {
                String v = new BlobLengthF().apply(ps.getValue(BINARY));
                val.append(" = {" + v + "}");
            } else if (ps.getType() == BINARIES) {
                String v = transform(ps.getValue(BINARIES), new BlobLengthF())
                        .toString();
                val.append("[" + ps.count() + "] = " + v);
            } else if (ps.isArray()) {
                val.append("[" + ps.count() + "] = ");
                val.append(ps.getValue(STRINGS));
            } else {
                val.append(" = " + ps.getValue(STRING));
            }
            return ps.getName() + "<" + ps.getType() + ">" + val.toString();
        }
    }

    private static class BlobLengthF implements Function<Blob, String> {

        @Override
        public String apply(Blob b) {
            return safeGetLength(b);
        }

        public static String safeGetLength(Blob b) {
            try {
                return byteCountToDisplaySize(b.length());
            } catch (IllegalStateException e) {
                // missing BlobStore probably
            }
            return "[N/A]";
        }
    }

    private static class DummyBlobStore implements BlobStore {
        @Override
        public String writeBlob(InputStream in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readBlob(String blobId, long pos, byte[] buff, int off,
                int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getBlobLength(String blobId) throws IOException {
            // best effort length extraction
            int indexOfSep = blobId.lastIndexOf("#");
            if (indexOfSep != -1) {
                return Long.valueOf(blobId.substring(indexOfSep + 1));
            }
            return -1;
        }

        @Override
        public InputStream getInputStream(String blobId) throws IOException {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public String getBlobId(String reference) {
            return reference;
        }

        @Override
        public String getReference(String blobId) {
            return blobId;
        }
    }

}
