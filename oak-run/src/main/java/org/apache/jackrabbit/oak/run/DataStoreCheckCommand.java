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

import static org.apache.jackrabbit.guava.common.base.Stopwatch.createStarted;
import static org.apache.jackrabbit.guava.common.io.Closeables.close;
import static java.io.File.createTempFile;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.forceDelete;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeAsLine;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.base.Splitter;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.io.Closeables;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.guava.common.io.Files;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.io.FileLineDifferenceIterator;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;

/**
 * Command to check data store consistency and also optionally retrieve ids
 * and references.
 *
 * NOTE - OAK-7671 plans on deprecating this command to delegate internally to use
 * @see org.apache.jackrabbit.oak.run.DataStoreCommand instead. So
 * any new support around Datastore should be added to @see org.apache.jackrabbit.oak.run.DataStoreCommand
 */
public class DataStoreCheckCommand implements Command {
    private static final String DELIM = ",";
    private static final String FDS = "--fds";
    private static final String S3DS = "--s3ds";
    private static final String AZUREDS = "--azureblobds";
    private static final String DASH = "-";
    private static final String HASH = "#";

    private static final Comparator<String> idComparator = new Comparator<String>() {
        @Override
        public int compare(String s1, String s2) {
            return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
        }
    };

    @Override
    public void execute(String... args) throws Exception {
        System.exit(checkDataStore(args));
    }

    static int checkDataStore(String... args) {
        OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();

        String helpStr =
            "datastorecheck [--id] [--ref] [--consistency] [--store <path>|<mongo_uri>] "
                + "[--s3ds <s3ds_config>|--fds <fds_config>|--azureblobds <azureblobds_config>|--nods]"
                + " [--dump <path>] [--repoHome <repo_home>] [--track] " +
                    "[--verbose] [--verboseRootPath <verbose_root_path>]";

        try (Closer closer = Utils.createCloserWithShutdownHook()) {
            // Options for operations requested
            OptionSpecBuilder idOp = parser.accepts("id", "Get ids");
            OptionSpecBuilder refOp = parser.accepts("ref", "Get references");
            OptionSpecBuilder consistencyOp = parser.accepts("consistency", "Check consistency");

            // Node Store - needed for --ref, --consistency
            ArgumentAcceptingOptionSpec<String> store = parser.accepts("store", "Node Store")
                .requiredIf(refOp, consistencyOp).withRequiredArg().ofType(String.class);
            // Optional argument to specify the dump path
            ArgumentAcceptingOptionSpec<String> dump = parser.accepts("dump", "Dump Path")
                .withRequiredArg().ofType(String.class);

            // Optional argument to specify tracking
            OptionSpecBuilder trackOverride = parser.accepts("track", "Force override tracked ids");

            // Required argument for --consistency to specify tracking folder (otherwise can have inconsistencies)
            ArgumentAcceptingOptionSpec<String> repoHome = parser.accepts("repoHome", "Local repository home folder")
                .requiredIf(trackOverride, consistencyOp).withRequiredArg().ofType(String.class);

            // Optional argument to specify tracking
            OptionSpecBuilder verbose = parser.accepts("verbose", "Output backend formatted ids/paths");

            // Optional argument to specify root path under which tracking if to be done. Defaults to "/" if not specified
            ArgumentAcceptingOptionSpec verboseRootPath = parser.accepts("verboseRootPath",
                    "Root path to output backend formatted ids/paths")
                    .withRequiredArg().withValuesSeparatedBy(DELIM).ofType(String.class);

            OptionSpec<?> help = parser.acceptsAll(asList("h", "?", "help"),
                "show help").forHelp();

            // Required rules (any one of --id, --ref, --consistency)
            idOp.requiredUnless(refOp, consistencyOp);
            refOp.requiredUnless(idOp, consistencyOp);
            consistencyOp.requiredUnless(idOp, refOp);
            trackOverride.availableIf(idOp, consistencyOp);

            OptionSet options = null;
            try {
                options = parser.parse(args);
            } catch (Exception e) {
                System.err.println(e);
                System.err.println(Arrays.toString(args));
                System.err.println();
                System.err.println("Options :");
                parser.printHelpOn(System.err);
                return 1;
            }

            if (options.has(help)) {
                parser.printHelpOn(System.out);
                return 0;
            }

            String dumpPath = System.getProperty("java.io.tmpdir");
            if (options.has(dump)) {
                dumpPath = options.valueOf(dump);
            }

            GarbageCollectableBlobStore blobStore  = null;
            BlobReferenceRetriever marker = null;
            NodeStore nodeStore = null;
            if (options.has(store)) {
                String source = options.valueOf(store);
                if (source.startsWith(MongoURI.MONGODB_PREFIX)) {
                    MongoClientURI uri = new MongoClientURI(source);
                    MongoClient client = new MongoClient(uri);
                    DocumentNodeStore docNodeStore =
                        newMongoDocumentNodeStoreBuilder().setMongoDB(client, uri.getDatabase()).build();
                    closer.register(Utils.asCloseable(docNodeStore));
                    blobStore = (GarbageCollectableBlobStore) docNodeStore.getBlobStore();
                    marker = new DocumentBlobReferenceRetriever(docNodeStore);
                    nodeStore = docNodeStore;
                } else {
                    FileStore fileStore = fileStoreBuilder(new File(source)).withStrictVersionCheck(true).build();
                    marker = new SegmentBlobReferenceRetriever(fileStore);
                    closer.register(fileStore);
                    nodeStore =
                        SegmentNodeStoreBuilders.builder(fileStore).build();
                }
            }

            // Initialize S3/FileDataStore if configured
            String dsType = "";
            GarbageCollectableBlobStore dataStore  = Utils.bootstrapDataStore(args, closer);
            if (dataStore != null) {
                dsType = getDSType(args);
                blobStore = dataStore;
            }

            // blob store still not initialized means configuration not supported
            if (blobStore == null) {
                System.err.println("Operation not defined for SegmentNodeStore without external datastore");
                parser.printHelpOn(System.err);
                return 1;
            }

            FileRegister register = new FileRegister(options);
            closer.register(register);

            if (options.has(idOp) || options.has(consistencyOp)) {
                File idTemp = createTempFile("ids", null);
                closer.register(new Closeable() {
                    @Override public void close() throws IOException {
                        forceDelete(idTemp);
                    }
                });

                File dumpFile = register.createFile(idOp, dumpPath);
                retrieveBlobIds(blobStore, idTemp);

                // If track path and track override specified copy the file to the location
                if (options.has(repoHome) && options.has(trackOverride)) {
                    String trackPath = options.valueOf(repoHome);
                    File trackingFileParent = new File(FilenameUtils.concat(trackPath, "blobids"));
                    File trackingFile = new File(trackingFileParent,
                        "blob-" + String.valueOf(System.currentTimeMillis()) + ".gen");
                    FileUtils.copyFile(idTemp, trackingFile);
                }

                if (options.has(verbose)) {
                    verboseIds(closer, dsType, idTemp, dumpFile);
                } else {
                    FileUtils.copyFile(idTemp, dumpFile);
                }
            }

            if (options.has(refOp) || options.has(consistencyOp)) {

                // Find blob ids by traversal for verbose mode + Segment Store or if verboseRootPath option
                // is present (find blob references under a specific root path.)
                if ((options.has(verbose) &&
                    (nodeStore instanceof SegmentNodeStore ||
                        nodeStore instanceof org.apache.jackrabbit.oak.segment.SegmentNodeStore)) ||
                        options.has(verboseRootPath)) {
                    NodeTraverser traverser = new NodeTraverser(nodeStore, dsType);
                    closer.register(traverser);

                    List<String> rootPathList = options.valuesOf(verboseRootPath);
                    traverser.traverse((String[]) rootPathList.toArray(new String[rootPathList.size()]));

                    FileUtils.copyFile(traverser.references, register.createFile(refOp, dumpPath));
                } else {
                    retrieveBlobReferences(blobStore, marker,
                        register.createFile(refOp, dumpPath), dsType, options.has(verbose));
                }
            }

            if (options.has(consistencyOp)) {
                checkConsistency(register.get(idOp), register.get(refOp),
                    register.createFile(consistencyOp, dumpPath), options.valueOf(repoHome), dsType);
            }

            return 0;
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            return 1;
        }
    }

    private static void verboseIds(Closer closer, final String dsType, File readFile, File writeFile) throws IOException {
        LineIterator idIterator = FileUtils.lineIterator(readFile, StandardCharsets.UTF_8.name());
        try {
            // Create a temp file to write real ids and register with closer
            File longIdTemp = createTempFile("longids", null);
            closer.register(new Closeable() {
                @Override public void close() throws IOException {
                    forceDelete(longIdTemp);
                }
            });

            // Read and write the converted ids
            FileIOUtils.writeStrings(idIterator, longIdTemp, false,
                    (java.util.function.Function<String, String>) ((input) -> encodeId(input, dsType)), null, null);
            FileUtils.copyFile(longIdTemp, writeFile);
        } finally {
            if (idIterator != null) {
                idIterator.close();
            }
        }
    }

    private static String getDSType(String[] args) {
        List<String> argList = Arrays.asList(args);
        if (argList.contains(S3DS)) {
            return S3DS;
        } else if (argList.contains(FDS)) {
            return FDS;
        } else if (argList.contains(AZUREDS)) {
            return AZUREDS;
        }
        return "";
    }

    static String encodeId(String id, String dsType) {
        List<String> idLengthSepList = Splitter.on(HASH).trimResults().omitEmptyStrings().splitToList(id);
        String blobId = idLengthSepList.get(0);

        if (dsType.equals(FDS)) {
            return String.join(System.getProperty("file.separator"), blobId.substring(0, 2), blobId.substring(2, 4),
                    blobId.substring(4, 6), blobId);
        } else if (dsType.equals(S3DS) || dsType.equals(AZUREDS)) {
            return (blobId.substring(0, 4) + DASH + blobId.substring(4));
        }
        return id;
    }

    private static String decodeId(String id) {
        List<String> list = Splitter.on(System.getProperty("file.separator")).trimResults().omitEmptyStrings().splitToList(id);
        String pathStrippedId = list.get(list.size() -1);
        return String.join("", Splitter.on(DASH).omitEmptyStrings().trimResults().splitToList(pathStrippedId));
    }

    static class FileRegister implements Closeable {
        Map<OptionSpec, File> opFiles = new HashMap<>();
        String suffix = String.valueOf(System.currentTimeMillis());
        OptionSet options;

        public FileRegister(OptionSet options) {
            this.options = options;
        }

        public File createFile(OptionSpec spec, String path) {
            File f = new File(path, spec.toString() + suffix);
            opFiles.put(spec, f);
            return f;
        }

        public File get(OptionSpec spec) {
            return opFiles.get(spec);
        }

        @Override
        public void close() throws IOException {
            Iterator<Map.Entry<OptionSpec, File>> iterator = opFiles.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<OptionSpec, File> entry = iterator.next();
                File f = entry.getValue();
                if (options.has(entry.getKey()) && f.length() != 0) {
                    System.out.println(entry.getKey().toString() + " - " + f.getAbsolutePath());
                } else {
                    if (f.exists()) {
                        forceDelete(f);
                    }
                }
            }
        }
    }

    private static void checkConsistency(File ids, File refs, File missing, String trackRoot, String dsType)
        throws IOException {
        System.out.println("Starting consistency check");
        Stopwatch watch = createStarted();

        FileLineDifferenceIterator iter = new FileLineDifferenceIterator(ids, refs, new java.util.function.Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                if (input != null) {
                    return input.split(DELIM)[0];
                }
                return "";
            }});


        // write the candidates identified to a temp file
        File candTemp = createTempFile("candTemp", null);
        int candidates = writeStrings(iter, candTemp, false);

        try {
            // retrieve the .del file from track directory
            File trackingFileParent = new File(FilenameUtils.concat(trackRoot, "blobids"));
            if (trackingFileParent.exists()) {
                Collection<File> files =
                    listFiles(trackingFileParent, FileFilterUtils.suffixFileFilter(".del"), null);

                // If a delete file is present filter the tracked deleted ids
                if (!files.isEmpty()) {
                    File delFile = files.iterator().next();
                    FileLineDifferenceIterator filteringIter = new FileLineDifferenceIterator(delFile, candTemp, new java.util.function.Function<String, String>() {
                        @Nullable @Override public String apply(@Nullable String input) {
                            if (input != null) {
                                return encodeId(decodeId(input.split(DELIM)[0]), dsType);
                            }
                            return "";
                        }
                    });
                    candidates = FileIOUtils.writeStrings(filteringIter, missing, false);
                }
            } else {
                System.out.println("Skipping active deleted tracked as parameter [repoHome] : [" + trackRoot + "] incorrect");
                FileUtils.copyFile(candTemp, missing);
            }
        } finally {
            FileUtils.forceDelete(candTemp);
        }

        System.out.println("Consistency check found " + candidates + " missing blobs");
        if (candidates > 0) {
            System.out.println("Consistency check failure for the data store");
        }
        System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    private static void retrieveBlobReferences(GarbageCollectableBlobStore blobStore, BlobReferenceRetriever marker,
        File marked, String dsType, boolean isVerbose) throws IOException {
        final BufferedWriter writer = Files.newWriter(marked, StandardCharsets.UTF_8);
        final AtomicInteger count = new AtomicInteger();
        boolean threw = true;
        try {
            final GarbageCollectableBlobStore finalBlobStore = blobStore;

            System.out.println("Starting dump of blob references");
            Stopwatch watch = createStarted();

            marker.collectReferences(
                new ReferenceCollector() {
                    @Override
                    public void addReference(String blobId, String nodeId) {
                        try {
                            Iterator<String> idIter = finalBlobStore.resolveChunks(blobId);

                            while (idIter.hasNext()) {
                                String id = idIter.next();
                                if (isVerbose) {
                                    id = encodeId(id, dsType);
                                }
                                String combinedId = nodeId == null ?
                                        id : String.join(DELIM, id, escapeLineBreak(nodeId));
                                count.getAndIncrement();
                                writeAsLine(writer, combinedId, false);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error in retrieving references", e);
                        }
                    }
                }
            );
            writer.flush();
            sort(marked, idComparator);

            System.out.println(count.get() + " blob references found");
            System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
            threw = false;
        } finally {
            close(writer, threw);
        }
    }

    private static void retrieveBlobIds(GarbageCollectableBlobStore blobStore, File blob)
        throws Exception {

        System.out.println("Starting dump of blob ids");
        Stopwatch watch = createStarted();

        Iterator<String> blobIter = blobStore.getAllChunkIds(0);
        int count = writeStrings(blobIter, blob, false);

        sort(blob);
        System.out.println(count + " blob ids found");
        System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    static class NodeTraverser implements Closeable {
        private final String dsType;
        private final File references;
        private final NodeStore nodeStore;

        public NodeTraverser(NodeStore nodeStore, String dsType) throws IOException {
            this.references = File.createTempFile("traverseref", null);
            this.nodeStore = nodeStore;
            this.dsType = dsType;
        }

        private void binaryProperties(NodeState state, String path, BufferedWriter writer, AtomicInteger count) {
            for (PropertyState p : state.getProperties()) {
                String propPath = PathUtils.concat(path, p.getName());
                try {
                    String id ;
                    if (p.getType() == Type.BINARY) {
                        id = p.getValue(Type.BINARY).getContentIdentity();
                        // Ignore inline encoded binaries in document mk and null references in segment mk
                        if (id == null || p.getValue(Type.BINARY).isInlined()) {
                            continue;
                        }
                        writeAsLine(writer,
                                getLine(id, propPath), false);
                        count.incrementAndGet();

                    } else if (p.getType() == Type.BINARIES && p.count() > 0) {
                        Iterator<Blob> iterator = p.getValue(Type.BINARIES).iterator();
                        while (iterator.hasNext()) {

                            Blob blob = iterator.next();
                            id = blob.getContentIdentity();
                            // Ignore inline encoded binaries in document mk
                            if (id == null || blob.isInlined()) {
                                continue;
                            }
                            writeAsLine(writer,
                                getLine(id, propPath), false);
                            count.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error in retrieving blob id for path " + propPath);
                }
            }
        }

        private String getLine(String id, String path) {
            return path != null ? String.join(DELIM, encodeId(id, dsType), escapeLineBreak(path)) : id;
        }

        private void traverseChildren(NodeState state, String path, BufferedWriter writer, AtomicInteger count) {
            binaryProperties(state, path, writer, count);
            for (ChildNodeEntry c : state.getChildNodeEntries()) {
                traverseChildren(c.getNodeState(), PathUtils.concat(path, c.getName()), writer, count);
            }
        }

        public void traverse(String ... paths) throws IOException {
            BufferedWriter writer = null;
            final AtomicInteger count = new AtomicInteger();
            boolean threw = true;
            System.out.println("Starting dump of blob references by traversing");
            Stopwatch watch = createStarted();

            try {
                writer = Files.newWriter(references, StandardCharsets.UTF_8);
                if (paths.length == 0) {
                    traverseChildren(nodeStore.getRoot(), "/", writer, count);
                } else {
                    for (String path: paths ) {
                        Iterable<String> nodeList = PathUtils.elements(path);
                        NodeState state = nodeStore.getRoot();
                        for (String node: nodeList) {
                            state = state.getChildNode(node);
                        }
                        traverseChildren(state, path, writer, count);
                    }
                }

                writer.flush();
                sort(references, idComparator);

                System.out.println(count.get() + " blob references found");
                System.out.println("Finished in " + watch.elapsed(TimeUnit.SECONDS) + " seconds");
                threw = false;
            } finally {
                Closeables.close(writer, threw);
            }
        }


        @Override
        public void close() throws IOException {
            FileUtils.forceDelete(references);
        }
    }
}
