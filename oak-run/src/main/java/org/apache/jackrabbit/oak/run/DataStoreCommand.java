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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Splitter;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.io.Closeables;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.guava.common.io.Files;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.io.BurnOnCloseFileIterator;
import org.apache.jackrabbit.oak.commons.sort.EscapeUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.run.cli.BlobStoreOptions;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.jackrabbit.guava.common.base.Stopwatch.createStarted;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeAsLine;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escapeLineBreak;
import static org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type.AZURE;
import static org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type.FAKE;
import static org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type.FDS;
import static org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type.S3;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getService;

/**
 * Command to check data store consistency and also optionally retrieve ids
 * and references.
 */
public class DataStoreCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(DataStoreCommand.class);

    public static final String NAME = "datastore";
    private static final String summary = "Provides DataStore management operations";
    private static final String DELIM = ",";

    private Options opts;
    private DataStoreOptions dataStoreOpts;

    private static final Comparator<String> idComparator = new Comparator<String>() {
        @Override public int compare(String s1, String s2) {
            return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
        }
    };


    @Override public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(DataStoreOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        dataStoreOpts = opts.getOptionBean(DataStoreOptions.class);

        //Clean up before setting up NodeStore as the temp
        //directory might be used by NodeStore for cache stuff like persistentCache
        setupDirectories(dataStoreOpts);
        setupLogging(dataStoreOpts);

        logCliArgs(args);

        boolean success = false;
        try (Closer closer = Utils.createCloserWithShutdownHook()) {
            opts.setTempDirectory(dataStoreOpts.getWorkDir().getAbsolutePath());

            log.info("Creating Node Store fixture");
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
            log.info("Registering Node Store fixture");
            closer.register(fixture);
            log.info("Node Store fixture created and registered");

            if (!checkParameters(dataStoreOpts, opts, fixture, parser)) {
                return;
            }
            execute(fixture, dataStoreOpts, opts, closer);
            success = true;
        } catch (Throwable e) {
            log.error("Error occurred while performing datastore operation", e);
            e.printStackTrace(System.err);
        } finally {
            shutdownLogging();
        }

        if (!success) {
            System.exit(1);
        }
    }

    private static boolean checkParameters(DataStoreOptions dataStoreOpts, Options opts, NodeStoreFixture fixture,
        OptionParser parser) throws IOException {

        if (!dataStoreOpts.anyActionSelected()) {
            log.info("No actions specified");
            parser.printHelpOn(System.out);
            return false;
        } else if (fixture.getStore() == null) {
            log.info("No NodeStore specified");
            parser.printHelpOn(System.out);
            return false;
        } else if (!opts.getCommonOpts().isDocument() && fixture.getBlobStore() == null) {
            log.info("No BlobStore specified");
            parser.printHelpOn(System.out);
            return false;
        }
        return true;
    }

    private void execute(NodeStoreFixture fixture, DataStoreOptions dataStoreOpts, Options opts, Closer closer)
        throws Exception {

        final BlobStoreOptions optionBean = opts.getOptionBean(BlobStoreOptions.class);
        try (Closer metricsCloser = Utils.createCloserWithShutdownHook()) {
            MetricsExporterFixture metricsExporterFixture =
                MetricsExporterFixtureProvider.create(dataStoreOpts, fixture.getWhiteboard());
            metricsCloser.register(metricsExporterFixture);

            if (dataStoreOpts.dumpRefs()) {
                log.info("Initiating dump of data store references");
                final File referencesTemp = File.createTempFile("traverseref", null, new File(opts.getTempDirectory()));
                final BufferedWriter writer = Files.newWriter(referencesTemp, StandardCharsets.UTF_8);

                boolean threw = true;
                try {
                    BlobReferenceRetriever retriever = getRetriever(fixture, dataStoreOpts, opts);

                    retriever.collectReferences(new ReferenceCollector() {
                        @Override public void addReference(String blobId, String nodeId) {
                            try {
                                Iterator<String> idIter =
                                    ((GarbageCollectableBlobStore) fixture.getBlobStore()).resolveChunks(blobId);

                                while (idIter.hasNext()) {
                                    String id = idIter.next();
                                    final Joiner delimJoiner = Joiner.on(DELIM).skipNulls();
                                    // If --verbose is present, convert blob ID to a backend friendly format and
                                    // concat the path that has the ref. Otherwise simply add the ID to the o/p file
                                    // as it is.
                                    String line = dataStoreOpts.isVerbose() ?
                                        VerboseIdLogger.encodeId(delimJoiner.join(id, escapeLineBreak(nodeId)),
                                            optionBean.getBlobStoreType()) :
                                        id;
                                    writeAsLine(writer, line, true);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Error in retrieving references", e);
                            }
                        }
                    });

                    writer.flush();
                    threw = false;

                    sort(referencesTemp, idComparator);

                    File parent = new File(dataStoreOpts.getOutDir().getAbsolutePath(), "dump");
                    long startTime = System.currentTimeMillis();
                    final File references = new File(parent, "dump-ref-" + startTime);
                    FileUtils.forceMkdir(parent);

                    FileUtils.copyFile(referencesTemp, references);
                } finally {
                    Closeables.close(writer, threw);
                }
            } else if (dataStoreOpts.dumpIds()) {
                log.info("Initiating dump of data store IDs");
                final File blobidsTemp = File.createTempFile("blobidstemp", null, new File(opts.getTempDirectory()));

                retrieveBlobIds((GarbageCollectableBlobStore) fixture.getBlobStore(), blobidsTemp);

                File parent = new File(dataStoreOpts.getOutDir().getAbsolutePath(), "dump");
                long startTime = System.currentTimeMillis();
                final File ids = new File(parent, "dump-id-" + startTime);
                FileUtils.forceMkdir(parent);

                if (dataStoreOpts.isVerbose()) {
                    verboseIds(optionBean, blobidsTemp, ids);
                } else {
                    FileUtils.copyFile(blobidsTemp, ids);
                }
            } else if (dataStoreOpts.getMetadata()) {
                log.info("Initiating dump of data store metadata");

                List<String> data = getMetadata(fixture);
                File outDir = opts.getOptionBean(DataStoreOptions.class).getOutDir();
                outDir.mkdirs();
                FileIOUtils.writeStrings(data.iterator(), new File(outDir, "metadata"), false);
            } else {
                MarkSweepGarbageCollector collector = getCollector(fixture, dataStoreOpts, opts, closer);
                if (dataStoreOpts.checkConsistency()) {
                    long missing = collector.checkConsistency(dataStoreOpts.consistencyCheckMarkOnly());
                    log.warn("Found {} missing blobs", missing);

                    if (dataStoreOpts.isVerbose()) {
                        new VerboseIdLogger(opts).log();
                    }
                } else if (dataStoreOpts.collectGarbage()) {
                    collector.collectGarbage(dataStoreOpts.markOnly());
                }
            }
        }
    }

    private static void setupDirectories(DataStoreOptions opts) throws IOException {
        if (opts.getOutDir().exists()) {
            FileUtils.cleanDirectory(opts.getOutDir());
        }
        FileUtils.cleanDirectory(opts.getWorkDir());
    }

    private static List<String> getMetadata(NodeStoreFixture fixture) {
        String repositoryId = ClusterRepositoryInfo.getId(fixture.getStore());
        requireNonNull(repositoryId);

        SharedDataStore dataStore = (SharedDataStore) fixture.getBlobStore();
        // Get all the start markers available
        List<DataRecord>  markerFiles =
            dataStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.MARKED_START_MARKER.getType());
        Map<String, List<DataRecord>> markers = markerFiles.stream().collect(Collectors.groupingBy(
            input -> SharedDataStoreUtils.SharedStoreRecordType.MARKED_START_MARKER
                .getIdFromName(input.getIdentifier().toString()),
            Collectors.mapping(Function.identity(), Collectors.toList())));
        log.info("Mapped markers {}", markers);

        // Get all the markers available
        List<DataRecord> refFiles =
            dataStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.REFERENCES.getType());

        Map<String, DataRecord> references = refFiles.stream().collect(Collectors.toMap(
            dataRecord -> dataRecord.getIdentifier().toString()
                .substring(SharedDataStoreUtils.SharedStoreRecordType.REFERENCES.getType().length() + 1),
            Function.identity()));
        log.info("Mapped references {}", references);

        // Get all the repositories registered
        List<DataRecord> repoFiles =
            dataStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getType());
        log.info("Repository files {}", repoFiles);

        List<String> records = new ArrayList<>();
        for (DataRecord repoRec : repoFiles) {
            String id =
                SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getIdFromName(repoRec.getIdentifier().toString());

            long markerTime = 0;
            long refTime = 0;
            if (markers.containsKey(id)) {
                List<DataRecord> refStartMarkers = markers.get(id);
                DataRecord earliestRefRecord = SharedDataStoreUtils.getEarliestRecord(refStartMarkers);
                log.info("Earliest record {}", earliestRefRecord);

                markerTime = TimeUnit.MILLISECONDS.toSeconds(earliestRefRecord.getLastModified());

                String uniqueSessionId = earliestRefRecord.getIdentifier().toString()
                    .substring(SharedDataStoreUtils.SharedStoreRecordType.MARKED_START_MARKER.getType().length() + 1);
                if (references.containsKey(uniqueSessionId)) {
                    refTime = TimeUnit.MILLISECONDS.toSeconds(references.get(uniqueSessionId).getLastModified());
                }
            }
            String isLocal = "-";
            if (id != null && id.equals(repositoryId)) {
                isLocal = "*";
            }
            records.add(Joiner.on("|").join(id, markerTime, refTime, isLocal));
        }
        log.info("Metadata retrieved {}", records);
        return records;
    }

    private static MarkSweepGarbageCollector getCollector(NodeStoreFixture fixture, DataStoreOptions dataStoreOpts,
        Options opts, Closer closer) throws IOException {

        BlobReferenceRetriever retriever = getRetriever(fixture, dataStoreOpts, opts);

        ExecutorService service = Executors.newSingleThreadExecutor();
        closer.register(new ExecutorCloser(service));

        String repositoryId = ClusterRepositoryInfo.getId(fixture.getStore());
        requireNonNull(repositoryId);

        MarkSweepGarbageCollector collector =
            new MarkSweepGarbageCollector(retriever, (GarbageCollectableBlobStore) fixture.getBlobStore(), service,
                dataStoreOpts.getOutDir().getAbsolutePath(), dataStoreOpts.getBatchCount(),
                SECONDS.toMillis(dataStoreOpts.getBlobGcMaxAgeInSecs()), dataStoreOpts.checkConsistencyAfterGC(),
                dataStoreOpts.sweepIfRefsPastRetention(), repositoryId, fixture.getWhiteboard(),
                getService(fixture.getWhiteboard(), StatisticsProvider.class));
        collector.setTraceOutput(true);

        return collector;
    }

    private static BlobReferenceRetriever getRetriever(NodeStoreFixture fixture, DataStoreOptions dataStoreOpts,
        Options opts) {
        BlobReferenceRetriever retriever;
        if (opts.getCommonOpts().isDocument() && !dataStoreOpts.hasVerboseRootPaths()) {
            retriever = new DocumentBlobReferenceRetriever((DocumentNodeStore) fixture.getStore());
        } else {
            if (dataStoreOpts.isVerbose()) {
                List<String> rootPathList = dataStoreOpts.getVerboseRootPaths();
                List<String> roothPathInclusionRegex = dataStoreOpts.getVerboseInclusionRegex();
                retriever = new NodeTraverserReferenceRetriever(fixture.getStore(),
                    rootPathList.toArray(new String[rootPathList.size()]),
                    roothPathInclusionRegex.toArray(new String[roothPathInclusionRegex.size()]),
                    dataStoreOpts.isUseDirListing());
            } else {
                ReadOnlyFileStore fileStore = getService(fixture.getWhiteboard(), ReadOnlyFileStore.class);
                retriever = new SegmentBlobReferenceRetriever(fileStore);
            }
        }
        return retriever;
    }

    private static void retrieveBlobIds(GarbageCollectableBlobStore blobStore, File blob) throws Exception {

        System.out.println("Starting dump of blob ids");
        Stopwatch watch = createStarted();

        Iterator<String> blobIter = blobStore.getAllChunkIds(0);
        int count = writeStrings(blobIter, blob, false);

        sort(blob);
        System.out.println(count + " blob ids found");
        System.out.println("Finished in " + watch.elapsed(SECONDS) + " seconds");
    }

    private static void verboseIds(BlobStoreOptions blobOpts, File readFile, File writeFile) throws IOException {
        LineIterator idIterator = FileUtils.lineIterator(readFile, StandardCharsets.UTF_8.name());

        try (BurnOnCloseFileIterator<String> iterator = new BurnOnCloseFileIterator<String>(idIterator, readFile,
            (Function<String, String>) input -> VerboseIdLogger.encodeId(input, blobOpts.getBlobStoreType()))) {
            writeStrings(iterator, writeFile, true, log, "Transformed to verbose ids - ");
        }
    }

    protected static void setupLogging(DataStoreOptions dataStoreOpts) throws IOException {
        new LoggingInitializer(dataStoreOpts.getWorkDir(), NAME, dataStoreOpts.isResetLoggingConfig()).init();
    }

    private static void shutdownLogging() {
        LoggingInitializer.shutdownLogging();
    }

    private static void logCliArgs(String[] args) {
        String[] filteredArgs = Arrays.stream(args).filter(str -> !str.startsWith("az:") && !str.startsWith("mongodb:"))
            .toArray(String[]::new);
        log.info("Command line arguments used for datastore command [{}]", Joiner.on(' ').join(filteredArgs));
        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        if (!inputArgs.isEmpty()) {
            log.info("System properties and vm options passed {}", inputArgs);
        }
    }

    /**
     * {@link BlobReferenceRetriever} instance which iterates over the whole node store to find
     * blobs being referred. Useful when path of those blobs needed and the underlying {@link NodeStore}
     * native implementation does not provide that.
     */
    static class NodeTraverserReferenceRetriever implements BlobReferenceRetriever {
        private final NodeStore nodeStore;
        private final String[] paths;
        private final String[] inclusionRegex;
        private boolean useDirListing;

        public NodeTraverserReferenceRetriever(NodeStore nodeStore) {
            this(nodeStore, null, null, false);
        }

        public NodeTraverserReferenceRetriever(NodeStore nodeStore,
                                               String[] paths,
                                               String[] inclusionRegex,
                                               boolean useDirListing) {
            this.nodeStore = nodeStore;
            this.paths = paths;
            this.inclusionRegex = inclusionRegex;
            this.useDirListing = useDirListing;
        }

        private void binaryProperties(NodeState state, String path, ReferenceCollector collector) {
            for (PropertyState p : state.getProperties()) {
                String propPath = path;//PathUtils.concat(path, p.getName());
                if (p.getType() == Type.BINARY) {
                    String blobId = p.getValue(Type.BINARY).getContentIdentity();
                    if (blobId != null && !p.getValue(Type.BINARY).isInlined()) {
                        collector.addReference(blobId, propPath);
                    }
                } else if (p.getType() == Type.BINARIES && p.count() > 0) {
                    Iterator<Blob> iterator = p.getValue(Type.BINARIES).iterator();
                    while (iterator.hasNext()) {
                        Blob blob = iterator.next();
                        String blobId = blob.getContentIdentity();
                        if (blobId != null && !blob.isInlined()) {
                            collector.addReference(blobId, propPath);
                        }
                    }
                }
            }
        }

        private void traverseChildren(NodeState state, String path, ReferenceCollector collector) {
            binaryProperties(state, path, collector);
            for (ChildNodeEntry c : getChildNodeEntries(state)) {
                traverseChildren(c.getNodeState(), PathUtils.concat(path, c.getName()), collector);
            }
        }

        private Iterable<? extends ChildNodeEntry> getChildNodeEntries(NodeState state) {
            if (useDirListing) {
                PropertyState dirListing = state.getProperty(OakDirectory.PROP_DIR_LISTING);
                if (dirListing != null && dirListing.isArray()) {
                    return StreamSupport.stream(dirListing.getValue(Type.STRINGS).spliterator(), false)
                            .map(name -> new AbstractChildNodeEntry() {
                                @Override
                                public @NotNull String getName() {
                                    return name;
                                }

                                @Override
                                public @NotNull NodeState getNodeState() {
                                    return state.getChildNode(name);
                                }
                            })
                            .filter(cne -> cne.getNodeState().exists())
                            .collect(Collectors.toList());
                }
            }

            // fallback to full traversal
            return state.getChildNodeEntries();
        }

        @Override public void collectReferences(ReferenceCollector collector) throws IOException {
            log.info("Starting dump of blob references by traversing");
            if (paths == null || paths.length == 0) {
                traverseChildren(nodeStore.getRoot(), "/", collector);
            } else {
                for (String path : paths) {
                    Iterable<String> nodeList = PathUtils.elements(path);
                    NodeState state = nodeStore.getRoot();
                    for (String node : nodeList) {
                        state = state.getChildNode(node);
                    }

                    if (inclusionRegex == null || inclusionRegex.length == 0) {
                        traverseChildren(state, path, collector);
                    } else {
                        for (String regex : inclusionRegex) {
                            Map<NodeState, String> inclusionMap = new HashMap<NodeState, String>();
                            getInclusionListFromRegex(state, path, regex, inclusionMap);
                            if (inclusionMap.size() == 0) {
                                System.out.println(
                                    "No valid paths found for traversal, " + "for the inclusion Regex " + regex
                                        + " under the path " + path);
                                continue;
                            }
                            for (NodeState s : inclusionMap.keySet()) {
                                traverseChildren(s, inclusionMap.get(s), collector);
                            }
                        }
                    }

                }
            }


        }

        private void getInclusionListFromRegex(NodeState rootState, String rootPath, String inclusionRegex,
            Map<NodeState, String> inclusionNodeStates) {
            Splitter delimSplitter = Splitter.on("/").trimResults().omitEmptyStrings();
            List<String> pathElementList = delimSplitter.splitToList(inclusionRegex);

            Joiner delimJoiner = Joiner.on("/").skipNulls();

            // Get the first pathElement from the regexPath
            String pathElement = pathElementList.get(0);
            // If the pathElement == *, get all child nodes and scan under them for the rest of the regex
            if ("*".equals(pathElement)) {
                for (String nodeName : rootState.getChildNodeNames()) {
                    String rootPathTemp = PathUtils.concat(rootPath, nodeName);
                    // Remove the current Path Element from the regexPath
                    // and recurse on getInclusionListFromRegex with this childNodeState and the regexPath
                    // under the current pathElement
                    String sub = delimJoiner.join(pathElementList.subList(1, pathElementList.size()));
                    getInclusionListFromRegex(rootState.getChildNode(nodeName), rootPathTemp, sub, inclusionNodeStates);
                }
            } else {
                NodeState rootStateToInclude = rootState.getChildNode(pathElement);
                if (rootStateToInclude.exists()) {
                    inclusionNodeStates.put(rootStateToInclude, PathUtils.concat(rootPath, pathElement));
                }

            }

        }
    }


    static class VerboseIdLogger {
        static final String DELIM = ",";
        static final String DASH = "-";
        static final String HASH = "#";
        static final Comparator<String> idComparator = new Comparator<String>() {
            @Override public int compare(String s1, String s2) {
                return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
            }
        };
        private final static Joiner delimJoiner = Joiner.on(DELIM).skipNulls();
        private final static Splitter delimSplitter = Splitter.on(DELIM).trimResults().omitEmptyStrings();

        private final BlobStoreOptions optionBean;
        private final BlobStoreOptions.Type blobStoreType;
        private final File outDir;
        private final List<File> outFileList = new ArrayList<File>();

        public VerboseIdLogger(Options options) {
            this.optionBean = options.getOptionBean(BlobStoreOptions.class);
            this.blobStoreType = optionBean.getBlobStoreType();
            outDir = options.getOptionBean(DataStoreOptions.class).getOutDir();

            outFileList.add(filterFiles(outDir, "marked-"));
            outFileList.add(filterFiles(outDir, "gccand-"));

            outFileList.removeAll(Collections.singleton(null));

            if (outFileList.size() == 0) {
                throw new IllegalArgumentException("No candidate file found");
            }
        }

        static File filterFiles(File outDir, String filePrefix) {
            return filterFiles(outDir, "gcworkdir-", filePrefix);
        }

        @Nullable static File filterFiles(File outDir, String dirPrefix, String filePrefix) {
            List<File> subDirs = FileFilterUtils.filterList(
                FileFilterUtils.and(FileFilterUtils.prefixFileFilter(dirPrefix), FileFilterUtils.directoryFileFilter()),
                outDir.listFiles());

            if (subDirs != null && !subDirs.isEmpty()) {
                File workDir = subDirs.get(0);
                List<File> outFiles =
                    FileFilterUtils.filterList(FileFilterUtils.prefixFileFilter(filePrefix), workDir.listFiles());

                if (outFiles != null && !outFiles.isEmpty()) {
                    return outFiles.get(0);
                }
            }

            return null;
        }

        /**
         * Encode the blob id/blob ref in a format understood by the backing datastore
         * <p>
         * Example:
         * b47b58169f121822cd4a...#123311,/a/b/c => b47b-58169f121822cd4a...,/a/b/c (dsType = S3 or Azure)
         * b47b58169f121822cd4a...#123311 => b47b-58169f121822cd4a... (dsType = S3 or Azure)
         *
         * @param line   can be either of the format b47b...#12311,/a/b/c or
         *               b47b...#12311
         * @param dsType
         * @return In case of ref dump, concatanated encoded blob ref in a
         * format understood by backing datastore impl and the path
         * on which ref is present separated by delimJoiner
         * In case of id dump, just the encoded blob ids.
         */
        static String encodeId(String line, BlobStoreOptions.Type dsType) {
            // Split the input line on ",". This would be the case while dumping refs along with paths
            // Line would be like b47b58169f121822cd4a0a0a153ba5910e581ad2bc450b6af7e51e6214c2b173#123311,/a/b/c
            // In case of dumping ids, there would not be any paths associated and there the line would simply be
            // b47b58169f121822cd4a0a0a153ba5910e581ad2bc450b6af7e51e6214c2b173#123311
            List<String> list = delimSplitter.splitToList(line);

            String id = list.get(0);
            // Split b47b58169f121822cd4a0a0a153ba5910e581ad2bc450b6af7e51e6214c2b173#123311 on # to get the id
            List<String> idLengthSepList = Splitter.on(HASH).trimResults().omitEmptyStrings().splitToList(id);
            String blobId = idLengthSepList.get(0);

            if (dsType == FAKE || dsType == FDS) {
                // 0102030405... => 01/02/03/0102030405...
                blobId = String.join(System.getProperty("file.separator"), blobId.substring(0, 2), blobId.substring(2, 4),
                        blobId.substring(4, 6), blobId);
            } else if (dsType == S3 || dsType == AZURE) {
                //b47b58169f121822cd4a0... => b47b-58169f121822cd4a0...
                blobId = (blobId.substring(0, 4) + DASH + blobId.substring(4));
            }

            // Check if the line provided as input was a line dumped from blob refs or blob ids
            // In case of blob refs dump, the list size would be 2 (Consisting of blob ref and the path on which ref
            //is present)
            // In case of blob ids dump, the list size would be 1 (Consisting of just the id)
            if (list.size() > 1) {
                // Join back the encoded blob ref and the path on which the ref is present
                return delimJoiner.join(blobId, EscapeUtils.unescapeLineBreaks(list.get(1)));
            } else {
                // return the encoded blob id
                return blobId;
            }


        }

        public void log() throws IOException {
            for (File outFile : outFileList) {
                File tempFile = new File(outDir, outFile.getName() + "-temp");
                FileUtils.moveFile(outFile, tempFile);
                try (BurnOnCloseFileIterator<String> iterator = new BurnOnCloseFileIterator<String>(
                    FileUtils.lineIterator(tempFile, StandardCharsets.UTF_8.toString()), tempFile,
                    (Function<String, String>) input -> encodeId(input, blobStoreType))) {
                    writeStrings(iterator, outFile, true, log, "Transformed to verbose ids - ");
                }
            }
        }
    }

    public static void main(String[] args) {
        long timestamp = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        String dt = formatter.format(timestamp);
        System.out.println(dt);
    }
}

