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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.Closer;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.FileIOUtils.BurnOnCloseFileIterator;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.sort.EscapeUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.document.DocumentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
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
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.StandardSystemProperty.FILE_SEPARATOR;
import static java.util.concurrent.TimeUnit.SECONDS;
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

    private Options opts;
    private DataStoreOptions dataStoreOpts;

    @Override
    public void execute(String... args) throws Exception {
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
        try (Closer closer = Closer.create()) {
            opts.setTempDirectory(dataStoreOpts.getWorkDir().getAbsolutePath());
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
            closer.register(fixture);

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

    private void execute(NodeStoreFixture fixture,  DataStoreOptions dataStoreOpts, Options opts, Closer closer)
        throws Exception {

        try (Closer metricsCloser = Closer.create()) {
            MetricsExporterFixture metricsExporterFixture =
                MetricsExporterFixtureProvider.create(dataStoreOpts, fixture.getWhiteboard());
            metricsCloser.register(metricsExporterFixture);

            MarkSweepGarbageCollector collector = getCollector(fixture, dataStoreOpts, opts, closer);
            if (dataStoreOpts.checkConsistency()) {
                long missing = collector.checkConsistency();
                log.warn("Found {} missing blobs", missing);

                if (dataStoreOpts.isVerbose()) {
                    new VerboseIdLogger(opts).log();
                }
            } else if (dataStoreOpts.collectGarbage()) {
                collector.collectGarbage(dataStoreOpts.markOnly());
            }
        }
    }

    private static void setupDirectories(DataStoreOptions opts) throws IOException {
        if (opts.getOutDir().exists()) {
            FileUtils.cleanDirectory(opts.getOutDir());
        }
        FileUtils.cleanDirectory(opts.getWorkDir());
    }

    private static MarkSweepGarbageCollector getCollector(NodeStoreFixture fixture, DataStoreOptions dataStoreOpts,
        Options opts, Closer closer) throws IOException {

        BlobReferenceRetriever retriever;
        if (opts.getCommonOpts().isDocument()) {
            retriever = new DocumentBlobReferenceRetriever((DocumentNodeStore) fixture.getStore());
        } else {
            if (dataStoreOpts.isVerbose()) {
                retriever = new NodeTraverserReferenceRetriever(fixture.getStore());
            } else {
                ReadOnlyFileStore fileStore = getService(fixture.getWhiteboard(), ReadOnlyFileStore.class);
                retriever = new SegmentBlobReferenceRetriever(fileStore);
            }
        }

        ExecutorService service = Executors.newSingleThreadExecutor();
        closer.register(new ExecutorCloser(service));

        String repositoryId = ClusterRepositoryInfo.getId(fixture.getStore());
        checkNotNull(repositoryId);

        MarkSweepGarbageCollector collector =
            new MarkSweepGarbageCollector(retriever, (GarbageCollectableBlobStore) fixture.getBlobStore(), service,
                dataStoreOpts.getOutDir().getAbsolutePath(), dataStoreOpts.getBatchCount(),
                SECONDS.toMillis(dataStoreOpts.getBlobGcMaxAgeInSecs()), repositoryId, fixture.getWhiteboard(),
                getService(fixture.getWhiteboard(), StatisticsProvider.class));
        collector.setTraceOutput(true);

        return collector;
    }

    protected static void setupLogging(DataStoreOptions dataStoreOpts) throws IOException {
        new LoggingInitializer(dataStoreOpts.getWorkDir(), NAME, dataStoreOpts.isResetLoggingConfig()).init();
    }

    private static void shutdownLogging() {
        LoggingInitializer.shutdownLogging();
    }

    private static void logCliArgs(String[] args) {
        log.info("Command line arguments used for datastore command [{}]", Joiner.on(' ').join(args));
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

        public NodeTraverserReferenceRetriever(NodeStore nodeStore) {
            this.nodeStore = nodeStore;
        }

        private void binaryProperties(NodeState state, String path, ReferenceCollector collector) {
            for (PropertyState p : state.getProperties()) {
                String propPath = path;//PathUtils.concat(path, p.getName());
                if (p.getType() == Type.BINARY) {
                    String blobId = p.getValue(Type.BINARY).getContentIdentity();
                    if (blobId != null) {
                        collector.addReference(blobId, propPath);
                    }
                } else if (p.getType() == Type.BINARIES && p.count() > 0) {
                    Iterator<Blob> iterator = p.getValue(Type.BINARIES).iterator();
                    while (iterator.hasNext()) {
                        String blobId = iterator.next().getContentIdentity();
                        if (blobId != null) {
                            collector.addReference(blobId, propPath);
                        }
                    }
                }
            }
        }

        private void traverseChildren(NodeState state, String path, ReferenceCollector collector) {
            binaryProperties(state, path, collector);
            for (ChildNodeEntry c : state.getChildNodeEntries()) {
                traverseChildren(c.getNodeState(), PathUtils.concat(path, c.getName()), collector);
            }
        }

        @Override public void collectReferences(ReferenceCollector collector) throws IOException {
            log.info("Starting dump of blob references by traversing");
            traverseChildren(nodeStore.getRoot(), "/", collector);
        }
    }

    static class VerboseIdLogger {
        static final String DELIM = ",";
        static final String DASH = "-";
        static final String HASH = "#";
        static final Comparator<String> idComparator = new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
            }
        };
        private final static Joiner delimJoiner = Joiner.on(DELIM).skipNulls();
        private final static Splitter delimSplitter = Splitter.on(DELIM).trimResults().omitEmptyStrings();

        private final BlobStoreOptions optionBean;
        private final BlobStoreOptions.Type blobStoreType;
        private final File outDir;
        private final File outFile;

        public VerboseIdLogger(Options options) {
            this.optionBean = options.getOptionBean(BlobStoreOptions.class);
            this.blobStoreType = optionBean.getBlobStoreType();
            outDir = options.getOptionBean(DataStoreOptions.class).getOutDir();

            outFile = filterFiles(outDir, "gccand-");
            if (outFile == null) {
                throw new IllegalArgumentException("No candidate file found");
            }
        }

        @Nullable
        static File filterFiles(File outDir, String prefix) {
            List<File> subDirs = FileFilterUtils.filterList(FileFilterUtils
                    .and(FileFilterUtils.prefixFileFilter("gcworkdir-"), FileFilterUtils.directoryFileFilter()),
                outDir.listFiles());

            if (subDirs != null && !subDirs.isEmpty()) {
                File workDir = subDirs.get(0);
                List<File> outFiles = FileFilterUtils.filterList(FileFilterUtils.prefixFileFilter(prefix), workDir.listFiles());

                if (outFiles != null && !outFiles.isEmpty()) {
                    return outFiles.get(0);
                }
            }

            return null;
        }

        static String encodeId(String line, BlobStoreOptions.Type dsType) {
            List<String> list = delimSplitter.splitToList(line);

            String id = list.get(0);
            List<String> idLengthSepList = Splitter.on(HASH).trimResults().omitEmptyStrings().splitToList(id);
            String blobId = idLengthSepList.get(0);

            if (dsType == FAKE || dsType == FDS) {
                blobId = (blobId.substring(0, 2) + FILE_SEPARATOR.value() + blobId.substring(2, 4) + FILE_SEPARATOR.value() + blobId
                    .substring(4, 6) + FILE_SEPARATOR.value() + blobId);
            } else if (dsType == S3 || dsType == AZURE) {
                blobId = (blobId.substring(0, 4) + DASH + blobId.substring(4));
            }
            return delimJoiner.join(blobId, EscapeUtils.unescapeLineBreaks(list.get(1)));
        }

        public void log() throws IOException {
            File tempFile = new File(outDir, outFile.getName() + "-temp");
            FileUtils.moveFile(outFile, tempFile);
            try (BurnOnCloseFileIterator iterator =
                    new BurnOnCloseFileIterator(FileUtils.lineIterator(tempFile, UTF_8.toString()), tempFile,
                        (Function<String, String>) input -> encodeId(input, blobStoreType))) {
                FileIOUtils.writeStrings(iterator, outFile, true, log, "Transformed to verbose ids - ");
            }
        }
    }
}

