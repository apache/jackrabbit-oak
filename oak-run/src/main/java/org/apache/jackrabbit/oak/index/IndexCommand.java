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

package org.apache.jackrabbit.oak.index;

import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.io.Closer;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.async.AsyncIndexerLucene;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.DocumentBuilderCustomizer;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_SORTED_FILE_PATH;

public class IndexCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(IndexCommand.class);
    private static final String LOG_SUFFIX = "indexing";

    public static final String NAME = "index";
    public static final String INDEX_DEFINITIONS_JSON = "index-definitions.json";
    public static final String INDEX_INFO_TXT = "index-info.txt";
    public static final String INDEX_CONSISTENCY_CHECK_TXT = "index-consistency-check-report.txt";

    private final String summary = "Provides index management related operations";

    private File info;
    private File definitions;
    private File consistencyCheckReport;
    private Options opts;
    private IndexOptions indexOpts;
    private static boolean disableExitOnError;

    public void checkTikaDependency() throws ClassNotFoundException {
        Class.forName("org.apache.tika.parser.pdf.PDFParser");
    }

    // to be overridden by test cases that want to check the return value
    public void exit(int status) {
        System.exit(status);
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(IndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        indexOpts = opts.getOptionBean(IndexOptions.class);

        if (indexOpts.isReindex() && !opts.getCommonOpts().isHelpRequested() && !indexOpts.isIgnoreMissingTikaDep()) {
            try {
                checkTikaDependency();
            } catch (Throwable e) {
                System.err.println("Missing tika parser dependencies, use --ignore-missing-tika-dep to force continue");
                exit(1);
                return;
            }
        }

        //Clean up before setting up NodeStore as the temp
        //directory might be used by NodeStore for cache stuff like persistentCache
        setupDirectories(indexOpts);
        setupLogging(indexOpts);

        logCliArgs(args);

        boolean success = false;
        try {
            if (indexOpts.isReindex() && opts.getCommonOpts().isReadWrite()) {
                performReindexInReadWriteMode(indexOpts);
            } else if (indexOpts.isAsyncIndex()) {
                Closer closer = Closer.create();
                NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);
                AsyncIndexerLucene asyncIndexerService = new AsyncIndexerLucene(extendedIndexHelper, indexOpts.isCowCorEnabled(), closer,
                        indexOpts.getAsyncLanes(), indexOpts.aysncDelay());
                closer.register(asyncIndexerService);
                closer.register(fixture);
                asyncIndexerService.execute();
            } else {
                try (Closer closer = Closer.create()) {
                    configureCustomizer(opts, closer, true);
                    NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                    closer.register(fixture);
                    execute(fixture, indexOpts, closer);
                    tellReportPaths();
                }
            }
            success = true;
        } catch (Throwable e) {
            log.error("Error occurred while performing index tasks", e);
            e.printStackTrace(System.err);
            if (disableExitOnError) {
                throw e;
            }
        } finally {
            shutdownLogging();
        }

        if (!success) {
            System.exit(1);
        }
    }

    public static void setDisableExitOnError(boolean disableExitOnError) {
        IndexCommand.disableExitOnError = disableExitOnError;
    }

    private void tellReportPaths() {
        if (info != null) {
            System.out.printf("Index stats stored at %s%n", getPath(info));
        }

        if (definitions != null) {
            System.out.printf("Index definitions stored at %s%n", getPath(definitions));
        }

        if (consistencyCheckReport != null) {
            System.out.printf("Index consistency check report stored at %s%n", getPath(consistencyCheckReport));
        }
    }

    private void execute(NodeStoreFixture fixture, IndexOptions indexOpts, Closer closer)
            throws IOException, CommitFailedException {
        ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);

        dumpIndexStats(indexOpts, extendedIndexHelper);
        dumpIndexDefinitions(indexOpts, extendedIndexHelper);
        performConsistencyCheck(indexOpts, extendedIndexHelper);
        dumpIndexContents(indexOpts, extendedIndexHelper);
        reindexOperation(indexOpts, extendedIndexHelper);
        importIndexOperation(indexOpts, extendedIndexHelper);

        log.info("[INDEXING_REPORT:INDEX_UPLOAD]\n{}", extendedIndexHelper.getIndexReporter().generateReport());
    }

    private ExtendedIndexHelper createIndexHelper(NodeStoreFixture fixture,
                                                  IndexOptions indexOpts, Closer closer) throws IOException {
        ExtendedIndexHelper extendedIndexHelper = new ExtendedIndexHelper(fixture.getStore(), fixture.getBlobStore(), fixture.getWhiteboard(),
                indexOpts.getOutDir(), indexOpts.getWorkDir(), computeIndexPaths(indexOpts));

        configurePreExtractionSupport(indexOpts, extendedIndexHelper);

        closer.register(extendedIndexHelper);
        return extendedIndexHelper;
    }

    private List<String> computeIndexPaths(IndexOptions indexOpts) throws IOException {
        //Combine the indexPaths from json and cli args
        Set<String> indexPaths = new LinkedHashSet<>(indexOpts.getIndexPaths());
        File definitions = indexOpts.getIndexDefinitionsFile();
        if (definitions != null) {
            IndexDefinitionUpdater updater = new IndexDefinitionUpdater(definitions);
            Set<String> indexPathsFromJson = updater.getIndexPaths();
            Set<String> diff = Sets.difference(indexPathsFromJson, indexPaths);
            if (!diff.isEmpty()) {
                log.info("Augmenting the indexPaths with {} which are present in {}", diff, definitions);
            }
            indexPaths.addAll(indexPathsFromJson);
        }
        return new ArrayList<>(indexPaths);
    }

    private void configurePreExtractionSupport(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        File preExtractedTextDir = indexOpts.getPreExtractedTextDir();
        if (preExtractedTextDir != null) {
            extendedIndexHelper.setPreExtractedTextDir(preExtractedTextDir);
            log.info("Using pre-extracted text directory {}", getPath(preExtractedTextDir));
        }
    }

    private void reindexOperation(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException, CommitFailedException {
        if (!indexOpts.isReindex()) {
            return;
        }

        String checkpoint = indexOpts.getCheckpoint();
        File destDir = reindex(indexOpts, extendedIndexHelper, checkpoint);
        log.info("To complete indexing import the created index files via IndexerMBean#importIndex operation with " +
                "[{}] as input", getPath(destDir));
    }

    private void importIndexOperation(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException, CommitFailedException {
        if (indexOpts.isImportIndex()) {
            File importDir = indexOpts.getIndexImportDir();
            importIndex(extendedIndexHelper, importDir);
        }
    }

    private File reindex(IndexOptions idxOpts, ExtendedIndexHelper extendedIndexHelper, String checkpoint) throws IOException, CommitFailedException {
        requireNonNull(checkpoint, "Checkpoint value is required for reindexing done in read only mode");

        Stopwatch reindexWatch = Stopwatch.createStarted();
        IndexerSupport indexerSupport = createIndexerSupport(extendedIndexHelper, checkpoint);
        log.info("Proceeding to index {} upto checkpoint {} {}", extendedIndexHelper.getIndexPaths(), checkpoint,
                indexerSupport.getCheckpointInfo());

        if (opts.getCommonOpts().isMongo() && idxOpts.isDocTraversalMode()) {
            log.info("Using Document order traversal to perform reindexing");
            try (DocumentStoreIndexer indexer = new DocumentStoreIndexer(extendedIndexHelper, indexerSupport)) {
                if (idxOpts.buildFlatFileStoreSeparately()) {
                    IndexStore store = indexer.buildStore();
                    String pathToStore = store.getStorePath();
                    System.setProperty(OAK_INDEXER_SORTED_FILE_PATH, pathToStore);
                }
                indexer.reindex();
            }
        } else {
            try (OutOfBandIndexer indexer = new OutOfBandIndexer(extendedIndexHelper, indexerSupport)) {
                indexer.reindex();
            }
        }

        indexerSupport.writeMetaInfo(checkpoint);
        File destDir = indexerSupport.copyIndexFilesToOutput();
        log.info("Indexing completed for indexes {} in {} ({} ms) and index files are copied to {}",
                extendedIndexHelper.getIndexPaths(), reindexWatch, reindexWatch.elapsed(TimeUnit.MILLISECONDS), IndexCommand.getPath(destDir));
        return destDir;
    }

    private void importIndex(ExtendedIndexHelper extendedIndexHelper, File importDir) throws IOException, CommitFailedException {
        new IndexImporterSupport(extendedIndexHelper).importIndex(importDir);
    }

    private void performReindexInReadWriteMode(IndexOptions indexOpts) throws Exception {
        Stopwatch w = Stopwatch.createStarted();
        //TODO To support restart we need to store this checkpoint somewhere
        String checkpoint = connectInReadWriteModeAndCreateCheckPoint(indexOpts);
        log.info("Created checkpoint [{}] for indexing", checkpoint);

        log.info("Proceeding to reindex with read only access to NodeStore");
        File indexDir = performReindexInReadOnlyMode(indexOpts, checkpoint);

        Stopwatch importWatch = Stopwatch.createStarted();
        log.info("Proceeding to import index data from [{}] by connecting to NodeStore in read-write mode", getPath(indexDir));
        connectInReadWriteModeAndImportIndex(indexOpts, indexDir);
        log.info("Indexes imported successfully in {} ({} ms)", importWatch, importWatch.elapsed(TimeUnit.MILLISECONDS));

        log.info("Indexing completed and imported successfully in {} ({} ms)", w, w.elapsed(TimeUnit.MILLISECONDS));
    }

    private File performReindexInReadOnlyMode(IndexOptions indexOpts, String checkpoint) throws Exception {
        try (Closer closer = Closer.create()) {
            configureCustomizer(opts, closer, true);
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts, true);
            closer.register(fixture);
            ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);
            reindex(indexOpts, extendedIndexHelper, checkpoint);
            return new File(indexOpts.getOutDir(), OutOfBandIndexer.LOCAL_INDEX_ROOT_DIR);
        }
    }

    private String connectInReadWriteModeAndCreateCheckPoint(IndexOptions indexOpts) throws Exception {
        String checkpoint = indexOpts.getCheckpoint();
        if (checkpoint != null) {
            log.info("Using provided checkpoint [{}]", checkpoint);
            return checkpoint;
        }

        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            return fixture.getStore().checkpoint(TimeUnit.DAYS.toMillis(100), ImmutableMap.of(
                    "creator", IndexCommand.class.getSimpleName(),
                    "created", now()));
        }
    }

    private void connectInReadWriteModeAndImportIndex(IndexOptions indexOpts, File indexDir) throws Exception {
        try (Closer closer = Closer.create()) {
            configureCustomizer(opts, closer, false);
            NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
            closer.register(fixture);
            ExtendedIndexHelper extendedIndexHelper = createIndexHelper(fixture, indexOpts, closer);
            importIndex(extendedIndexHelper, indexDir);
        }
    }

    private void dumpIndexContents(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.dumpIndex()) {
            new IndexDumper(extendedIndexHelper, indexOpts.getOutDir()).dump();
        }
    }

    private void performConsistencyCheck(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.checkConsistency()) {
            IndexConsistencyCheckPrinter printer =
                    new IndexConsistencyCheckPrinter(extendedIndexHelper, indexOpts.consistencyCheckLevel());
            PrinterDumper dumper = new PrinterDumper(extendedIndexHelper.getOutputDir(), INDEX_CONSISTENCY_CHECK_TXT,
                    false, Format.TEXT, printer);
            dumper.dump();
            consistencyCheckReport = dumper.getOutFile();
        }
    }

    private void dumpIndexDefinitions(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.dumpDefinitions()) {
            PrinterDumper dumper = new PrinterDumper(extendedIndexHelper.getOutputDir(), INDEX_DEFINITIONS_JSON,
                    false, Format.JSON, extendedIndexHelper.getIndexDefnPrinter());
            dumper.dump();
            definitions = dumper.getOutFile();
        }
    }

    private void dumpIndexStats(IndexOptions indexOpts, ExtendedIndexHelper extendedIndexHelper) throws IOException {
        if (indexOpts.dumpStats()) {
            PrinterDumper dumper = new PrinterDumper(extendedIndexHelper.getOutputDir(), INDEX_INFO_TXT,
                    true, Format.TEXT, extendedIndexHelper.getIndexPrinter());
            dumper.dump();
            info = dumper.getOutFile();
        }
    }

    private IndexerSupport createIndexerSupport(ExtendedIndexHelper extendedIndexHelper, String checkpoint) {
        IndexerSupport indexerSupport = new IndexerSupport(extendedIndexHelper, checkpoint)
                .withExistingDataDumpDir(indexOpts.getExistingDataDumpDir());

        File definitions = indexOpts.getIndexDefinitionsFile();
        if (definitions != null) {
            checkArgument(definitions.exists(), "Index definitions file [%s] not found", getPath(definitions));
            indexerSupport.setIndexDefinitions(definitions);
        }
        return indexerSupport;
    }

    private static void setupDirectories(IndexOptions indexOpts) throws IOException {
        if (indexOpts.getOutDir().exists()) {
            if (indexOpts.isImportIndex() &&
                    FileUtils.directoryContains(indexOpts.getOutDir(), indexOpts.getIndexImportDir())) {
                //Do not clean directory in this case
            } else {
                FileUtils.cleanDirectory(indexOpts.getOutDir());
            }
        }
        cleanWorkDir(indexOpts.getWorkDir());
    }

    private static void cleanWorkDir(File workDir) throws IOException {
        //TODO Do not clean if restarting
        String[] dirListing = workDir.list();
        if (dirListing != null && dirListing.length != 0) {
            FileUtils.cleanDirectory(workDir);
        }
    }

    private static void setupLogging(IndexOptions indexOpts) throws IOException {
        new LoggingInitializer(indexOpts.getWorkDir(), LOG_SUFFIX).init();
    }

    private void shutdownLogging() {
        LoggingInitializer.shutdownLogging();
    }

    private static String now() {
        return ISO8601.format(Calendar.getInstance());
    }

    private static void logCliArgs(String[] args) {
        log.info("Command line arguments used for indexing [{}]", Joiner.on(' ').join(args));
        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        if (!inputArgs.isEmpty()) {
            log.info("System properties and vm options passed {}", inputArgs);
        }
    }

    static Path getPath(File file) {
        return file.toPath().normalize().toAbsolutePath();
    }

    private static void configureCustomizer(Options opts, Closer closer, boolean readOnlyAccess) {
        if (opts.getCommonOpts().isDocument()) {
            IndexOptions indexOpts = opts.getOptionBean(IndexOptions.class);
            if (indexOpts.isReindex()) {
                IndexDocumentBuilderCustomizer customizer = new IndexDocumentBuilderCustomizer(opts, readOnlyAccess);
                Registration reg = opts.getWhiteboard().register(DocumentBuilderCustomizer.class, customizer, emptyMap());
                closer.register(reg::unregister);
            }
        }
    }
}
