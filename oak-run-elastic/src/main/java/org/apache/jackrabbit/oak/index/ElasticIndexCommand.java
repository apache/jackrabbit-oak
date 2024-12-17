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

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.io.Closer;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.index.async.AsyncIndexerElastic;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/*
Command file for Elastic index operation.
 */
public class ElasticIndexCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(ElasticIndexCommand.class);
    private Options opts;
    private ElasticIndexOptions indexOpts;
    public static final String NAME = "index";
    private static final String LOG_SUFFIX = "indexing";

    private final String summary = "Provides elastic index management related operations";
    private static boolean disableExitOnError;


    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(ElasticIndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        indexOpts = opts.getOptionBean(ElasticIndexOptions.class);

        //Clean up before setting up NodeStore as the temp
        //directory might be used by NodeStore for cache stuff like persistentCache
        setupDirectories(indexOpts);
        setupLogging(indexOpts);

        logCliArgs(args);

        boolean success = false;
        try {
            if (indexOpts.isAsyncIndex()) {
                Closer closer = Closer.create();
                NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                IndexHelper indexHelper = createIndexHelper(fixture, indexOpts, closer);
                AsyncIndexerElastic asyncIndexerService = new AsyncIndexerElastic(indexHelper, closer,
                        indexOpts.getAsyncLanes(), indexOpts.aysncDelay(), indexOpts);
                closer.register(asyncIndexerService);
                closer.register(fixture);
                asyncIndexerService.execute();
            } else {
                try (Closer closer = Closer.create()) {
                    NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                    closer.register(fixture);
                    execute(fixture, indexOpts, closer);
                }
            }
            success = true;
        } catch (Throwable e) {
            log.error("Error occurred while performing index tasks", e);
            if (disableExitOnError) {
                throw e;
            }
        } finally {
            shutdownLogging();
        }

        if (!success) {
            //Needed for changes after OAK-6409
            System.exit(1);
        }
    }

    private void execute(NodeStoreFixture fixture, ElasticIndexOptions indexOpts, Closer closer)
            throws IOException, CommitFailedException {
        IndexHelper indexHelper = createIndexHelper(fixture, indexOpts, closer);

        // TODO : See if we need to support dumpIndexStats and index defs for elastic - not needed for now
        //dumpIndexStats(indexOpts, indexHelper);
        //dumpIndexDefinitions(indexOpts, indexHelper);
        reindexOperation(indexOpts, indexHelper);

        // For elastic implementation - this applies the newly created elastic definition to the repo and brings the index up to date with the
        // current state for async lane for this index.
        importIndexOperation(indexOpts, indexHelper);
    }

    private IndexHelper createIndexHelper(NodeStoreFixture fixture,
                                          ElasticIndexOptions indexOpts, Closer closer) throws IOException {
        IndexHelper indexHelper = new IndexHelper(fixture.getStore(), fixture.getBlobStore(), fixture.getWhiteboard(),
                indexOpts.getOutDir(), indexOpts.getWorkDir(), computeIndexPaths(indexOpts));

        // TODO : See if pre text extraction is needed for elastic
        //configurePreExtractionSupport(indexOpts, indexHelper);

        closer.register(indexHelper);
        return indexHelper;
    }

    private List<String> computeIndexPaths(ElasticIndexOptions indexOpts) throws IOException {
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

    private void importIndexOperation(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException, CommitFailedException {
        if (indexOpts.isImportIndex()) {
            File importDir = indexOpts.getIndexImportDir();
            importIndex(indexHelper, importDir);
        }
    }

    private void importIndex(IndexHelper indexHelper, File importDir) throws IOException, CommitFailedException {
        try (ElasticIndexImporterSupport elasticIndexImporterSupport = new ElasticIndexImporterSupport(indexHelper, indexOpts.getIndexPrefix(),
                indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret())) {
           elasticIndexImporterSupport.importIndex(importDir);
       }
    }

    private void reindexOperation(ElasticIndexOptions indexOpts, IndexHelper indexHelper) throws IOException, CommitFailedException {
        if (!indexOpts.isReindex()) {
            return;
        }
        String checkpoint = indexOpts.getCheckpoint();
        reindex(indexOpts, indexHelper, checkpoint);
    }

    private void reindex(ElasticIndexOptions indexOpts, IndexHelper indexHelper, String checkpoint) throws IOException, CommitFailedException {
        Objects.requireNonNull(checkpoint, "Checkpoint value is required for reindexing done in read only mode");

        Stopwatch w = Stopwatch.createStarted();
        IndexerSupport indexerSupport = createIndexerSupport(indexHelper, checkpoint);
        log.info("Proceeding to index {} upto checkpoint {} {}", indexHelper.getIndexPaths(), checkpoint,
                indexerSupport.getCheckpointInfo());

        List<String> indexNames = indexerSupport.getIndexDefinitions().stream().map(IndexDefinition::getIndexName).collect(Collectors.toList());
        indexHelper.getIndexReporter().setIndexNames(indexNames);

        if (opts.getCommonOpts().isMongo() && indexOpts.isDocTraversalMode()) {
            log.info("Using Document order traversal to perform reindexing");
            try (ElasticDocumentStoreIndexer indexer = new ElasticDocumentStoreIndexer(indexHelper, indexerSupport, indexOpts.getIndexPrefix(),
                    indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                    indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret())) {
                indexer.reindex();
            }
        } else {
            try (ElasticOutOfBandIndexer indexer = new ElasticOutOfBandIndexer(indexHelper, indexerSupport, indexOpts.getIndexPrefix(),
                    indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                    indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret())) {

                indexer.reindex();
            }
        }
        indexerSupport.writeMetaInfo(checkpoint);

        // This will copy the metadata files (consisting of the checkpoint info and indexes that have been re-indexed)
        // to the o/p directory. We need to do this because the working dir where they have been created would be cleaned up.
        // In case of lucene, even the index files are created here and copied as part of this, but for elastic - it's just metadata.
        File destDir = indexerSupport.copyIndexFilesToOutput();
        log.info("Indexing completed for indexes {} in {} ({} ms) and index metadata files are copied to {}",
                indexHelper.getIndexPaths(), w, w.elapsed(TimeUnit.MILLISECONDS), ElasticIndexCommand.getPath(destDir));
    }

    private IndexerSupport createIndexerSupport(IndexHelper indexHelper, String checkpoint) {
        IndexerSupport indexerSupport = new ElasticIndexerSupport(indexHelper, checkpoint);

        File definitions = indexOpts.getIndexDefinitionsFile();
        if (definitions != null) {
            Validate.checkArgument(definitions.exists(), "Index definitions file [%s] not found", getPath(definitions));
            indexerSupport.setIndexDefinitions(definitions);
        }
        return indexerSupport;
    }

    static Path getPath(File file) {
        return file.toPath().normalize().toAbsolutePath();
    }

    private void shutdownLogging() {
        LoggingInitializer.shutdownLogging();
    }

    private static void logCliArgs(String[] args) {
        log.info("Command line arguments used for indexing [{}]", String.join(" ", args));
        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        if (!inputArgs.isEmpty()) {
            log.info("System properties and vm options passed {}", inputArgs);
        }
    }

    public static void setDisableExitOnError(boolean disableExitOnError) {
        ElasticIndexCommand.disableExitOnError = disableExitOnError;
    }

    private static void setupLogging(IndexOptions indexOpts) throws IOException {
        new LoggingInitializer(indexOpts.getWorkDir(), LOG_SUFFIX).init();
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

    static void mergeWithConcurrentCheck(NodeStore nodeStore, NodeBuilder builder) throws CommitFailedException {
        CompositeHook hooks = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(CompositeEditorProvider.compose(singletonList(new ConflictValidatorProvider())))
        );
        nodeStore.merge(builder, hooks, createCommitInfo());
    }

    private static CommitInfo createCommitInfo() {
        Map<String, Object> info = Map.of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }

}
