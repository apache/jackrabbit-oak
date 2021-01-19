package org.apache.jackrabbit.oak.index;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ElasticIndexCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(ElasticIndexCommand.class);
    private Options opts;
    private ElasticIndexOptions indexOpts;
    public static final String NAME = "index";

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
        //setupDirectories(indexOpts);
        //setupLogging(indexOpts);

        logCliArgs(args);

        boolean success = false;
        try {
            try (Closer closer = Closer.create()) {
                //configureCustomizer(opts, closer, true);
                NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
                closer.register(fixture);
                execute(fixture, indexOpts, closer);
                //tellReportPaths();
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

    private void execute(NodeStoreFixture fixture, IndexOptions indexOpts, Closer closer)
            throws IOException, CommitFailedException {
        IndexHelper indexHelper = createIndexHelper(fixture, indexOpts, closer);

        // TODO : See if we need to support dumpIndexStats and index defs for elastic - not needed for now
        //dumpIndexStats(indexOpts, indexHelper);
        //dumpIndexDefinitions(indexOpts, indexHelper);
        reindexOperation(indexOpts, indexHelper);
    }

    private IndexHelper createIndexHelper(NodeStoreFixture fixture,
                                          IndexOptions indexOpts, Closer closer) throws IOException {
        IndexHelper indexHelper = new IndexHelper(fixture.getStore(), fixture.getBlobStore(), fixture.getWhiteboard(),
                indexOpts.getOutDir(), indexOpts.getWorkDir(), computeIndexPaths(indexOpts));

        // TODO : See if pre text extraction is needed for elastic
        //configurePreExtractionSupport(indexOpts, indexHelper);

        closer.register(indexHelper);
        return indexHelper;
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

    private void reindexOperation(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException, CommitFailedException {
        if (!indexOpts.isReindex()) {
            return;
        }

        String checkpoint = indexOpts.getCheckpoint();
        reindex(indexOpts, indexHelper, checkpoint);
    }

    private void reindex(IndexOptions idxOpts, IndexHelper indexHelper, String checkpoint) throws IOException, CommitFailedException {
        Preconditions.checkNotNull(checkpoint, "Checkpoint value is required for reindexing done in read only mode");

        Stopwatch w = Stopwatch.createStarted();
        IndexerSupport indexerSupport = createIndexerSupport(indexHelper, checkpoint);
        log.info("Proceeding to index {} upto checkpoint {} {}", indexHelper.getIndexPaths(), checkpoint,
                indexerSupport.getCheckpointInfo());

        if (opts.getCommonOpts().isMongo() && idxOpts.isDocTraversalMode()) {
            log.info("Using Document order traversal to perform reindexing");
            try (ElasticDocumentStoreIndexer indexer = new ElasticDocumentStoreIndexer(indexHelper, indexerSupport, indexOpts.getIndexPrefix(),
                    indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                    indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret())) {
                indexer.reindex();
                // Wait for default flush interval before exiting the try block
                // to make sure the client is not closed before the last flush
                // TODO : See if this can be handled in a better manner
                Thread.sleep(ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT*2);
            } catch (InterruptedException e) {
                //
            }
        } else {
            try(ElasticOutOfBandIndexer indexer = new ElasticOutOfBandIndexer(indexHelper, indexerSupport, indexOpts.getIndexPrefix(),
                    indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                    indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret())) {

                indexer.reindex();
                // Wait for default flush interval before exiting the try block
                // to make sure the client is not closed before the last flush
                Thread.sleep(ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT*2);
            } catch (InterruptedException e) {
                //
            }
        }
        indexerSupport.writeMetaInfo(checkpoint);
    }

    private IndexerSupport createIndexerSupport(IndexHelper indexHelper, String checkpoint) {
        IndexerSupport indexerSupport = new IndexerSupport(indexHelper, checkpoint);

        File definitions = indexOpts.getIndexDefinitionsFile();
        if (definitions != null) {
            Preconditions.checkArgument(definitions.exists(), "Index definitions file [%s] not found", getPath(definitions));
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
        log.info("Command line arguments used for indexing [{}]", Joiner.on(' ').join(args));
        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        if (!inputArgs.isEmpty()) {
            log.info("System properties and vm options passed {}", inputArgs);
        }
    }

    public static void setDisableExitOnError(boolean disableExitOnError) {
        ElasticIndexCommand.disableExitOnError = disableExitOnError;
    }

}
