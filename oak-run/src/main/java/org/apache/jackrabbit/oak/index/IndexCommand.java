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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.google.common.io.Closer;
import joptsimple.OptionParser;
import org.apache.commons.io.FileUtils;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(IndexCommand.class);
    public static final String NAME = "index";
    public static final String INDEX_DEFINITIONS_JSON = "index-definitions.json";
    public static final String INDEX_INFO_TXT = "index-info.txt";
    public static final String INDEX_CONSISTENCY_CHECK_TXT = "index-consistency-check-report.txt";

    private final String summary = "Provides index management related operations";

    private File info;
    private File definitions;
    private File consistencyCheckReport;
    private Options opts;

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(IndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        IndexOptions indexOpts = opts.getOptionBean(IndexOptions.class);

        //Clean up before setting up NodeStore as the temp
        //directory might be used by NodeStore for cache stuff like persistentCache
        setupDirectories(indexOpts);

        NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts);
        try (Closer closer = Closer.create()) {
            closer.register(fixture);
            StatisticsProvider statisticsProvider = WhiteboardUtils.getService(fixture.getWhiteboard(), StatisticsProvider.class);
            execute(fixture.getStore(), fixture.getBlobStore(), statisticsProvider, indexOpts, closer);
            tellReportPaths();
        }
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

    private void execute(NodeStore store, BlobStore blobStore, StatisticsProvider statisticsProvider,
                         IndexOptions indexOpts, Closer closer) throws IOException, CommitFailedException {
        IndexHelper indexHelper = new IndexHelper(store, blobStore, statisticsProvider, indexOpts.getOutDir(),
                indexOpts.getWorkDir(), indexOpts.getIndexPaths());

        configurePreExtractionSupport(indexOpts, indexHelper);

        closer.register(indexHelper);

        dumpIndexStats(indexOpts, indexHelper);
        dumpIndexDefinitions(indexOpts, indexHelper);
        performConsistencyCheck(indexOpts, indexHelper);
        dumpIndexContents(indexOpts, indexHelper);
        reindexIndex(indexOpts, indexHelper);
        importIndex(indexOpts, indexHelper);
    }

    private void configurePreExtractionSupport(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException {
        File preExtractedTextDir = indexOpts.getPreExtractedTextDir();
        if (preExtractedTextDir != null) {
            indexHelper.setPreExtractedTextDir(preExtractedTextDir);
            log.info("Using pre-extracted text directory {}", getPath(preExtractedTextDir));
        }
    }

    private void reindexIndex(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException, CommitFailedException {
        if (!indexOpts.isReindex()){
            return;
        }

        if (opts.getCommonOpts().isReadWrite()) {
            new ReIndexer(indexHelper).reindex();
        } else {
            String checkpoint = indexOpts.getCheckpoint();
            checkNotNull(checkpoint, "Checkpoint value is required for reindexing done in read only mode");
            try (OutOfBandIndexer indexer = new OutOfBandIndexer(indexHelper, checkpoint)) {
                indexer.reindex();
            }
        }
    }

    private void importIndex(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException, CommitFailedException {
        if (indexOpts.isImportIndex()) {
            File importDir = indexOpts.getIndexImportDir();
            new IndexImporterSupport(indexHelper).importIndex(importDir);
        }
    }

    private void dumpIndexContents(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException {
        if (indexOpts.dumpIndex()) {
            new IndexDumper(indexHelper, indexOpts.getOutDir()).dump();
        }
    }

    private void performConsistencyCheck(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException {
        if (indexOpts.checkConsistency()) {
            IndexConsistencyCheckPrinter printer =
                    new IndexConsistencyCheckPrinter(indexHelper, indexOpts.consistencyCheckLevel());
            PrinterDumper dumper = new PrinterDumper(indexHelper.getOutputDir(), INDEX_CONSISTENCY_CHECK_TXT,
                    false, Format.TEXT, printer);
            dumper.dump();
            consistencyCheckReport = dumper.getOutFile();
        }
    }

    private void dumpIndexDefinitions(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException {
        if (indexOpts.dumpDefinitions()) {
            PrinterDumper dumper = new PrinterDumper(indexHelper.getOutputDir(), INDEX_DEFINITIONS_JSON,
                    false, Format.JSON, indexHelper.getIndexDefnPrinter());
            dumper.dump();
            definitions = dumper.getOutFile();
        }
    }

    private void dumpIndexStats(IndexOptions indexOpts, IndexHelper indexHelper) throws IOException {
        if (indexOpts.dumpStats()) {
            PrinterDumper dumper = new PrinterDumper(indexHelper.getOutputDir(), INDEX_INFO_TXT,
                    true, Format.TEXT, indexHelper.getIndexPrinter());
            dumper.dump();
            info = dumper.getOutFile();
        }
    }

    private static void setupDirectories(IndexOptions indexOpts) throws IOException {
        if (indexOpts.getOutDir().exists()) {
            FileUtils.cleanDirectory(indexOpts.getOutDir());
        }
        cleanWorkDir(indexOpts.getWorkDir());
    }

    private static void cleanWorkDir(File workDir) throws IOException {
        //TODO Do not clean if restarting
        String[] dirListing = workDir.list();
        if (dirListing != null && dirListing.length != 0) {
            log.info("Cleaning existing work directory {}", workDir.getAbsolutePath());
            FileUtils.cleanDirectory(workDir);
        }
    }


    static Path getPath(File file) {
        return file.toPath().normalize().toAbsolutePath();
    }
}
