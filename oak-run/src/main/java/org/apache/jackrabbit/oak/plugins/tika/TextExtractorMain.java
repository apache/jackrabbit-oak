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

package org.apache.jackrabbit.oak.plugins.tika;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.File;

import org.apache.jackrabbit.oak.plugins.index.datastore.DataStoreTextWriter;
import org.apache.jackrabbit.oak.run.cli.BlobStoreFixture;
import org.apache.jackrabbit.oak.run.cli.BlobStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.io.Closer;

import joptsimple.OptionParser;

public class TextExtractorMain {
    private static final Logger log = LoggerFactory.getLogger(TextExtractorMain.class);

    private TextExtractorMain() {
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();

        Options opts = new Options();
        opts.setCommandName(TikaCommandOptions.NAME);
        opts.setSummary("Provides text extraction related operations");
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(TikaCommandOptions.FACTORY);

        //NodeStore is only required for generate command. So make it optional
        opts.parseAndConfigure(parser, args, false);

        TikaCommandOptions tikaOpts = opts.getOptionBean(TikaCommandOptions.class);

        //If generate then check that NodeStore is specified
        if (tikaOpts.generate()) {
            opts.checkNonOptions();
        }

        try (Closer closer = Closer.create()) {
            boolean report = tikaOpts.report();
            boolean extract = tikaOpts.extract();
            boolean populate = tikaOpts.populate();
            boolean generate = tikaOpts.generate();
            BlobStore blobStore = null;
            NodeStore nodeStore = null;
            File dataFile = tikaOpts.getDataFile();
            File indexDir = tikaOpts.getIndexDir();
            File storeDir = tikaOpts.getStoreDir();
            File tikaConfigFile = tikaOpts.getTikaConfig();
            BinaryResourceProvider binaryResourceProvider = null;
            BinaryStats stats = null;
            String path = tikaOpts.getPath();

            if (tikaConfigFile != null) {
                checkArgument(tikaConfigFile.exists(), "Tika config file %s does not exist",
                        tikaConfigFile.getAbsolutePath());
            }

            if (storeDir != null) {
                if (storeDir.exists()) {
                    checkArgument(storeDir.isDirectory(), "Path [%s] specified for storing extracted " +
                            "text content is not a directory", storeDir.getAbsolutePath());
                }
            }

            requireNonNull(dataFile, String.format("Data file not configured with %s", tikaOpts.getDataFileSpecOpt()));

            if (report || extract) {
                //For report and extract case we do not need NodeStore access so create BlobStore directly
                BlobStoreFixture blobStoreFixture = BlobStoreFixtureProvider.create(opts);
                closer.register(blobStoreFixture);
                blobStore = requireNonNull(blobStoreFixture).getBlobStore();
            } else if (generate) {
                NodeStoreFixture nodeStoreFixture = NodeStoreFixtureProvider.create(opts);
                closer.register(nodeStoreFixture);
                blobStore = nodeStoreFixture.getBlobStore();
                nodeStore = nodeStoreFixture.getStore();
            }

            if (!populate) {
                requireNonNull(blobStore, "This command requires an external BlobStore configured");
            }

            // NOTE: The order of executing generate, populate and extract is correct in case the user
            // calls the tool with multiple actions in same run.

            if (generate){
                requireNonNull(dataFile, "Data file path not provided");
                log.info("Generated csv data to be stored in {}", dataFile.getAbsolutePath());
                BinaryResourceProvider brp = new NodeStoreBinaryResourceProvider(nodeStore, blobStore);
                CSVFileGenerator generator = new CSVFileGenerator(dataFile);
                generator.generate(brp.getBinaries(path));
            }

            if (populate) {
                checkArgument(dataFile.exists(),
                        "Data file %s does not exist", dataFile.getAbsolutePath());
                requireNonNull(indexDir, String.format("Lucene index directory " +
                        "must be specified via %s", tikaOpts.getIndexDirSpecOpt()));
                requireNonNull(storeDir, String.format("Directory to store extracted text content " +
                        "must be specified via %s", tikaOpts.getStoreDirSpecOpt()));

                DataStoreTextWriter writer = closer.register(new DataStoreTextWriter(storeDir, false));

                TextPopulator textPopulator = new TextPopulator(writer);
                textPopulator.populate(dataFile, indexDir);
            }

            if (report || extract) {
                checkArgument(dataFile.exists(),
                        "Data file %s does not exist", dataFile.getAbsolutePath());

                CSVFileBinaryResourceProvider csvProvider = new CSVFileBinaryResourceProvider(dataFile, blobStore);
                closer.register(csvProvider);
                binaryResourceProvider = csvProvider;

                stats = new BinaryStats(tikaConfigFile, binaryResourceProvider);
                String summary = stats.getSummary();
                log.info(summary);
            }

            if (extract) {
                requireNonNull(storeDir, String.format("Directory to store extracted text content " +
                        "must be specified via %s", tikaOpts.getStoreDirSpecOpt()));
                requireNonNull(blobStore, "BlobStore found to be null.");

                DataStoreTextWriter writer = new DataStoreTextWriter(storeDir, false);
                TextExtractor extractor = new TextExtractor(writer);

                if (tikaOpts.isPoolSizeDefined()) {
                    extractor.setThreadPoolSize(tikaOpts.getPoolSize());
                }

                if (tikaConfigFile != null) {
                    extractor.setTikaConfig(tikaConfigFile);
                }

                closer.register(writer);
                closer.register(extractor);

                extractor.setStats(stats);
                log.info("Using path {}", path);
                extractor.extract(binaryResourceProvider.getBinaries(path));

                extractor.close();
                writer.close();
            }
        }
    }
}