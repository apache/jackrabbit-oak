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
package org.apache.jackrabbit.oak.upgrade.cli;

import joptsimple.OptionSet;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.UUIDConflictDetector;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.DatastoreArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationCliArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ServiceLoader;

public class OakUpgrade {

    private static final Logger log = LoggerFactory.getLogger(OakUpgrade.class);

    public static void main(String... args) throws IOException, InterruptedException {
        Thread.sleep(10000);
        OptionSet options = OptionParserFactory.create().parse(args);
        try {
            MigrationCliArguments cliArguments = new MigrationCliArguments(options);
            if (cliArguments.hasOption(OptionParserFactory.HELP) || cliArguments.getArguments().isEmpty()) {
                CliUtils.displayUsage();
                return;
            }

            if (cliArguments.hasOption("check-uuid-conflicts")) {
                checkUUIDConflicts(cliArguments);
                return;
            }

            migrate(cliArguments);
        } catch(CliArgumentException e) {
            if (e.getMessage() != null) {
                System.err.println(e.getMessage());
            }
            System.exit(e.getExitCode());
        }
    }

    public static void migrate(MigrationCliArguments argumentParser) throws IOException, CliArgumentException {
        MigrationOptions options = new MigrationOptions(argumentParser);
        options.logOptions();

        StoreArguments stores = new StoreArguments(options, argumentParser.getArguments());
        stores.logOptions();

        boolean srcEmbedded = stores.srcUsesEmbeddedDatastore();
        DatastoreArguments datastores = new DatastoreArguments(options, stores, srcEmbedded);

        migrate(options, stores, datastores);
    }

    public static void migrate(MigrationOptions options, StoreArguments stores, DatastoreArguments datastores) throws IOException, CliArgumentException {
        Closer closer = Closer.create();
        CliUtils.handleSigInt(closer);
        MigrationFactory factory = new MigrationFactory(options, stores, datastores, closer);
        try {
            if (stores.getSrcStore().isJcr2()) {
                upgrade(factory);
            } else {
                sidegrade(factory);
            }
        } catch (Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
    }

    private static void upgrade(MigrationFactory migrationFactory) throws IOException, RepositoryException, CliArgumentException {
        migrationFactory.createUpgrade().copy(createCompositeInitializer());
    }

    private static void sidegrade(MigrationFactory migrationFactory) throws IOException, RepositoryException, CliArgumentException {
        migrationFactory.createSidegrade().copy();
    }

    private static RepositoryInitializer createCompositeInitializer() {
        ServiceLoader<RepositoryInitializer> loader = ServiceLoader.load(RepositoryInitializer.class);
        List<RepositoryInitializer> initializers = Lists.newArrayList(loader.iterator());
        return new CompositeInitializer(initializers);
    }

    private static void checkUUIDConflicts(MigrationCliArguments argumentParser) throws CliArgumentException, IOException {
        MigrationOptions migrationOptions = new MigrationOptions(argumentParser);
        migrationOptions.logOptions();

        StoreArguments stores = new StoreArguments(migrationOptions, argumentParser.getArguments());
        stores.logOptions();

        boolean srcEmbedded = stores.srcUsesEmbeddedDatastore();
        DatastoreArguments datastores = new DatastoreArguments(migrationOptions, stores, srcEmbedded);

        Closer closer = Closer.create();
        CliUtils.handleSigInt(closer);

        try {
            BlobStore srcBlobStore = datastores.getSrcBlobStore().create(closer);
            NodeStore sourceNodeStore = stores.getSrcStore().create(srcBlobStore, closer);

            BlobStore dstBlobStore = datastores.getDstBlobStore(srcBlobStore).create(closer);
            NodeStore targetNodeStore = stores.getDstStore().create(dstBlobStore, closer);

            UUIDConflictDetector uuidConflictDetector = new UUIDConflictDetector(sourceNodeStore, targetNodeStore, new File("/tmp"));
            uuidConflictDetector.detectConflicts(migrationOptions.getIncludePaths());

        } catch (Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
    }
}