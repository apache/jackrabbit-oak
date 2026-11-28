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

import java.io.IOException;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.jackrabbit.oak.commons.pio.Closer;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.DatastoreArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationCliArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.OptionParserFactory;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;

public class OakUpgrade {

    private static final List<String> RETIRED_CLASSIC_OPTIONS = List.of(
            "early-shutdown", "fail-on-error", "skip-init", "skip-name-check");

    private static final String CLASSIC_UPGRADE_JAR_URL = "https://repo.maven.apache.org/maven2/org/apache/jackrabbit/"
            + "oak-upgrade/2.8.0/oak-upgrade-2.8.0.jar";

    public static void main(String... args) throws IOException {
        try {
            MigrationCliArguments cliArguments = new MigrationCliArguments(parseOptions(args));
            if (cliArguments.hasOption(OptionParserFactory.HELP) || cliArguments.getArguments().isEmpty()) {
                CliUtils.displayUsage();
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
            factory.createSidegrade().copy();
        } catch (Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
    }

    /** Parse retired flags as no-ops without mistaking option values or paths for flags. */
    private static OptionSet parseOptions(String... args) {
        OptionParser parser = OptionParserFactory.create();
        RETIRED_CLASSIC_OPTIONS.forEach(parser::accepts);
        OptionSet options = parser.parse(args);
        List<String> ignored = RETIRED_CLASSIC_OPTIONS.stream()
                .filter(options::has)
                .map(option -> "--" + option)
                .toList();
        if (!ignored.isEmpty()) {
            System.err.printf("Warning: ignoring retired Jackrabbit Classic options: %s.%n"
                    + "Remove them from Oak-to-Oak sidegrade commands. For Jackrabbit Classic migration, "
                    + "download oak-upgrade from Oak 2.8.0: %s%n", String.join(", ", ignored), CLASSIC_UPGRADE_JAR_URL);
        }
        return options;
    }

}