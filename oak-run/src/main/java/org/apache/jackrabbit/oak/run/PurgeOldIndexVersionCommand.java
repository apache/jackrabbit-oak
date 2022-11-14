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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.indexversion.PurgeOldIndexVersion;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class PurgeOldIndexVersionCommand implements Command {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeOldIndexVersionCommand.class);

    private long threshold;
    private List<String> indexPaths;
    private long DEFAULT_PURGE_THRESHOLD = TimeUnit.DAYS.toMillis(5); // 5 days in millis
    private final static String DEFAULT_INDEX_PATH = "/oak:index";
    private boolean shouldPurgeBaseIndex;

    @Override
    public void execute(String... args) throws Exception {
        Options opts = parseCommandLineParams(args);
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(opts)) {
            if (!opts.getCommonOpts().isReadWrite()) {
                LOG.info("Repository connected in read-only mode. Use '--read-write' for write operations");
            }
            new PurgeOldIndexVersion().execute(fixture.getStore(), opts.getCommonOpts().isReadWrite(), threshold, indexPaths, shouldPurgeBaseIndex);
        }
    }

    private Options parseCommandLineParams(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Long> thresholdOption = parser.accepts("threshold")
                .withOptionalArg().ofType(Long.class).defaultsTo(DEFAULT_PURGE_THRESHOLD);
        OptionSpec<String> indexPathsOption = parser.accepts("index-paths", "Comma separated list of index paths for which the " +
                "selected operations need to be performed")
                .withOptionalArg().ofType(String.class).withValuesSeparatedBy(",").defaultsTo(DEFAULT_INDEX_PATH);
        OptionSpec<Void> donotPurgeBaseIndexOption = parser.accepts("donot-purge-base-index", "Don't disable base index");

        Options opts = new Options();
        OptionSet optionSet = opts.parseAndConfigure(parser, args);
        this.threshold = optionSet.valueOf(thresholdOption);
        this.indexPaths = optionSet.valuesOf(indexPathsOption);
        this.shouldPurgeBaseIndex = !optionSet.has(donotPurgeBaseIndexOption);
        return opts;
    }
}
