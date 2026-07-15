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
package org.apache.jackrabbit.oak.index.merge;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsBuilder;
import org.apache.jackrabbit.oak.run.commons.Command;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class IndexStoreStatsCommand implements Command {
    
    public final static String NAME = "index-store-stats";

    @Override
    public void execute(String... args) throws IOException {
        OptionParser parser = new OptionParser();
        OptionSpec<String> nodeNameFilterOption = parser
                .accepts("nodeNameFilter", "The node name to filter, if any").withOptionalArg()
                .defaultsTo("");
        OptionSpec<Boolean> profilerOption = parser
                .accepts("profiler", "Use the profiler (default: disabled)").withOptionalArg()
                .ofType(Boolean.class).defaultsTo(false);
        OptionSpec<?> helpSpec = parser.acceptsAll(
                asList("h", "?", "help"), "Prints help and exits").forHelp();
        OptionSet options = parser.parse(args);
        parser.nonOptions(
                "An index store file").ofType(File.class);
        
        if (options.has(helpSpec)
                || options.nonOptionArguments().isEmpty()) {
            System.out.println("Mode: " + NAME);
            System.out.println("Calculate statistics (node count, binary size,...) of a tree store");
            System.out.println();
            parser.printHelpOn(System.out);
            return;
        }
        String nodeNameFilter = nodeNameFilterOption.value(options);
        boolean profiler = profilerOption.value(options);
        
        if (options.nonOptionArguments().size() < 1) {
            System.err.println("This command requires a file name");
            System.exit(1);
        }
        File file = new File(options.nonOptionArguments().get(0).toString());
        if (!file.exists()) {
            System.out.println("File not found: " + file.getAbsolutePath());
            return;
        }
        StatsBuilder.buildStats(file.getAbsolutePath(), nodeNameFilter, profiler);
    }

}
