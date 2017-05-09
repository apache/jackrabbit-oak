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

import static org.apache.jackrabbit.oak.segment.FileStoreHelper.isValidFileStoreOrFail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashSet;
import java.util.Set;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;

class CheckCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> journal = parser.accepts(
                "journal", "journal file")
                .withRequiredArg().ofType(String.class).defaultsTo("journal.log");
        OptionSpec<?> deep = parser.accepts(
                "deep", "<deprecated> enable deep consistency checking.");
        ArgumentAcceptingOptionSpec<Long> notify = parser.accepts(
                "notify", "number of seconds between progress notifications")
                .withRequiredArg().ofType(Long.class).defaultsTo(Long.MAX_VALUE);
        OptionSpec<?> bin = parser.accepts("bin", "read the content of binary properties");
        ArgumentAcceptingOptionSpec<String> filter = parser.accepts(
                "filter", "comma separated content paths to be checked")
                .withRequiredArg().ofType(String.class).withValuesSeparatedBy(',').defaultsTo("/");
        OptionSpec<?> ioStatistics = parser.accepts("io-stats", "Print I/O statistics (only for oak-segment-tar)");

        OptionSet options = parser.parse(args);
        
        PrintWriter out = new PrintWriter(System.out, true);
        PrintWriter err = new PrintWriter(System.err, true);

        if (options.nonOptionArguments().size() != 1) {
            printUsage(parser, err);
        }

        File dir = isValidFileStoreOrFail(new File(options.nonOptionArguments().get(0).toString()));
        String journalFileName = journal.value(options);
        long debugLevel = notify.value(options);
        Set<String> filterPaths = new LinkedHashSet<String>(filter.values(options));

        if (options.has(deep)) {
            printUsage(parser, err, "The --deep option was deprecated! Please do not use it in the future!"
                    , "A deep scan of the content tree, traversing every node, will be performed by default.");
        }
        
        SegmentTarUtils.check(dir, journalFileName, debugLevel, options.has(bin), filterPaths, options.has(ioStatistics), out, err);
    }

    private void printUsage(OptionParser parser, PrintWriter err, String... messages) throws IOException {
        for (String message : messages) {
            err.println(message);
        }
        
        err.println("usage: check path/to/segmentstore <options>");
        parser.printHelpOn(err);
        System.exit(1);
    }

}