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

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.isValidFileStoreOrFail;

import java.io.File;
import java.io.IOException;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

class CheckCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> journal = parser.accepts(
                "journal", "journal file")
                .withRequiredArg().ofType(String.class).defaultsTo("journal.log");
        OptionSpec deep = parser.accepts(
                "deep", "<deprecated> enable deep consistency checking. ");
        ArgumentAcceptingOptionSpec<Long> notify = parser.accepts(
                "notify", "number of seconds between progress notifications")
                .withRequiredArg().ofType(Long.class).defaultsTo(Long.MAX_VALUE);
        ArgumentAcceptingOptionSpec<Long> bin = parser.accepts(
                "bin", "read the first n bytes from binary properties.")
                .withRequiredArg().ofType(Long.class);
        OptionSpec segment = parser.accepts("segment", "Use oak-segment instead of oak-segment-tar");
        OptionSpec ioStatistics = parser.accepts("io-stats", "Print I/O statistics (only for oak-segment-tar)");

        OptionSet options = parser.parse(args);

        if (options.nonOptionArguments().size() != 1) {
            printUsage(parser);
        }

        File dir = isValidFileStoreOrFail(new File(options.nonOptionArguments().get(0).toString()));
        String journalFileName = journal.value(options);
        long debugLevel = notify.value(options);

        long binLen = -1L;
        
        if (options.has(bin)) {
            binLen = bin.value(options);

            if (binLen < 0) {
                printUsage(parser, "The value for --bin option must be a positive number!");
            }
        }

        if (options.has(deep)) {
            printUsage(parser, "The --deep option was deprecated! Please do not use it in the future!"
                    , "A deep scan of the content tree, traversing every node, will be performed by default.");
        }
        
        if (options.has(segment)) {
            SegmentUtils.check(dir, journalFileName, true, debugLevel, binLen);
        } else {
            SegmentTarUtils.check(dir, journalFileName, true, debugLevel, binLen, options.has(ioStatistics));
        }
    }

    private void printUsage(OptionParser parser, String... messages) throws IOException {
        for (String message : messages) {
            System.err.println(message);
        }
        
        System.err.println("usage: check path/to/segmentstore <options>");
        parser.printHelpOn(System.err);
        System.exit(1);
    }

}
