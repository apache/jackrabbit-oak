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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashSet;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.tool.Check;

class CheckCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> journal = parser.accepts("journal", "journal file")
            .withRequiredArg()
            .ofType(File.class);
        OptionSpec<Long> notify = parser.accepts("notify", "number of seconds between progress notifications")
            .withRequiredArg()
            .ofType(Long.class)
            .defaultsTo(Long.MAX_VALUE);
        OptionSpec<?> bin = parser.accepts("bin", "read the content of binary properties");
        OptionSpec<String> filter = parser.accepts("filter", "comma separated content paths to be checked")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .defaultsTo("/");
        OptionSpec<?> head = parser.accepts("head", "checks only latest /root (i.e without checkpoints)");
        OptionSpec<String> cp = parser.accepts("checkpoints", "checks only specified checkpoints (comma separated); use --checkpoints all to check all checkpoints")
            .withOptionalArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .defaultsTo("all");
        OptionSpec<?> ioStatistics = parser.accepts("io-stats", "Print I/O statistics (only for oak-segment-tar)");
        OptionSpec<File> dir = parser.nonOptions()
            .describedAs("path")
            .ofType(File.class);
        OptionSet options = parser.parse(args);

        if (options.valuesOf(dir).isEmpty()) {
            printUsageAndExit(parser, "Segment Store path not specified");
        }

        if (options.valuesOf(dir).size() > 1) {
            printUsageAndExit(parser, "Too many Segment Store paths specified");
        }

        Check.Builder builder = Check.builder()
            .withPath(options.valueOf(dir))
            .withDebugInterval(notify.value(options))
            .withCheckBinaries(options.has(bin))
            .withCheckHead(shouldCheckHead(options, head, cp))
            .withCheckpoints(toCheckpointsSet(options, head, cp))
            .withFilterPaths(toSet(options, filter))
            .withIOStatistics(options.has(ioStatistics))
            .withOutWriter(new PrintWriter(System.out, true))
            .withErrWriter(new PrintWriter(System.err, true));

        if (options.has(journal)) {
            builder.withJournal(journal.value(options));
        }

        System.exit(builder.build().run());
    }

    private void printUsageAndExit(OptionParser parser, String... messages) throws IOException {
        for (String message : messages) {
            System.err.println(message);
        }
        System.err.println("usage: check path/to/segmentstore <options>");
        parser.printHelpOn(System.err);
        System.exit(1);
    }

    private static Set<String> toSet(OptionSet options, OptionSpec<String> option) {
        return new LinkedHashSet<>(option.values(options));
    }

    private static Set<String> toCheckpointsSet(OptionSet options, OptionSpec<?> head, OptionSpec<String> cp) {
        Set<String> checkpoints = new LinkedHashSet<>();
        if (options.has(cp) || !options.has(head)) {
            checkpoints.addAll(cp.values(options));
        }
        return checkpoints;
    }

    private static boolean shouldCheckHead(OptionSet options, OptionSpec<?> head, OptionSpec<String> cp) {
        return !options.has(cp) || options.has(head);
    }

}