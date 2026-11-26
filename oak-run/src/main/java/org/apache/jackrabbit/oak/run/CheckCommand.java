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
import java.util.Locale;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCheck;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCheck.PersistentCacheType;
import org.apache.jackrabbit.oak.segment.tool.Check;

class CheckCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        System.exit(run(args));
    }

    int run(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<Boolean> mmapArg = parser.accepts("mmap", "use memory mapping for the file store (default: true)")
            .withOptionalArg()
            .ofType(Boolean.class)
            .defaultsTo(true);
        OptionSpec<String> journal = parser.accepts("journal", "journal file")
            .withRequiredArg()
            .ofType(String.class);
        OptionSpec<Long> notify = parser.accepts("notify", "number of seconds between progress notifications")
            .withRequiredArg()
            .ofType(Long.class)
            .defaultsTo(Long.MAX_VALUE);
        OptionSpec<Integer> last = parser.accepts("last", "define the number of revisions to be checked (default: 1)")
                .withOptionalArg()
                .ofType(Integer.class);
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
        OptionSpec<String> dir = parser.nonOptions()
            .describedAs("Path/URI to TAR/remote segment store (required)")
            .ofType(String.class);
        OptionSpec<Boolean> failFast = parser.accepts("fail-fast", "eagerly fail if first path/revision checked is inconsistent (default: false)")
                .withOptionalArg()
                .ofType(Boolean.class)
                .defaultsTo(false);
        OptionSpec<?> continueOnError = parser.accepts("continue-on-error", "report all inconsistent nodes of a revision instead of stopping at the first one "
                + "(default: false); best combined with --head and --last 1");
        OptionSpec<String> persistentCachePath = parser.accepts("persistent-cache-path", "Path/URI to persistent cache where " +
                        "resulting segments will be written")
                .withRequiredArg()
                .ofType(String.class);
        OptionSpec<Integer> persistentCacheSizeGb = parser.accepts("persistent-cache-size-gb", "Size in GB (defaults to 50 GB) for "
                        + "the persistent disk cache")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(50);
        OptionSpec<String> persistentCacheType = parser.accepts("persistent-cache-type",
                        "Azure disk cache type: legacy (default), sync or async")
                .withRequiredArg()
                .defaultsTo("legacy");
        OptionSet options = parser.parse(args);

        if (options.valuesOf(dir).isEmpty()) {
            return usageError(parser, "Segment Store path not specified");
        }

        if (options.valuesOf(dir).size() > 1) {
            return usageError(parser, "Too many Segment Store paths specified");
        }

        boolean azure = options.valueOf(dir).startsWith("az:");
        if (options.has(persistentCacheType) && (!azure || !options.has(persistentCachePath))) {
            return usageError(parser, "--persistent-cache-type requires an Azure store and --persistent-cache-path");
        }

        if (azure) {
            AzureCheck.Builder builder = AzureCheck.builder()
                    .withPath(options.valueOf(dir))
                    .withDebugInterval(notify.value(options))
                    .withCheckBinaries(options.has(bin))
                    .withCheckHead(shouldCheckHead(options, head, cp))
                    .withCheckpoints(toCheckpointsSet(options, head, cp))
                    .withFilterPaths(toSet(options, filter))
                    .withIOStatistics(options.has(ioStatistics))
                    .withOutWriter(new PrintWriter(System.out, true))
                    .withErrWriter(new PrintWriter(System.err, true))
                    .withFailFast(failFast.value(options))
                    .withContinueOnError(options.has(continueOnError));

            if (options.has(last)) {
                builder.withRevisionsCount(options.valueOf(last) != null ? last.value(options) : 1);
            }

            if (options.has(persistentCachePath)) {
                PersistentCacheType cacheType;
                try {
                    cacheType = PersistentCacheType.valueOf(persistentCacheType.value(options).toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    return usageError(parser, "--persistent-cache-type must be legacy, sync or async");
                }
                builder.withPersistentCachePath(persistentCachePath.value(options));
                builder.withPersistentCacheSizeGb(persistentCacheSizeGb.value(options));
                builder.withPersistentCacheType(cacheType);
            }

            return builder.build().run();
        } else {
            Check.Builder builder = Check.builder()
                    .withPath(new File(options.valueOf(dir)))
                    .withMmap(mmapArg.value(options))
                    .withDebugInterval(notify.value(options))
                    .withCheckBinaries(options.has(bin))
                    .withCheckHead(shouldCheckHead(options, head, cp))
                    .withCheckpoints(toCheckpointsSet(options, head, cp))
                    .withFilterPaths(toSet(options, filter))
                    .withIOStatistics(options.has(ioStatistics))
                    .withOutWriter(new PrintWriter(System.out, true))
                    .withErrWriter(new PrintWriter(System.err, true))
                    .withFailFast(failFast.value(options))
                    .withContinueOnError(options.has(continueOnError));

            if (options.has(journal)) {
                builder.withJournal(new File(journal.value(options)));
            }

            if (options.has(last)) {
                builder.withRevisionsCount(options.valueOf(last) != null ? last.value(options) : 1);
            }

            return builder.build().run();
        }
    }

    private int usageError(OptionParser parser, String message) throws IOException {
        System.err.println(message);
        System.err.println("usage: check path/to/segmentstore <options>");
        parser.printHelpOn(System.err);
        return 1;
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
