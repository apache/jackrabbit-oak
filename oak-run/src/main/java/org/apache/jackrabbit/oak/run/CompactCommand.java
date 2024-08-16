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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCompact;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.CompactorType;
import org.apache.jackrabbit.oak.segment.aws.tool.AwsCompact;
import org.apache.jackrabbit.oak.segment.tool.Compact;

class CompactCommand implements Command {

    private static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> directoryArg = parser.nonOptions(
                "Path/URI to TAR/remote segment store (required)").ofType(String.class);
        OptionSpec<Boolean> mmapArg = parser.accepts("mmap",
                "Use memory mapped access if true, use file access if false. " +
                    "If not specified, memory mapped access is used on 64 bit " +
                    "systems and file access is used on 32 bit systems. For " +
                    "remote segment stores and on Windows, regular file access " +
                    "is always enforced and this option is ignored.")
                .withOptionalArg()
                .ofType(Boolean.class);
        OptionSpec<Void> forceArg = parser.accepts("force",
                "Force compaction and ignore a non matching segment store version. " +
                        "CAUTION: this will upgrade the segment store to the latest version, " +
                        "which is incompatible with older versions of Oak.");
        OptionSpec<Void> tailArg = parser.accepts("tail", "Use tail compaction instead of a full repository rewrite.");
        OptionSpec<String> compactor = parser.accepts("compactor",
                "Allow the user to control compactor type to be used. Valid choices are \"classic\", \"diff\", \"parallel\". " +
                        "While \"classic\" is slower, it might be more stable, due to lack of optimisations employed " +
                        "by the \"diff\" compactor which compacts the checkpoints on top of each other and \"parallel\" compactor, which splits " +
                        "the repository into smaller parts and compacts them concurrently. If not specified, \"parallel\" compactor is used.")
                .withRequiredArg().ofType(String.class);
        OptionSpec<Integer> nThreads = parser.accepts("threads", "Specify the number of threads used" +
                "for compaction. This is only applicable to the \"parallel\" compactor. Defaults to 1.")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(1);
        OptionSpec<String> targetPath = parser.accepts("target-path", "Path/URI to TAR/remote segment store where " +
                "resulting archives will be written")
                .withRequiredArg()
                .ofType(String.class);
        OptionSpec<String> persistentCachePath = parser.accepts("persistent-cache-path", "Path/URI to persistent cache where " +
                "resulting segments will be written")
                .withRequiredArg()
                .ofType(String.class);
        OptionSpec<Integer> persistentCacheSizeGb = parser.accepts("persistent-cache-size-gb", "Size in GB (defaults to 50 GB) for "
                + "the persistent disk cache")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(50);
        OptionSpec<Integer> garbageThresholdGb = parser.accepts("garbage-threshold-gb", "Minimum amount of garbage in GB (defaults to 0 GB) for "
                        + "compaction to run")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(0);
        OptionSpec<Integer> garbageThresholdPercentage = parser.accepts("garbage-threshold-percentage", "Minimum amount of garbage in percentage (defaults to 0%) for "
                        + "compaction to run")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(0);


        OptionSet options = parser.parse(args);

        String path = directoryArg.value(options);

        if (path == null) {
            System.err.println("Compact a file store. Usage: compact [path] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        int code;

        if (path.startsWith("az:")) {
            if (targetPath.value(options) == null) {
                System.err.println("A destination for the compacted Azure Segment Store needs to be specified");
                parser.printHelpOn(System.err);
                System.exit(-1);
            }

            AzureCompact.Builder azureBuilder = AzureCompact.builder()
                    .withPath(path)
                    .withTargetPath(targetPath.value(options))
                    .withForce(options.has(forceArg))
                    .withGCLogInterval(Long.getLong("compaction-progress-log", 150000))
                    .withConcurrency(nThreads.value(options));

            if (options.has(persistentCachePath)) {
                azureBuilder.withPersistentCachePath(persistentCachePath.value(options));
                azureBuilder.withPersistentCacheSizeGb(persistentCacheSizeGb.value(options));
            }

            if (options.has(garbageThresholdGb)) {
                azureBuilder.withGarbageThresholdGb(garbageThresholdGb.value(options));
            }

            if (options.has(garbageThresholdPercentage)) {
                azureBuilder.withGarbageThresholdPercentage(garbageThresholdPercentage.value(options));
            }

            if (options.has(tailArg)) {
                azureBuilder.withGCType(SegmentGCOptions.GCType.TAIL);
            }

            if (options.has(compactor)) {
                azureBuilder.withCompactorType(CompactorType.fromDescription(compactor.value(options)));
            }

            code = azureBuilder.build().run();
        } else if (path.startsWith("aws:")) {
            AwsCompact.Builder awsBuilder = AwsCompact.builder()
                    .withPath(path)
                    .withForce(options.has(forceArg))
                    .withSegmentCacheSize(Integer.getInteger("cache", 256))
                    .withGCLogInterval(Long.getLong("compaction-progress-log", 150000))
                    .withConcurrency(nThreads.value(options));

            if (options.has(tailArg)) {
                awsBuilder.withGCType(SegmentGCOptions.GCType.TAIL);
            }
            if (options.has(compactor)) {
                awsBuilder.withCompactorType(CompactorType.fromDescription(compactor.value(options)));
            }

            code = awsBuilder.build().run();
        } else {
            Compact.Builder tarBuilder = Compact.builder()
                    .withPath(new File(path))
                    .withForce(options.has(forceArg))
                    .withMmap(mmapArg.value(options))
                    .withOs(System.getProperty("os.name"))
                    .withSegmentCacheSize(Integer.getInteger("cache", 256))
                    .withGCLogInterval(Long.getLong("compaction-progress-log", 150000))
                    .withConcurrency(nThreads.value(options));

            if (options.has(tailArg)) {
                tarBuilder.withGCType(SegmentGCOptions.GCType.TAIL);
            }
            if (options.has(compactor)) {
                tarBuilder.withCompactorType(CompactorType.fromDescription(compactor.value(options)));
            }

            code = tarBuilder.build().run();
        }

        System.exit(code);
    }

}
