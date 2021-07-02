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

import com.google.common.base.StandardSystemProperty;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCompact;
import org.apache.jackrabbit.oak.segment.azure.tool.AzureCompact.Builder;
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
        OptionSpec<Boolean> forceArg = parser.accepts("force",
                "Force compaction and ignore a non matching segment store version. " +
                        "CAUTION: this will upgrade the segment store to the latest version, " +
                        "which is incompatible with older versions of Oak.")
                .withOptionalArg()
                .ofType(Boolean.class);
        OptionSpec<String> compactor = parser.accepts("compactor",
                "Allow the user to control compactor type to be used. Valid choices are \"classic\" and \"diff\". " +
                        "While the former is slower, it might be more stable, due to lack of optimisations employed " +
                        "by the \"diff\" compactor which compacts the checkpoints on top of each other. If not " +
                        "specified, \"diff\" compactor is used.")
                .withRequiredArg().ofType(String.class);
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
                .defaultsTo("50")
                .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        String path = directoryArg.value(options);

        if (path == null) {
            System.err.println("Compact a file store. Usage: compact [path] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        int code = 0;

        if (path.startsWith("az:")) {
            if (targetPath.value(options) == null) {
                System.err.println("A destination for the compacted Azure Segment Store needs to be specified");
                parser.printHelpOn(System.err);
                System.exit(-1);
            }

            if (persistentCachePath.value(options) == null) {
                System.err.println("A path for the persistent disk cache needs to be specified");
                parser.printHelpOn(System.err);
                System.exit(-1);
            }

            Builder azureBuilder = AzureCompact.builder()
                    .withPath(path)
                    .withTargetPath(targetPath.value(options))
                    .withPersistentCachePath(persistentCachePath.value(options))
                    .withPersistentCacheSizeGb(persistentCacheSizeGb.value(options))
                    .withForce(isTrue(forceArg.value(options)))
                    .withGCLogInterval(Long.getLong("compaction-progress-log", 150000));

            if (options.has(compactor)) {
                azureBuilder.withCompactorType(CompactorType.fromDescription(compactor.value(options)));
            }

            code = azureBuilder
                    .build()
                    .run();
        } else if (path.startsWith("aws:")) {
            code = AwsCompact.builder()
                    .withPath(path)
                    .withForce(isTrue(forceArg.value(options)))
                    .withSegmentCacheSize(Integer.getInteger("cache", 256))
                    .withGCLogInterval(Long.getLong("compaction-progress-log", 150000))
                    .build()
                    .run();
        } else {
            org.apache.jackrabbit.oak.segment.tool.Compact.Builder tarBuilder = Compact.builder()
                    .withPath(new File(path))
                    .withForce(isTrue(forceArg.value(options)))
                    .withMmap(mmapArg.value(options))
                    .withOs(StandardSystemProperty.OS_NAME.value())
                    .withSegmentCacheSize(Integer.getInteger("cache", 256))
                    .withGCLogInterval(Long.getLong("compaction-progress-log", 150000));

            if (options.has(compactor)) {
                tarBuilder.withCompactorType(CompactorType.fromDescription(compactor.value(options)));
            }

            code = tarBuilder
                    .build()
                    .run();
        }

        System.exit(code);
    }

}
