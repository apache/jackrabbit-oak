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
import org.apache.jackrabbit.oak.segment.tool.Compact;

class CompactCommand implements Command {

    private static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(String.class);
        OptionSpec<Boolean> mmapArg = parser.accepts("mmap",
                "Use memory mapped access if true, use file access if false. " +
                    "If not specified, memory mapped access is used on 64 bit " +
                    "systems and file access is used on 32 bit systems. On " +
                    "Windows, regular file access is always enforced and this " +
                    "option is ignored.")
                .withOptionalArg()
                .ofType(Boolean.class);
        OptionSpec<Boolean> forceArg = parser.accepts("force",
                "Force compaction and ignore a non matching segment store version. " +
                        "CAUTION: this will upgrade the segment store to the latest version, " +
                        "which is incompatible with older versions of Oak.")
                .withOptionalArg()
                .ofType(Boolean.class);
        OptionSet options = parser.parse(args);

        String path = directoryArg.value(options);

        if (path == null) {
            System.err.println("Compact a file store. Usage: compact [path] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        int code = Compact.builder()
            .withPath(new File(path))
            .withForce(isTrue(forceArg.value(options)))
            .withMmap(mmapArg.value(options))
            .withOs(StandardSystemProperty.OS_NAME.value())
            .withSegmentCacheSize(Integer.getInteger("cache", 256))
            .withGCLogInterval(Long.getLong("compaction-progress-log", 150000))
            .build()
            .run();

        System.exit(code);
    }

}
