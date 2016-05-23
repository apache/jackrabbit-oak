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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;

class CompactCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(String.class);
        OptionSpec<Void> forceFlag = parser.accepts(
                "force", "Force compaction and ignore non matching segment version");
        OptionSpec segmentTar = parser.accepts("segment-tar", "Use oak-segment-tar instead of oak-segment");
        OptionSet options = parser.parse(args);

        String path = directoryArg.value(options);
        if (path == null) {
            System.err.println("Compact a file store. Usage: compact [path] <options>");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        File directory = new File(path);
        boolean force = options.has(forceFlag);

        Stopwatch watch = Stopwatch.createStarted();

        if (options.has(segmentTar)) {
            SegmentTarUtils.compact(directory, force);
        } else {
            SegmentUtils.compact(directory, force);
        }

        watch.stop();
        System.out.println("    after  "
                + Arrays.toString(directory.list()));
        long sizeAfter = FileUtils.sizeOfDirectory(directory);
        System.out.println("    size "
                + IOUtils.humanReadableByteCount(sizeAfter) + " (" + sizeAfter
                + " bytes)");
        System.out.println("    duration  " + watch.toString() + " ("
                + watch.elapsed(TimeUnit.SECONDS) + "s).");
    }

}
