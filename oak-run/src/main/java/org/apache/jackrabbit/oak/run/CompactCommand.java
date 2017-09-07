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

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.run.commons.Command;

class CompactCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(String.class);
        OptionSpec<Boolean> mmapArg = parser.accepts("mmap",
                "Use memory mapped access if true, use file access if false. " +
                        "If not specified memory mapped access is used on 64 bit systems " +
                        "and file access is used on 32 bit systems.")
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

        File directory = new File(path);

        boolean success = false;
        Set<String> beforeLs = newHashSet();
        Set<String> afterLs = newHashSet();
        Stopwatch watch = Stopwatch.createStarted();

        Boolean mmap = mmapArg.value(options);
        System.out.println("Compacting " + directory);
        if (mmap == null) {
            System.out.println("With default access mode");
        } else if (mmap) {
            System.out.println("With memory mapped access");
        } else {
            System.out.println("With file access");
        }

        boolean force = isTrue(forceArg.value(options));

        System.out.println("    before ");
        beforeLs.addAll(list(directory));
        long sizeBefore = FileUtils.sizeOfDirectory(directory);
        System.out.println("    size "
                + IOUtils.humanReadableByteCount(sizeBefore) + " (" + sizeBefore
                + " bytes)");
        System.out.println("    -> compacting");

        try {
            SegmentTarUtils.compact(directory, mmap, force);
            success = true;
        } catch (Throwable e) {
            System.out.println("Compaction failure stack trace:");
            e.printStackTrace(System.out);
        } finally {
            watch.stop();
            if (success) {
                System.out.println("    after ");
                afterLs.addAll(list(directory));
                long sizeAfter = FileUtils.sizeOfDirectory(directory);
                System.out.println("    size "
                        + IOUtils.humanReadableByteCount(sizeAfter) + " ("
                        + sizeAfter + " bytes)");
                System.out.println("    removed files " + difference(beforeLs, afterLs));
                System.out.println("    added files " + difference(afterLs, beforeLs));
                System.out.println("Compaction succeeded in " + watch.toString()
                        + " (" + watch.elapsed(TimeUnit.SECONDS) + "s).");
            } else {
                System.out.println("Compaction failed in " + watch.toString()
                        + " (" + watch.elapsed(TimeUnit.SECONDS) + "s).");
                System.exit(1);
            }
        }
    }

    private static boolean isTrue(Boolean value) {
        return value != null && value;
    }

    private static Set<String> list(File directory) {
        Set<String> files = newHashSet();
        for (File f : directory.listFiles()) {
            String d = new Date(f.lastModified()).toString();
            String n = f.getName();
            System.out.println("        " + d + ", " + n);
            files.add(n);
        }
        return files;
    }

}