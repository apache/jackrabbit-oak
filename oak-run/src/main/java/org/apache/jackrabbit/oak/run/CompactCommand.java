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

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.openFileStore;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;

import com.google.common.base.Stopwatch;

class CompactCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(String.class);
        OptionSpec<Void> forceFlag = parser.accepts(
                "force", "Force compaction and ignore non matching segment version");
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

        System.out.println("Compacting " + directory);
        System.out.println("    before ");
        beforeLs.addAll(list(directory));
        long sizeBefore = FileUtils.sizeOfDirectory(directory);
        System.out.println("    size "
                + IOUtils.humanReadableByteCount(sizeBefore) + " (" + sizeBefore
                + " bytes)");
        System.out.println("    -> compacting");

        try {
            FileStore store = openFileStore(path, options.has(forceFlag));
            boolean persistCM = Boolean.getBoolean("tar.PersistCompactionMap");
            try {
                CompactionStrategy compactionStrategy = new CompactionStrategy(
                        false, CompactionStrategy.CLONE_BINARIES_DEFAULT,
                        CompactionStrategy.CleanupType.CLEAN_ALL, 0,
                        CompactionStrategy.MEMORY_THRESHOLD_DEFAULT) {
                    @Override
                    public boolean compacted(Callable<Boolean> setHead)
                            throws Exception {
                        // oak-run is doing compaction single-threaded
                        // hence no guarding needed - go straight ahead
                        // and call setHead
                        return setHead.call();
                    }
                };
                compactionStrategy.setOfflineCompaction(true);
                compactionStrategy.setPersistCompactionMap(persistCM);
                store.setCompactionStrategy(compactionStrategy);
                store.compact();
            } finally {
                store.close();
            }

            System.out.println("    -> cleaning up");
            store = openFileStore(path, false);
            try {
                for (File file : store.cleanup()) {
                    if (!file.exists() || file.delete()) {
                        System.out.println("    -> removed old file " + file.getName());
                    } else {
                        System.out.println("    -> failed to remove old file " + file.getName());
                    }
                }

                String head;
                File journal = new File(directory, "journal.log");
                JournalReader journalReader = new JournalReader(journal);
                try {
                    head = journalReader.iterator().next() + " root\n";
                } finally {
                    journalReader.close();
                }

                RandomAccessFile journalFile = new RandomAccessFile(journal, "rw");
                try {
                    System.out.println("    -> writing new " + journal.getName() + ": " + head);
                    journalFile.setLength(0);
                    journalFile.writeBytes(head);
                    journalFile.getChannel().force(false);
                } finally {
                    journalFile.close();
                }
            } finally {
                store.close();
            }
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
