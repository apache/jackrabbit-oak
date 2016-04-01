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

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;

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
        Stopwatch watch = Stopwatch.createStarted();
        FileStore store = openFileStore(path, options.has(forceFlag));
        File directory = new File(path);
        try {
            boolean persistCM = Boolean.getBoolean("tar.PersistCompactionMap");
            System.out.println("Compacting " + directory);
            System.out.println("    before " + Arrays.toString(directory.list()));
            long sizeBefore = FileUtils.sizeOfDirectory(directory);
            System.out.println("    size "
                    + IOUtils.humanReadableByteCount(sizeBefore) + " (" + sizeBefore
                    + " bytes)");

            System.out.println("    -> compacting");

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
                head = journalReader.iterator().next() + " root " + System.currentTimeMillis()+"\n";
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
