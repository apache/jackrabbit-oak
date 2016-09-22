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

package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;

/**
 * Perform an offline compaction of an existing segment store.
 */
public class Compact implements Runnable {

    /**
     * Create a builder for the {@link Compact} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Compact} command.
     */
    public static class Builder {

        private File path;

        private boolean force;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(File path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * Set whether or not to force compact concurrent commits on top of
         * already compacted commits after the maximum number of retries has
         * been reached. Force committing tries to exclusively write lock the
         * node store.
         *
         * @param force {@code true} to force an exclusive commit of the
         *              compacted state, {@code false} otherwise.
         * @return this builder.
         */
        public Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        /**
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            return new Compact(this);
        }

    }

    private final File path;

    private final boolean force;

    private Compact(Builder builder) {
        this.path = builder.path;
        this.force = builder.force;
    }

    @Override
    public void run() {
        try {
            compact();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void compact() throws IOException, InvalidFileStoreVersionException {
        try (FileStore store = newFileStore()) {
            store.compact();
        }

        System.out.println("    -> cleaning up");
        try (FileStore store = newFileStore()) {
            for (File file : store.cleanup()) {
                if (!file.exists() || file.delete()) {
                    System.out.println("    -> removed old file " + file.getName());
                } else {
                    System.out.println("    -> failed to remove old file " + file.getName());
                }
            }

            String head;

            File journal = new File(path, "journal.log");

            try (JournalReader journalReader = new JournalReader(journal)) {
                head = journalReader.next() + " root " + System.currentTimeMillis() + "\n";
            }

            try (RandomAccessFile journalFile = new RandomAccessFile(journal, "rw")) {
                System.out.println("    -> writing new " + journal.getName() + ": " + head);
                journalFile.setLength(0);
                journalFile.writeBytes(head);
                journalFile.getChannel().force(false);
            }
        }
    }

    private FileStore newFileStore() throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(path.getAbsoluteFile()).withGCOptions(defaultGCOptions().setOffline()).build();
    }

}
