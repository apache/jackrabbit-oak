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
import static org.apache.jackrabbit.oak.segment.tool.Utils.newBasicReadOnlyBlobStore;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.backup.impl.FileStoreBackupImpl;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;

/**
 * Perform a backup of a segment store into a specified folder.
 */
public class Backup implements Runnable {

    /**
     * Create a builder for the {@link Backup} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Backup} command.
     */
    public static class Builder {

        private File source;

        private File target;

        private boolean fakeBlobStore = FileStoreBackupImpl.USE_FAKE_BLOBSTORE;

        private final FileStoreBackup fileStoreBackup = new FileStoreBackupImpl();

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The source folder of the backup. This parameter is required. The path
         * should point to a valid segment store.
         *
         * @param source the path of the source folder of the backup.
         * @return this builder.
         */
        public Builder withSource(File source) {
            this.source = checkNotNull(source);
            return this;
        }

        /**
         * The target folder of the backup. This parameter is required. The path
         * should point to an existing segment store or to an empty folder. If
         * the folder doesn't exist, it will be created.
         *
         * @param target the path of the target folder of the backup.
         * @return this builder.
         */
        public Builder withTarget(File target) {
            this.target = checkNotNull(target);
            return this;
        }

        /**
         * Simulate the existence of a file-based blob store. This parameter is
         * not required and defaults to {@code false}.
         *
         * @param fakeBlobStore {@code true} if a file-based blob store should
         *                      be simulated, {@code false} otherwise.
         * @return this builder.
         */
        public Builder withFakeBlobStore(boolean fakeBlobStore) {
            this.fakeBlobStore = fakeBlobStore;
            return this;
        }

        /**
         * Create an executable version of the {@link Backup} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(source);
            checkNotNull(target);
            return new Backup(this);
        }

    }

    private final File source;

    private final File target;

    private final boolean fakeBlobStore;

    private final FileStoreBackup fileStoreBackup;

    private Backup(Builder builder) {
        this.source = builder.source;
        this.target = builder.target;
        this.fakeBlobStore = builder.fakeBlobStore;
        this.fileStoreBackup = builder.fileStoreBackup;
    }

    @Override
    public void run() {
        try (ReadOnlyFileStore fs = newFileStore()) {
            fileStoreBackup.backup(fs.getReader(), fs.getRevisions(), target);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ReadOnlyFileStore newFileStore() throws IOException, InvalidFileStoreVersionException {
        if (fakeBlobStore) {
            return openReadOnlyFileStore(source, newBasicReadOnlyBlobStore());
        }

        return openReadOnlyFileStore(source);
    }

}
