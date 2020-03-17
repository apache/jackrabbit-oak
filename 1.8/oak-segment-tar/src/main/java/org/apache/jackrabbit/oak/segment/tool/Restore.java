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

import java.io.File;

import org.apache.jackrabbit.oak.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.backup.impl.FileStoreRestoreImpl;

/**
 * Restore a backup of a segment store into an existing segment store.
 */
public class Restore implements Runnable {

    /**
     * Create a builder for the {@link Restore} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Restore} command.
     */
    public static class Builder {

        private File source;

        private File target;

        private final FileStoreRestore fileStoreRestore = new FileStoreRestoreImpl();

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The source path of the restore. This parameter is mandatory.
         *
         * @param source the source path of the restore.
         * @return this builder.
         */
        public Builder withSource(File source) {
            this.source = checkNotNull(source);
            return this;
        }

        /**
         * The target of the restore. This parameter is mandatory.
         *
         * @param target the target of the restore.
         * @return this builder.
         */
        public Builder withTarget(File target) {
            this.target = checkNotNull(target);
            return this;
        }

        /**
         * Create an executable version of the {@link Restore} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(source);
            checkNotNull(target);
            return new Restore(this);
        }

    }

    private final File source;

    private final File target;

    private final FileStoreRestore fileStoreRestore;

    private Restore(Builder builder) {
        this.source = builder.source;
        this.target = builder.target;
        this.fileStoreRestore = builder.fileStoreRestore;
    }

    @Override
    public void run() {
        try {
            fileStoreRestore.restore(source, target);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
