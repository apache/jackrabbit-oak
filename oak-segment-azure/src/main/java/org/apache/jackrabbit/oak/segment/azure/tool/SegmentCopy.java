/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.tool;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printMessage;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeDescription;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeTypeFromPathOrUri;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Check;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;

/**
 * Perform a full-copy of repository data at segment level.
 */
public class SegmentCopy {
    /**
     * Create a builder for the {@link SegmentCopy} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link SegmentCopy} command.
     */
    public static class Builder {

        private String source;

        private String destination;

        private SegmentNodeStorePersistence srcPersistence;

        private SegmentNodeStorePersistence destPersistence;

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The source path/URI to an existing segment store. This parameter is required.
         *
         * @param source
         *            the source path/URI to an existing segment store.
         * @return this builder.
         */
        public Builder withSource(String source) {
            this.source = checkNotNull(source);
            return this;
        }

        /**
         * The destination path/URI to an existing segment store. This parameter is
         * required.
         *
         * @param destination
         *            the destination path/URI to an existing segment store.
         * @return this builder.
         */
        public Builder withDestination(String destination) {
            this.destination = checkNotNull(destination);
            return this;
        }

        /**
         * The destination {@link SegmentNodeStorePersistence}.
         *
         * @param srcPersistence
         *            the destination {@link SegmentNodeStorePersistence}.
         * @return this builder.
         */
        public Builder withSrcPersistencee(SegmentNodeStorePersistence srcPersistence) {
            this.srcPersistence = checkNotNull(srcPersistence);
            return this;
        }

        /**
         * The destination {@link SegmentNodeStorePersistence}.
         *
         * @param destPersistence
         *            the destination {@link SegmentNodeStorePersistence}.
         * @return this builder.
         */
        public Builder withDestPersistence(SegmentNodeStorePersistence destPersistence) {
            this.destPersistence = checkNotNull(destPersistence);
            return this;
        }

        /**
         * The text output stream writer used to print normal output.
         *
         * @param outWriter
         *            the output writer.
         * @return this builder.
         */
        public Builder withOutWriter(PrintWriter outWriter) {
            this.outWriter = outWriter;
            return this;
        }

        /**
         * The text error stream writer used to print erroneous output.
         *
         * @param errWriter
         *            the error writer.
         * @return this builder.
         */
        public Builder withErrWriter(PrintWriter errWriter) {
            this.errWriter = errWriter;
            return this;
        }

        /**
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public SegmentCopy build() {
            if (srcPersistence == null && destPersistence == null) {
                checkNotNull(source);
                checkNotNull(destination);
            }
            return new SegmentCopy(this);
        }
    }

    private final String source;

    private final String destination;

    private final PrintWriter outWriter;

    private final PrintWriter errWriter;

    private SegmentNodeStorePersistence srcPersistence;

    private SegmentNodeStorePersistence destPersistence;


    public SegmentCopy(Builder builder) {
        this.source = builder.source;
        this.destination = builder.destination;
        this.srcPersistence = builder.srcPersistence;
        this.destPersistence = builder.destPersistence;
        this.outWriter = builder.outWriter;
        this.errWriter = builder.errWriter;
    }

    public int run() {
        Stopwatch watch = Stopwatch.createStarted();
        RepositoryLock srcRepositoryLock = null;

        SegmentStoreType srcType = storeTypeFromPathOrUri(source);
        SegmentStoreType destType = storeTypeFromPathOrUri(destination);

        String srcDescription = storeDescription(srcType, source);
        String destDescription = storeDescription(destType, destination);

        try {
            if (srcPersistence == null || destPersistence == null) {
                srcPersistence = newSegmentNodeStorePersistence(srcType, source);
                destPersistence = newSegmentNodeStorePersistence(destType, destination);
            }

            printMessage(outWriter, "Started segment-copy transfer!");
            printMessage(outWriter, "Source: {0}", srcDescription);
            printMessage(outWriter, "Destination: {0}", destDescription);

            try {
                srcRepositoryLock = srcPersistence.lockRepository();
            } catch (Exception e) {
                throw new Exception(MessageFormat.format(
                        "Cannot lock source segment store {0} for starting copying process. Giving up!",
                        srcDescription));
            }

            SegmentStoreMigrator migrator = new SegmentStoreMigrator.Builder()
                    .withSourcePersistence(srcPersistence, srcDescription)
                    .withTargetPersistence(destPersistence, destDescription)
                    .build();

            migrator.migrate();
        } catch (Exception e) {
            watch.stop();
            printMessage(errWriter, "A problem occured while copying archives from {0} to {1} ", source, destination);
            e.printStackTrace(errWriter);
            return 1;
        } finally {
            if (srcRepositoryLock != null) {
                try {
                    srcRepositoryLock.unlock();
                } catch (IOException e) {
                    printMessage(errWriter, "A problem occured while unlocking source repository {0} ", srcDescription);
                    e.printStackTrace(errWriter);
                }
            }
        }

        watch.stop();
        printMessage(outWriter, "Segment-copy succeeded in {0}", printableStopwatch(watch));

        return 0;
    }
}