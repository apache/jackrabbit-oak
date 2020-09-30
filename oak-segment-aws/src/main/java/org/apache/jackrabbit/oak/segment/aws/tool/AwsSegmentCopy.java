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
package org.apache.jackrabbit.oak.segment.aws.tool;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.printMessage;
import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.printableStopwatch;
import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.storeDescription;
import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.storeTypeFromPathOrUri;

import java.io.PrintWriter;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Check;

/**
 * Perform a full-copy of repository data at segment level.
 */
public class AwsSegmentCopy {
    /**
     * Create a builder for the {@link AwsSegmentCopy} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static boolean canExecute(String source, String destination) {
        return source.startsWith("aws:") || destination.startsWith("aws:");
    }

    /**
     * Collect options for the {@link AwsSegmentCopy} command.
     */
    public static class Builder {

        private String source;

        private String destination;

        private SegmentNodeStorePersistence srcPersistence;

        private SegmentNodeStorePersistence destPersistence;

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private Integer revisionsCount = Integer.MAX_VALUE;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The source path/URI to an existing segment store. This parameter is required.
         *
         * @param source the source path/URI to an existing segment store.
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
         * @param destination the destination path/URI to an existing segment store.
         * @return this builder.
         */
        public Builder withDestination(String destination) {
            this.destination = checkNotNull(destination);
            return this;
        }

        /**
         * The destination {@link SegmentNodeStorePersistence}.
         *
         * @param srcPersistence the destination {@link SegmentNodeStorePersistence}.
         * @return this builder.
         */
        public Builder withSrcPersistencee(SegmentNodeStorePersistence srcPersistence) {
            this.srcPersistence = checkNotNull(srcPersistence);
            return this;
        }

        /**
         * The destination {@link SegmentNodeStorePersistence}.
         *
         * @param destPersistence the destination {@link SegmentNodeStorePersistence}.
         * @return this builder.
         */
        public Builder withDestPersistence(SegmentNodeStorePersistence destPersistence) {
            this.destPersistence = checkNotNull(destPersistence);
            return this;
        }

        /**
         * The text output stream writer used to print normal output.
         *
         * @param outWriter the output writer.
         * @return this builder.
         */
        public Builder withOutWriter(PrintWriter outWriter) {
            this.outWriter = outWriter;
            return this;
        }

        /**
         * The text error stream writer used to print erroneous output.
         *
         * @param errWriter the error writer.
         * @return this builder.
         */
        public Builder withErrWriter(PrintWriter errWriter) {
            this.errWriter = errWriter;
            return this;
        }

        /**
         * The last {@code revisionsCount} revisions to be copied.
         * This parameter is not required and defaults to {@code 1}.
         *
         * @param revisionsCount number of revisions to copied.
         * @return this builder.
         */
        public Builder withRevisionsCount(Integer revisionsCount) {
            this.revisionsCount = revisionsCount;
            return this;
        }

        /**
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public AwsSegmentCopy build() {
            if (srcPersistence == null && destPersistence == null) {
                checkNotNull(source);
                checkNotNull(destination);
            }
            return new AwsSegmentCopy(this);
        }
    }

    private final String source;

    private final String destination;

    private final PrintWriter outWriter;

    private final PrintWriter errWriter;

    private final Integer revisionCount;

    private SegmentNodeStorePersistence srcPersistence;

    private SegmentNodeStorePersistence destPersistence;

    public AwsSegmentCopy(Builder builder) {
        this.source = builder.source;
        this.destination = builder.destination;
        this.srcPersistence = builder.srcPersistence;
        this.destPersistence = builder.destPersistence;
        this.revisionCount = builder.revisionsCount;
        this.outWriter = builder.outWriter;
        this.errWriter = builder.errWriter;
    }

    public int run() {
        Stopwatch watch = Stopwatch.createStarted();

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

            AwsSegmentStoreMigrator migrator = new AwsSegmentStoreMigrator.Builder()
                    .withSourcePersistence(srcPersistence, srcDescription)
                    .withTargetPersistence(destPersistence, destDescription)
                    .withRevisionCount(revisionCount)
                    .build();

            migrator.migrate();
        } catch (Exception e) {
            watch.stop();
            printMessage(errWriter, "A problem occured while copying archives from {0} to {1} ", source, destination);
            e.printStackTrace(errWriter);
            return 1;
        }

        watch.stop();
        printMessage(outWriter, "Segment-copy succeeded in {0}", printableStopwatch(watch));

        return 0;
    }
}