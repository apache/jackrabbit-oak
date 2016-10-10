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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;

public class SizeDeltaGcEstimation implements GCEstimation {

    private final long delta;

    private final GCJournal gcJournal;

    private final long totalSize;

    private boolean gcNeeded;

    private String gcInfo = "unknown";

    private boolean finished = false;

    public SizeDeltaGcEstimation(@Nonnull SegmentGCOptions opts,
            @Nonnull GCJournal gcJournal, long totalSize) {
        this.delta = checkNotNull(opts).getGcSizeDeltaEstimation();
        this.gcJournal = checkNotNull(gcJournal);
        this.totalSize = totalSize;
    }

    @Override
    public boolean gcNeeded() {
        if (!finished) {
            run();
        }
        return gcNeeded;
    }

    @Override
    public String gcLog() {
        if (!finished) {
            run();
        }
        return gcInfo;
    }

    private void run() {
        if (finished) {
            return;
        }
        if (delta == 0) {
            gcNeeded = true;
            gcInfo = format(
                    "Estimation skipped because the size delta value equals 0",
                    delta);
        } else if (getPreviousCleanupSize() < 0) {
            gcNeeded = true;
            gcInfo = "Estimation skipped because of missing gc journal data (expected on first run)";
        } else {
            long lastGc = getPreviousCleanupSize();
            long gain = totalSize - lastGc;
            long gainP = 100 * (totalSize - lastGc) / totalSize;
            gcNeeded = gain > delta;
            gcInfo = format(
                    "Segmentstore size has increased since the last compaction from %s (%s bytes) to %s (%s bytes), " +
                    "an increase of %s (%s bytes) or %s%%. ",
                    humanReadableByteCount(lastGc), lastGc,
                    humanReadableByteCount(totalSize), totalSize,
                    humanReadableByteCount(gain), gain, gainP);
            if (gcNeeded) {
                gcInfo = gcInfo + format(
                        "This is greater than sizeDeltaEstimation=%s (%s bytes), so running compaction",
                        humanReadableByteCount(delta), delta);
            } else {
                gcInfo = gcInfo + format(
                        "This is less than sizeDeltaEstimation=%s (%s bytes), so skipping compaction",
                        humanReadableByteCount(delta), delta);
            }
        }
        finished = true;
    }

    private long getPreviousCleanupSize() {
        return gcJournal.read().getRepoSize();
    }
}
