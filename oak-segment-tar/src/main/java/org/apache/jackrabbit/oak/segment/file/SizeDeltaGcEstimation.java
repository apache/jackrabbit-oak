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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;

class SizeDeltaGcEstimation implements GCEstimation {

    private final long delta;

    private final GCJournal gcJournal;

    private final long currentSize;

    private final boolean full;

    SizeDeltaGcEstimation(long delta, @Nonnull GCJournal gcJournal, long currentSize, boolean full) {
        this.delta = delta;
        this.gcJournal = checkNotNull(gcJournal);
        this.currentSize = currentSize;
        this.full = full;
    }

    @Override
    public GCEstimationResult estimate() {
        if (delta == 0) {
            return new GCEstimationResult(true, "Estimation skipped because the size delta value equals 0");
        }

        long previousSize = readPreviousSize();

        if (previousSize < 0) {
            return new GCEstimationResult(true, "Estimation skipped because of missing gc journal data (expected on first run)");
        }

        long gain = currentSize - previousSize;
        boolean gcNeeded = gain > delta;
        String gcInfo = format(
            "Segmentstore size has increased since the last %s garbage collection from %s (%s bytes) to %s (%s bytes), an increase of %s (%s bytes) or %s%%. ",
            full ? "full" : "tail",
            humanReadableByteCount(previousSize), previousSize,
            humanReadableByteCount(currentSize), currentSize,
            humanReadableByteCount(gain), gain, 100 * gain / previousSize
        );
        if (gcNeeded) {
            gcInfo = gcInfo + format(
                "This is greater than sizeDeltaEstimation=%s (%s bytes), so running garbage collection",
                humanReadableByteCount(delta), delta
            );
        } else {
            gcInfo = gcInfo + format(
                "This is less than sizeDeltaEstimation=%s (%s bytes), so skipping garbage collection",
                humanReadableByteCount(delta), delta
            );
        }
        return new GCEstimationResult(gcNeeded, gcInfo);
    }

    private long readPreviousSize() {
        if (full) {
            return readPreviousFullCleanupSize();
        }
        return readPreviousTailCleanupSize();
    }

    private long readPreviousFullCleanupSize() {
        List<GCJournalEntry> entries = new ArrayList<>(gcJournal.readAll());

        if (entries.isEmpty()) {
            return -1;
        }

        entries.sort((a, b) -> {
            if (a.getGcGeneration().getFullGeneration() > b.getGcGeneration().getFullGeneration()) {
                return -1;
            }
            if (a.getGcGeneration().getFullGeneration() < b.getGcGeneration().getFullGeneration()) {
                return 1;
            }
            return Integer.compare(a.getGcGeneration().getGeneration(), b.getGcGeneration().getGeneration());
        });

        return entries.iterator().next().getRepoSize();
    }

    private long readPreviousTailCleanupSize() {
        return gcJournal.read().getRepoSize();
    }

}
