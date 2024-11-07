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

import static java.lang.String.format;
import static org.apache.jackrabbit.oak.segment.file.PrintableBytes.newPrintableBytes;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;

class FullSizeDeltaEstimationStrategy implements EstimationStrategy {

    @Override
    public EstimationResult estimate(Context context) {
        if (context.getSizeDelta() == 0) {
            return new EstimationResult(true, "Estimation skipped because the size delta value equals 0");
        }

        long previousSize = readPreviousSize(context);

        if (previousSize < 0) {
            return new EstimationResult(true, "Estimation skipped because of missing gc journal data (expected on first run)");
        }

        if (previousIsTail(context)) {
            return new EstimationResult(true, "Detected previous garbage collection of type tail so running full garbage collection now.");
        }

        long gain = context.getCurrentSize() - previousSize;
        boolean gcNeeded = gain > context.getSizeDelta();
        String gcInfo = format(
            "Segmentstore size has increased since the last full garbage collection from %s to %s, an increase of %s or %s%%. ",
            newPrintableBytes(previousSize),
            newPrintableBytes(context.getCurrentSize()),
            newPrintableBytes(gain),
            100 * gain / previousSize
        );
        if (gcNeeded) {
            gcInfo = gcInfo + format(
                "This is greater than sizeDeltaEstimation=%s, so running garbage collection",
                newPrintableBytes(context.getSizeDelta())
            );
        } else {
            gcInfo = gcInfo + format(
                "This is less than sizeDeltaEstimation=%s, so skipping garbage collection",
                newPrintableBytes(context.getSizeDelta())
            );
        }
        return new EstimationResult(gcNeeded, gcInfo);
    }

    private long readPreviousSize(Context context) {
        List<GCJournalEntry> entries = new ArrayList<>(context.getGCJournal().readAll());

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

    private boolean previousIsTail(Context context) {
        List<GCJournalEntry> entries = new ArrayList<>(context.getGCJournal().readAll());
        if (entries.isEmpty()) {
            // We should not get here but if we do the condition is vacuously true
            return true;
        } else if (entries.size() == 1) {
            // A single entry in the gc log must be from a full compaction
            // as an initial compaction cannot be of type tail
            return false;
        } else {
            int m = entries.get(entries.size() - 2).getGcGeneration().getFullGeneration();
            int n = entries.get(entries.size() - 1).getGcGeneration().getFullGeneration();
            // No change in the full generation indicates the last compaction was of type tail
            return m == n;
        }
    }

}
