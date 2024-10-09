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

package org.apache.jackrabbit.oak.plugins.index.search.spi.binary;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

final class TextExtractionStats {
    private static final Logger log = LoggerFactory.getLogger(TextExtractionStats.class);
    /**
     * Log stats only if time spent is more than 1 min
     */
    private static final long LOGGING_THRESHOLD = TimeUnit.MINUTES.toMillis(1);


    private int numberOfExtractions;
    private long totalBytesRead;
    private long totalExtractedTextLength;
    private long totalExtractionTimeNanos;
    private long currentExtractionStartNanos = 0;
    private long startTimeNanos = System.nanoTime();

    public void reset() {
        this.numberOfExtractions = 0;
        this.totalBytesRead = 0;
        this.totalExtractedTextLength = 0;
        this.totalExtractionTimeNanos = 0;
        this.currentExtractionStartNanos = 0;
        this.startTimeNanos = System.nanoTime();
    }

    public void startExtraction() {
        currentExtractionStartNanos = System.nanoTime();
    }

    public void log(boolean reindex) {
        if (log.isDebugEnabled()) {
            log.debug("Text extraction stats {}", this);
        } else if (anyParsingDone() && (reindex || isTakingLotsOfTime())) {
            log.info("Text extraction stats {}", this);
        }
    }

    public long finishExtraction(long bytesRead, int extractedTextLength) {
        long elapsedNanos = System.nanoTime() - currentExtractionStartNanos;
        numberOfExtractions++;
        totalBytesRead += bytesRead;
        totalExtractedTextLength += extractedTextLength;
        totalExtractionTimeNanos += elapsedNanos;
        return elapsedNanos / 1_000_000;
    }

    public void collectStats(ExtractedTextCache cache) {
        cache.addStats(numberOfExtractions, totalExtractionTimeNanos / 1_000_000, totalBytesRead, totalExtractedTextLength);
    }

    private boolean isTakingLotsOfTime() {
        return totalExtractionTimeNanos > LOGGING_THRESHOLD * 1_000_000;
    }

    private boolean anyParsingDone() {
        return numberOfExtractions > 0;
    }

    @Override
    public String toString() {
        return String.format(" %d (Time Taken %s, Bytes Read %s, Extracted text size %s)",
                numberOfExtractions,
                timeInWords(totalExtractionTimeNanos),
                humanReadableByteCount(totalBytesRead),
                humanReadableByteCount(totalExtractedTextLength));
    }

    public String formatStats() {
        long timeSinceStartNanos = System.nanoTime() - startTimeNanos;
        double timeExtractingPercentage = FormattingUtils.safeComputePercentage(totalExtractionTimeNanos, timeSinceStartNanos);
        long avgExtractionTimeMillis = Math.round(FormattingUtils.safeComputeAverage(totalExtractionTimeNanos / 1_000_000, numberOfExtractions));

        return String.format("extractions: %d, timeExtracting: %s (%2.1f%%), " +
                        "avgExtractionTime: %s ms, bytesRead: %s, extractedTextSize: %s",
                numberOfExtractions,
                FormattingUtils.formatNanosToSeconds(totalExtractionTimeNanos),
                timeExtractingPercentage,
                avgExtractionTimeMillis,
                humanReadableByteCount(totalBytesRead), humanReadableByteCount(totalExtractedTextLength));
    }

    private static String timeInWords(long millis) {
        return String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }
}
