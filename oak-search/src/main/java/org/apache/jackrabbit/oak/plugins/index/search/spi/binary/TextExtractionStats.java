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
    private static final Logger LOG = LoggerFactory.getLogger(TextExtractionStats.class);
    /**
     * Log stats only if time spent is more than 1 min
     */
    private static final long LOGGING_THRESHOLD = TimeUnit.MINUTES.toMillis(1);

    private static long systemNanoAsMillis() {
        return System.nanoTime() / 1_000_000;
    }

    private int numberOfExtractions;
    private long totalBytesRead;
    private long totalExtractedTextLength;
    private long totalExtractionTimeMillis;
    private long currentExtractionStartMillis = 0;
    private long startTimeMillis = systemNanoAsMillis();

    public void reset() {
        LOG.info("Resetting statistics");
        this.numberOfExtractions = 0;
        this.totalBytesRead = 0;
        this.totalExtractedTextLength = 0;
        this.totalExtractionTimeMillis = 0;
        this.currentExtractionStartMillis = 0;
        this.startTimeMillis = systemNanoAsMillis();
    }

    public void startExtraction() {
        currentExtractionStartMillis = systemNanoAsMillis();
    }

    public void log(boolean reindex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Text extraction stats {}", this);
        } else if (anyParsingDone() && (reindex || isTakingLotsOfTime())) {
            LOG.info("Text extraction stats {}", this);
        }
    }

    public long finishExtraction(long bytesRead, int extractedTextLength) {
        long elapsedMillis = systemNanoAsMillis() - currentExtractionStartMillis;
        numberOfExtractions++;
        totalBytesRead += bytesRead;
        totalExtractedTextLength += extractedTextLength;
        totalExtractionTimeMillis += elapsedMillis;
        return elapsedMillis;
    }

    public void collectStats(ExtractedTextCache cache) {
        cache.addStats(numberOfExtractions, totalExtractionTimeMillis, totalBytesRead, totalExtractedTextLength);
    }

    private boolean isTakingLotsOfTime() {
        return totalExtractionTimeMillis > LOGGING_THRESHOLD;
    }

    private boolean anyParsingDone() {
        return numberOfExtractions > 0;
    }

    @Override
    public String toString() {
        return String.format(" %d (Time Taken %s, Bytes Read %s, Extracted text size %s)",
                numberOfExtractions,
                timeInWords(totalExtractionTimeMillis),
                humanReadableByteCount(totalBytesRead),
                humanReadableByteCount(totalExtractedTextLength));
    }

    public String formatStats() {
        long timeSinceStartMillis = systemNanoAsMillis() - startTimeMillis;
        double timeExtractingPercentage = timeSinceStartMillis == 0 ? -1 : (totalExtractionTimeMillis * 100.0) / timeSinceStartMillis;
        long avgExtractionTimeMillis = numberOfExtractions == 0 ? -1 : totalExtractionTimeMillis / numberOfExtractions;

        return String.format("{extractions: %d, timeExtracting: %s (%2.1f%%), totalTime: %s, " +
                        "avgExtractionTime: %s ms, bytesRead: %s, extractedTextSize: %s}",
                numberOfExtractions,
                FormattingUtils.formatToSeconds(totalExtractionTimeMillis / 1000),
                timeExtractingPercentage,
                FormattingUtils.formatToSeconds(timeSinceStartMillis / 1000),
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
