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

package org.apache.jackrabbit.oak.plugins.index.lucene.binary;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

class TextExtractionStats {
    private static final Logger log = LoggerFactory.getLogger(TextExtractionStats.class);
    /**
     * Log stats only if time spent is more than 1 min
     */
    private static final long LOGGING_THRESHOLD = TimeUnit.MINUTES.toMillis(1);
    private int count;
    private long totalBytesRead;
    private long totalTime;
    private long totalTextLength;

    public void addStats(long timeInMillis, long bytesRead, int textLength) {
        count++;
        totalBytesRead += bytesRead;
        totalTime += timeInMillis;
        totalTextLength += textLength;
    }

    public void log(boolean reindex) {
        if (log.isDebugEnabled()) {
            log.debug("Text extraction stats {}", this);
        } else if (anyParsingDone() && (reindex || isTakingLotsOfTime())) {
            log.info("Text extraction stats {}", this);
        }
    }

    public void collectStats(ExtractedTextCache cache){
        cache.addStats(count, totalTime, totalBytesRead, totalTextLength);
    }

    private boolean isTakingLotsOfTime() {
        return totalTime > LOGGING_THRESHOLD;
    }

    private boolean anyParsingDone() {
        return count > 0;
    }

    @Override
    public String toString() {
        return String.format(" %d (Time Taken %s, Bytes Read %s, Extracted text size %s)",
                count,
                timeInWords(totalTime),
                humanReadableByteCount(totalBytesRead),
                humanReadableByteCount(totalTextLength));
    }

    private static String timeInWords(long millis) {
        return String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }
}
