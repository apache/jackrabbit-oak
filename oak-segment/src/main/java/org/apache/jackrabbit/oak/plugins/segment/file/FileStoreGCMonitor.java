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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.text.DateFormat.getDateTimeInstance;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.stats.TimeSeriesStatsUtil.asCompositeData;
import static org.slf4j.helpers.MessageFormatter.arrayFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.stats.TimeSeriesRecorder;

/**
 * {@link GCMonitor} implementation providing the file store gc status
 * as {@link GCMonitorMBean}.
 * <p>
 * Users of this class need to schedule a call to {@link #run()} once per
 * second to ensure the various time series maintained by this implementation
 * are correctly aggregated.
 */
@Deprecated
public class FileStoreGCMonitor extends AnnotatedStandardMBean
        implements GCMonitor, GCMonitorMBean, Runnable {
    private final TimeSeriesRecorder gcCount = new TimeSeriesRecorder(true);
    private final TimeSeriesRecorder repositorySize = new TimeSeriesRecorder(false);
    private final TimeSeriesRecorder reclaimedSize = new TimeSeriesRecorder(true);

    private final Clock clock;

    private long lastCompaction;
    private long[] segmentCounts = new long[0];
    private long[] recordCounts = new long[0];
    private long[] compactionMapWeights = new long[0];
    private long lastCleanup;
    private String lastError;
    private String status = "NA";

    @Deprecated
    public FileStoreGCMonitor(@Nonnull Clock clock) {
        super(GCMonitorMBean.class);
        this.clock = checkNotNull(clock);
    }

    //------------------------------------------------------------< Runnable >---

    @Override
    @Deprecated
    public void run() {
        gcCount.recordOneSecond();
        repositorySize.recordOneSecond();
        reclaimedSize.recordOneSecond();
    }

    //------------------------------------------------------------< GCMonitor >---

    @Override
    @Deprecated
    public void info(String message, Object... arguments) {
        status = arrayFormat(message, arguments).getMessage();
    }

    @Override
    @Deprecated
    public void warn(String message, Object... arguments) {
        status = arrayFormat(message, arguments).getMessage();
    }

    @Override
    @Deprecated
    public void error(String message, Exception exception) {
        StringWriter sw = new StringWriter();
        sw.write(message + ": ");
        exception.printStackTrace(new PrintWriter(sw));
        lastError = sw.toString();
    }

    @Override
    @Deprecated
    public void skipped(String reason, Object... arguments) {
        status = arrayFormat(reason, arguments).getMessage();
    }

    @Override
    @Deprecated
    public void compacted() {
        lastCompaction = clock.getTime();
    }

    @Override
    @Deprecated
    public void cleaned(long reclaimed, long current) {
        lastCleanup = clock.getTime();
        gcCount.getCounter().addAndGet(1);
        repositorySize.getCounter().set(current);
        reclaimedSize.getCounter().addAndGet(reclaimed);
    }

    //------------------------------------------------------------< GCMonitorMBean >---

    @Override
    @Deprecated
    public String getLastCompaction() {
        return toString(lastCompaction);
    }

    @Override
    @Deprecated
    public String getLastCleanup() {
        return toString(lastCleanup);
    }

    private static String toString(long timestamp) {
        if (timestamp != 0) {
            return getDateTimeInstance().format(new Date(timestamp));
        } else {
            return null;
        }
    }

    @Override
    @Deprecated
    public String getLastError() {
        return lastError;
    }

    @Nonnull
    @Override
    @Deprecated
    public String getStatus() {
        return status;
    }

    @Override
    @Deprecated
    public String getCompactionMapStats() {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (int k = 0; k < segmentCounts.length; k++) {
            sb.append(sep).append('[')
                .append("Estimated Weight: ")
                .append(humanReadableByteCount(compactionMapWeights[k])).append(", ")
                .append("Segments: ")
                .append(segmentCounts[k]).append(", ")
                .append("Records: ")
                .append(recordCounts[k]).append(']');
            sep = ", ";
        }
        return sb.toString();
    }

    @Nonnull
    @Override
    @Deprecated
    public CompositeData getRepositorySize() {
        return asCompositeData(repositorySize, "RepositorySize");
    }

    @Nonnull
    @Override
    @Deprecated
    public CompositeData getReclaimedSize() {
        return asCompositeData(reclaimedSize, "ReclaimedSize");
    }

}
