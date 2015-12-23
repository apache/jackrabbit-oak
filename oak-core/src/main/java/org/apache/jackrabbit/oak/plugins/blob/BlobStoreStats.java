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

package org.apache.jackrabbit.oak.plugins.blob;

import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class BlobStoreStats implements BlobStoreStatsMBean, BlobStatsCollector {
    private static final String BLOB_UPLOADS = "BLOB_UPLOADS";
    private static final String BLOB_DOWNLOADS = "BLOB_DOWNLOADS";

    private final StatisticsProvider statisticsProvider;

    private final HistogramStats uploadHisto;
    private final MeterStats uploadSizeMeter;
    private final MeterStats uploadTimeMeter;

    private final HistogramStats downloadHisto;
    private final MeterStats downloadSizeMeter;
    private final MeterStats downloadTimeMeter;

    public BlobStoreStats(StatisticsProvider sp) {
        this.statisticsProvider = sp;

        this.uploadHisto = sp.getHistogram(BLOB_UPLOADS);
        //TODO Need to expose an API in StatisticsProvider to register for avg
        //That would give us upload and download *rate*
        this.uploadSizeMeter = sp.getMeter("BLOB_UPLOAD_SIZE");
        this.uploadTimeMeter = sp.getMeter("BLOB_UPLOAD_TIME");

        this.downloadHisto = sp.getHistogram(BLOB_DOWNLOADS);
        this.downloadSizeMeter = sp.getMeter("BLOB_DOWNLOAD_SIZE");
        this.downloadTimeMeter = sp.getMeter("BLOB_DOWNLOAD_TIME");
    }

    @Override
    public void uploaded(long timeTaken, TimeUnit unit, long size) {
        uploadHisto.update(size);

        //Recording upload like this is not accurate. A more accurate way
        //would be to mark as upload or download is progressing.
        //That would however add quite a bit of overhead
        //Approach below would record an upload/download at moment when
        //it got completed. So acts like a rough approximation
        uploadSizeMeter.mark(size);
        uploadTimeMeter.mark(TimeUnit.NANOSECONDS.convert(timeTaken, unit));
    }

    @Override
    public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {
        downloadHisto.update(size);
        downloadSizeMeter.mark(size);
        downloadTimeMeter.mark(TimeUnit.NANOSECONDS.convert(timeTaken, unit));
    }

    //~--------------------------------------< BlobStoreMBean >

    @Override
    public long getUploadTotalSize() {
        return uploadSizeMeter.getCount();
    }

    @Override
    public long getUploadCount() {
        return uploadHisto.getCount();
    }

    @Override
    public long getUploadTotalSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(uploadTimeMeter.getCount());
    }

    @Override
    public long getDownloadTotalSize() {
        return downloadSizeMeter.getCount();
    }

    @Override
    public long getDownloadCount() {
        return downloadHisto.getCount();
    }

    @Override
    public long getDownloadTotalSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(downloadTimeMeter.getCount());
    }

    @Override
    public String blobStoreInfoAsString() {
        return String.format("Uploads - size = %s, count = %d%nDownloads - size = %s, count = %d",
                humanReadableByteCount(getUploadTotalSize()),
                getUploadCount(),
                humanReadableByteCount(getDownloadTotalSize()),
                getDownloadCount()
        );
    }

    @Override
    public CompositeData getUploadSizeHistory() {
        return getTimeSeries(BLOB_UPLOADS);
    }

    @Override
    public CompositeData getDownloadSizeHistory() {
        return getTimeSeries(BLOB_DOWNLOADS);
    }

    private CompositeData getTimeSeries(String name){
        return TimeSeriesStatsUtil.asCompositeData(statisticsProvider.getStats().getTimeSeries(name, true),
                name);
    }
}
