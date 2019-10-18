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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.stats.TimeSeriesAverage;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class BlobStoreStats extends AnnotatedStandardMBean implements ExtendedBlobStoreStatsMBean, ExtendedBlobStatsCollector {
    private final Logger opsLogger = LoggerFactory.getLogger("org.apache.jackrabbit.oak.operations.blobs");
    private static final String BLOB_UPLOADS = "BLOB_UPLOADS";
    private static final String BLOB_UPLOAD_COUNT = "BLOB_UPLOAD_COUNT";
    private static final String BLOB_UPLOAD_SIZE = "BLOB_UPLOAD_SIZE";
    private static final String BLOB_UPLOAD_TIME = "BLOB_UPLOAD_TIME";
    private static final String BLOB_UPLOAD_ERROR_COUNT = "BLOB_UPLOAD_ERROR_COUNT";

    private static final String BLOB_DOWNLOADS = "BLOB_DOWNLOADS";
    private static final String BLOB_DOWNLOAD_COUNT = "BLOB_DOWNLOAD_COUNT";
    private static final String BLOB_DOWNLOAD_SIZE = "BLOB_DOWNLOAD_SIZE";
    private static final String BLOB_DOWNLOAD_TIME = "BLOB_DOWNLOAD_TIME";

    private static final String BLOB_DELETE_COUNT = "BLOB_DELETE_COUNT";
    private static final String BLOB_DELETE_TIME = "BLOB_DELETE_TIME";
    private static final String BLOB_DELETE_ERROR_COUNT = "BLOB_DELETE_ERROR_COUNT";
    private static final String BLOB_DELETE_BY_DATE_COUNT = "BLOB_DELETE_BY_DATE_COUNT";
    private static final String BLOB_DELETE_BY_DATE_TIME = "BLOB_DELETE_BY_DATE_TIME";
    private static final String BLOB_DELETE_BY_DATE_ERROR_COUNT = "BLOB_DELETE_BY_DATE_ERROR_COUNT";

    private static final String BLOB_ADD_RECORD_COUNT = "BLOB_ADD_RECORD_COUNT";
    private static final String BLOB_ADD_RECORD_SIZE = "BLOB_ADD_RECORD_SIZE";
    private static final String BLOB_ADD_RECORD_TIME = "BLOB_ADD_RECORD_TIME";
    private static final String BLOB_ADD_RECORD_ERROR_COUNT = "BLOB_ADD_RECORD_ERROR_COUNT";

    private static final String BLOB_GETREC_COUNT = "BLOB_GETREC_COUNT";
    private static final String BLOB_GETREC_TIME = "BLOB_GETREC_TIME";
    private static final String BLOB_GETREC_ERROR_COUNT = "BLOB_GETREC_ERROR_COUNT";
    private static final String BLOB_GETRECIFSTORED_COUNT = "BLOB_GETRECIFSTORED_COUNT";
    private static final String BLOB_GETRECIFSTORED_TIME = "BLOB_GETRECIFSTORED_TIME";
    private static final String BLOB_GETRECIFSTORED_ERROR_COUNT = "BLOB_GETRECIFSTORED_ERROR_COUNT";
    private static final String BLOB_GETRECFROMREF_COUNT = "BLOB_GETRECFROMREF_COUNT";
    private static final String BLOB_GETRECFROMREF_TIME = "BLOB_GETRECFROMREF_TIME";
    private static final String BLOB_GETRECFROMREF_ERROR_COUNT = "BLOB_GETRECFROMREF_ERROR_COUNT";
    private static final String BLOB_GETRECFORID_COUNT = "BLOB_GETRECFORID_COUNT";
    private static final String BLOB_GETRECFORID_TIME = "BLOB_GETRECFORID_TIME";
    private static final String BLOB_GETRECFORID_ERROR_COUNT = "BLOB_GETRECFORID_ERROR_COUNT";

    private static final String BLOB_GETALLRECORDS_COUNT = "BLOB_GETALLRECORDS_COUNT";
    private static final String BLOB_GETALLRECORDS_TIME = "BLOB_GETALLRECORDS_TIME";

    private static final String BLOB_LISTIDS_COUNT = "BLOB_LISTIDS_COUNT";
    private static final String BLOB_LISTIDS_TIME = "BLOB_LISTIDS_TIME";
    private static final String BLOB_LISTIDS_ERROR_COUNT = "BLOB_LISTIDS_ERROR_COUNT";


    private final StatisticsProvider statisticsProvider;

    private final HistogramStats uploadHisto;
    private final MeterStats uploadCount;
    private final MeterStats uploadErrorCount;
    private final MeterStats uploadSizeSeries;
    private final MeterStats uploadTimeSeries;
    private final TimeSeries uploadRateSeries;

    private final HistogramStats downloadHisto;
    private final MeterStats downloadCount;
    private final MeterStats downloadSizeSeries;
    private final MeterStats downloadTimeSeries;
    private final TimeSeries downloadRateSeries;

    private final MeterStats deleteCount;
    private final MeterStats deleteTimeSeries;
    private final MeterStats deleteErrorCount;
    private final MeterStats deleteByDateCount;
    private final MeterStats deleteByDateTimeSeries;
    private final MeterStats deleteByDateErrorCount;

    private final MeterStats addRecordCount;
    private final MeterStats addRecordSizeSeries;
    private final MeterStats addRecordTimeSeries;
    private final TimeSeries addRecordRateSeries;
    private final MeterStats addRecordErrorCount;

    private final MeterStats getRecordCount;
    private final MeterStats getRecordTimeSeries;
    private final MeterStats getRecordErrorCount;
    private final MeterStats getRecordIfStoredCount;
    private final MeterStats getRecordIfStoredTimeSeries;
    private final MeterStats getRecordIfStoredErrorCount;
    private final MeterStats getRecordFromRefCount;
    private final MeterStats getRecordFromRefTimeSeries;
    private final MeterStats getRecordFromRefErrorCount;
    private final MeterStats getRecordForIdCount;
    private final MeterStats getRecordForIdTimeSeries;
    private final MeterStats getRecordForIdErrorCount;

    private final MeterStats getAllRecordsCount;
    private final MeterStats getAllRecordsTimeSeries;

    private final MeterStats getListIdsCount;
    private final MeterStats getListIdsTimeSeries;
    private final MeterStats getListIdsErrorCount;

    private final TimeUnit recordedTimeUnit = TimeUnit.NANOSECONDS;

    public BlobStoreStats(@NotNull  StatisticsProvider sp) {
        super(BlobStoreStatsMBean.class);
        this.statisticsProvider = checkNotNull(sp);

        this.uploadHisto = sp.getHistogram(BLOB_UPLOADS, StatsOptions.DEFAULT);
        this.uploadCount = sp.getMeter(BLOB_UPLOAD_COUNT, StatsOptions.DEFAULT);
        this.uploadErrorCount = sp.getMeter(BLOB_UPLOAD_ERROR_COUNT, StatsOptions.DEFAULT);
        this.uploadSizeSeries = sp.getMeter(BLOB_UPLOAD_SIZE, StatsOptions.TIME_SERIES_ONLY);
        this.uploadTimeSeries = sp.getMeter(BLOB_UPLOAD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.uploadRateSeries = getAvgTimeSeries(BLOB_UPLOAD_SIZE, BLOB_UPLOAD_TIME);

        this.downloadHisto = sp.getHistogram(BLOB_DOWNLOADS, StatsOptions.DEFAULT);
        this.downloadCount = sp.getMeter(BLOB_DOWNLOAD_COUNT, StatsOptions.DEFAULT);
        this.downloadSizeSeries = sp.getMeter(BLOB_DOWNLOAD_SIZE, StatsOptions.TIME_SERIES_ONLY);
        this.downloadTimeSeries = sp.getMeter(BLOB_DOWNLOAD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.downloadRateSeries = getAvgTimeSeries(BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME);

        this.deleteCount = sp.getMeter(BLOB_DELETE_COUNT, StatsOptions.DEFAULT);
        this.deleteTimeSeries = sp.getMeter(BLOB_DELETE_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.deleteErrorCount = sp.getMeter(BLOB_DELETE_ERROR_COUNT, StatsOptions.DEFAULT);
        this.deleteByDateCount = sp.getMeter(BLOB_DELETE_BY_DATE_COUNT, StatsOptions.DEFAULT);
        this.deleteByDateTimeSeries = sp.getMeter(BLOB_DELETE_BY_DATE_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.deleteByDateErrorCount = sp.getMeter(BLOB_DELETE_BY_DATE_ERROR_COUNT, StatsOptions.DEFAULT);

        this.addRecordCount = sp.getMeter(BLOB_ADD_RECORD_COUNT, StatsOptions.DEFAULT);
        this.addRecordSizeSeries = sp.getMeter(BLOB_ADD_RECORD_SIZE, StatsOptions.TIME_SERIES_ONLY);
        this.addRecordTimeSeries = sp.getMeter(BLOB_ADD_RECORD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.addRecordRateSeries = getAvgTimeSeries(BLOB_ADD_RECORD_SIZE, BLOB_ADD_RECORD_TIME);
        this.addRecordErrorCount = sp.getMeter(BLOB_ADD_RECORD_ERROR_COUNT, StatsOptions.DEFAULT);

        this.getRecordCount = sp.getMeter(BLOB_GETREC_COUNT, StatsOptions.DEFAULT);
        this.getRecordTimeSeries = sp.getMeter(BLOB_GETREC_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getRecordErrorCount = sp.getMeter(BLOB_GETREC_ERROR_COUNT, StatsOptions.DEFAULT);
        this.getRecordIfStoredCount = sp.getMeter(BLOB_GETRECIFSTORED_COUNT, StatsOptions.DEFAULT);
        this.getRecordIfStoredTimeSeries = sp.getMeter(BLOB_GETRECIFSTORED_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getRecordIfStoredErrorCount = sp.getMeter(BLOB_GETRECIFSTORED_ERROR_COUNT, StatsOptions.DEFAULT);
        this.getRecordFromRefCount = sp.getMeter(BLOB_GETRECFROMREF_COUNT, StatsOptions.DEFAULT);
        this.getRecordFromRefTimeSeries = sp.getMeter(BLOB_GETRECFROMREF_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getRecordFromRefErrorCount = sp.getMeter(BLOB_GETRECFROMREF_ERROR_COUNT, StatsOptions.DEFAULT);
        this.getRecordForIdCount = sp.getMeter(BLOB_GETRECFORID_COUNT, StatsOptions.DEFAULT);
        this.getRecordForIdTimeSeries = sp.getMeter(BLOB_GETRECFORID_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getRecordForIdErrorCount = sp.getMeter(BLOB_GETRECFORID_ERROR_COUNT, StatsOptions.DEFAULT);

        this.getAllRecordsCount = sp.getMeter(BLOB_GETALLRECORDS_COUNT, StatsOptions.DEFAULT);
        this.getAllRecordsTimeSeries = sp.getMeter(BLOB_GETALLRECORDS_TIME, StatsOptions.TIME_SERIES_ONLY);

        this.getListIdsCount = sp.getMeter(BLOB_LISTIDS_COUNT, StatsOptions.DEFAULT);
        this.getListIdsTimeSeries = sp.getMeter(BLOB_LISTIDS_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getListIdsErrorCount = sp.getMeter(BLOB_LISTIDS_ERROR_COUNT, StatsOptions.DEFAULT);
    }

    @Override
    public void uploaded(long timeTaken, TimeUnit unit, long size) {
        uploadHisto.update(size);

        //Recording upload like this is not accurate. A more accurate way
        //would be to mark as upload or download is progressing.
        //That would however add quite a bit of overhead
        //Approach below would record an upload/download at moment when
        //it got completed. So acts like a rough approximation
        uploadSizeSeries.mark(size);
        uploadTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Uploaded {} bytes in {} ms", size, unit.toMillis(timeTaken));
    }

    @Override
    public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {
        downloadHisto.update(size);

        downloadSizeSeries.mark(size);
        downloadTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Downloaded {} - {} bytes in {} ms", blobId, size, unit.toMillis(timeTaken));
    }

    @Override
    public void uploadCompleted(String blobId) {
        uploadCount.mark();
        opsLogger.debug("Upload completed - {}", blobId);
    }

    @Override
    public void downloadCompleted(String blobId) {
        downloadCount.mark();
        opsLogger.debug("Download completed - {}", blobId);
    }

    @Override
    public void uploadFailed() {
        uploadErrorCount.mark();
    }

    @Override
    public void deleted(String blobId, long timeTaken, TimeUnit unit) {
        deleteTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Deleted {} in {} ms", blobId, unit.toMillis(timeTaken));
    }

    @Override
    public void deleteCompleted(String blobId) {
        deleteCount.mark();
        opsLogger.debug("Delete completed - {}", blobId);
    }

    @Override

    public void deleteFailed() {
        deleteErrorCount.mark();
        opsLogger.debug("Delete failed");
    }

    @Override
    public void deletedAllOlderThan(long timeTaken, TimeUnit unit, long min) {
        deleteByDateTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Deleted all records older than {} in {}", min, unit.toMillis(timeTaken));
    }

    @Override
    public void deleteAllOlderThanCompleted(int deleteCount) {
        deleteByDateCount.mark();
        opsLogger.debug("Delete all older than completed - {} records deleted", deleteCount);
    }

    @Override
    public void deleteAllOlderThanFailed(long min) {
        deleteByDateErrorCount.mark();
        opsLogger.debug("Delete all older than failed for time {}", min);
    }

    @Override
    public void recordAdded(long timeTaken, TimeUnit unit, long size) {
        addRecordSizeSeries.mark(size);
        addRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Record added - {} bytes in {} ms", size, unit.toMillis(timeTaken));
    }

    @Override
    public void addRecordCompleted(String blobId) {
        addRecordCount.mark();
        opsLogger.debug("Add record completed - {}", blobId);
    }

    @Override
    public void addRecordFailed() {
        addRecordErrorCount.mark();
        opsLogger.debug("Add record failed");
    }

    @Override
    public void getRecordCalled(long timeTaken, TimeUnit unit) {
        getRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get record called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getRecordCompleted(String blobId) {
        getRecordCount.mark();
        opsLogger.debug("Get record completed - {}", blobId);
    }

    @Override
    public void getRecordFailed(String blobId) {
        getRecordErrorCount.mark();
        opsLogger.debug("Get record failed - {}", blobId);
    }

    @Override
    public void getRecordIfStoredCalled(long timeTaken, TimeUnit unit) {
        getRecordIfStoredTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get record if stored called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getRecordIfStoredCompleted(String blobId) {
        getRecordIfStoredCount.mark();
        opsLogger.debug("Get record if stored completed - {}", blobId);
    }

    @Override
    public void getRecordIfStoredFailed(String blobId) {
        getRecordIfStoredErrorCount.mark();
        opsLogger.debug("Get record if stored failed - {}", blobId);
    }

    @Override
    public void getRecordFromReferenceCalled(long timeTaken, TimeUnit unit) {
        getRecordFromRefTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get record from reference called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getRecordFromReferenceCompleted(String reference) {
        getRecordFromRefCount.mark();
        opsLogger.debug("Get record from reference completed - {}", reference);
    }

    @Override
    public void getRecordFromReferenceFailed(String reference) {
        getRecordFromRefErrorCount.mark();
        opsLogger.debug("Get record from reference failed - {}", reference);
    }

    @Override
    public void getRecordForIdCalled(long timeTaken, TimeUnit unit) {
        getRecordForIdTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get record for id called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getRecordForIdCompleted(String blobId) {
        getRecordForIdCount.mark();
        opsLogger.debug("Get record for id completed - {}", blobId);
    }

    @Override
    public void getRecordForIdFailed(String blobId) {
        getRecordForIdErrorCount.mark();
        opsLogger.debug("Get record for id failed - {}", blobId);
    }

    @Override
    public void getAllRecordsCalled(long timeTaken, TimeUnit unit) {
        getAllRecordsTimeSeries.mark(recordedTimeUnit.convert(timeTaken,unit));
        opsLogger.debug("Get all records called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getAllRecordsCompleted() {
        getAllRecordsCount.mark();
        opsLogger.debug("Get all records completed");
    }

    @Override
    public void getAllIdentifiersCalled(long timeTaken, TimeUnit unit) {
        getListIdsTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get all identifiers called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getAllIdentifiersCompleted() {
        getListIdsCount.mark();
        opsLogger.debug("Get all identifiers completed");
    }

    @Override
    public void getAllIdentifiersFailed() {
        getListIdsErrorCount.mark();
        opsLogger.debug("Get all identifiers failed");
    }


    //~--------------------------------------< BlobStoreMBean >

    @Override
    public long getUploadTotalSize() {
        return uploadSizeSeries.getCount();
    }

    @Override
    public long getUploadCount() {
        return uploadCount.getCount();
    }

    @Override
    public long getUploadTotalSeconds() {
        return recordedTimeUnit.toSeconds(uploadTimeSeries.getCount());
    }

    @Override
    public long getUploadErrorCount() { return uploadErrorCount.getCount(); }

    @Override
    public long getDownloadTotalSize() {
        return downloadSizeSeries.getCount();
    }

    @Override
    public long getDownloadCount() {
        return downloadCount.getCount();
    }

    @Override
    public long getDownloadTotalSeconds() {
        return recordedTimeUnit.toSeconds(downloadTimeSeries.getCount());
    }

    @Override
    public long getAddRecordTotalSize() { return addRecordSizeSeries.getCount(); }

    @Override
    public long getAddRecordCount() { return addRecordCount.getCount(); }

    @Override
    public long getDeleteCount() { return deleteCount.getCount(); }

    @Override
    public long getDeleteErrorCount() { return deleteErrorCount.getCount(); }

    @Override
    public long getDeleteByDateCount() { return deleteByDateCount.getCount(); }

    @Override
    public long getDeleteByDateErrorCount() { return deleteByDateErrorCount.getCount(); }

    @Override
    public long getGetRecordCount() { return getRecordCount.getCount(); }

    @Override
    public long getGetRecordErrorCount() { return getRecordErrorCount.getCount(); }

    @Override
    public long getGetRecordIfStoredCount() { return getRecordIfStoredCount.getCount(); }

    @Override
    public long getGetRecordIfStoredErrorCount() { return getRecordIfStoredErrorCount.getCount(); }

    @Override
    public long getGetRecordFromReferenceCount() { return getRecordFromRefCount.getCount(); }

    @Override
    public long getGetRecordFromReferenceErrorCount() { return getRecordFromRefErrorCount.getCount(); }

    @Override
    public long getGetRecordForIdCount() { return getRecordForIdCount.getCount(); }

    @Override
    public long getGetRecordForIdErrorCount() { return getRecordForIdErrorCount.getCount(); }

    @Override
    public long getGetAllRecordsCount() { return getAllRecordsCount.getCount(); }

    @Override
    public long getListIdsCount() { return getListIdsCount.getCount(); }

    @Override
    public long getListIdsErrorCount() { return getListIdsErrorCount.getCount(); }

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
        return getTimeSeriesData(BLOB_UPLOADS, "Blob Uploads (bytes)");
    }

    @Override
    public CompositeData getDownloadSizeHistory() {
        return getTimeSeriesData(BLOB_DOWNLOADS, "Blob Downloads (bytes)");
    }

    @Override
    public CompositeData getUploadRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(uploadRateSeries, "Blob uploads bytes/secs");
    }

    @Override
    public CompositeData getDownloadRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(downloadRateSeries, "Blob downloads bytes/secs");
    }

    @Override
    public CompositeData getUploadCountHistory() {
        return getTimeSeriesData(BLOB_UPLOAD_COUNT, "Blob Upload Counts");
    }

    @Override
    public CompositeData getUploadErrorCountHistory() { return getTimeSeriesData(BLOB_UPLOAD_ERROR_COUNT, "Blob Upload Error Counts"); }

    @Override
    public CompositeData getDownloadCountHistory() { return getTimeSeriesData(BLOB_DOWNLOAD_COUNT, "Blob Download Counts"); }

    @Override
    public CompositeData getDeleteCountHistory() {
        return getTimeSeriesData(BLOB_DELETE_COUNT, "Blob Delete Counts");
    }

    @Override
    public CompositeData getDeleteTimeHistory() { return getTimeSeriesData(BLOB_DELETE_TIME, "Blob record deletes/sec"); }

    @Override
    public CompositeData getDeleteErrorCountHistory() { return getTimeSeriesData(BLOB_DELETE_ERROR_COUNT, "Blob Delete Error Counts"); }

    @Override
    public CompositeData getDeleteByDateCountHistory() { return getTimeSeriesData(BLOB_DELETE_BY_DATE_COUNT, "Blob Delete By Date Counts"); }

    @Override
    public CompositeData getDeleteByDateTimeHistory() { return getTimeSeriesData(BLOB_DELETE_BY_DATE_TIME, "Blob Delete By Date deletes/sec"); }

    @Override
    public CompositeData getDeleteByDateErrorCountHistory() { return getTimeSeriesData(BLOB_DELETE_BY_DATE_ERROR_COUNT, "Blob Delete By Date Error Counts"); }

    @Override
    public CompositeData getAddRecordCountHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_COUNT, "Blob Add Record Counts"); }

    @Override
    public CompositeData getAddRecordErrorCountHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_ERROR_COUNT, "Blob Add Record Error Counts"); }

    @Override
    public CompositeData getAddRecordSizeHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_SIZE, "Blob Add Record (bytes)"); }

    @Override
    public CompositeData getAddRecordRateHistory() { return TimeSeriesStatsUtil.asCompositeData(addRecordRateSeries, "Blob Add Record bytes/secs"); }

    @Override
    public long getAddRecordErrorCount() { return addRecordErrorCount.getCount(); }

    @Override
    public CompositeData getGetRecordCountHistory() { return getTimeSeriesData(BLOB_GETREC_COUNT, "Blob Get Record Counts"); }

    @Override
    public CompositeData getGetRecordErrorCountHistory() { return getTimeSeriesData(BLOB_GETREC_ERROR_COUNT, "Blob Get Record Error Counts"); }

    @Override
    public CompositeData getGetRecordTimeHistory() { return getTimeSeriesData(BLOB_GETREC_TIME, "Blob Get Record per second"); }

    @Override
    public CompositeData getGetRecordIfStoredCountHistory() { return getTimeSeriesData(BLOB_GETRECIFSTORED_COUNT, "Blob Get Record If Stored Counts"); }

    @Override
    public CompositeData getGetRecordIfStoredErrorCountHistory() { return getTimeSeriesData(BLOB_GETRECIFSTORED_ERROR_COUNT, "Blob Get Record If Stored Error Counts"); }

    @Override
    public CompositeData getGetRecordIfStoredTimeHistory() { return getTimeSeriesData(BLOB_GETRECIFSTORED_TIME, "Blob Get Record If Stored per second"); }

    @Override
    public CompositeData getGetRecordFromReferenceCountHistory() { return getTimeSeriesData(BLOB_GETRECFROMREF_COUNT, "Blob Get Record From Reference Counts"); }

    @Override
    public CompositeData getGetRecordFromReferenceErrorCountHistory() { return getTimeSeriesData(BLOB_GETRECFROMREF_ERROR_COUNT, "Blob Get Record From Reference Error Counts"); }

    @Override
    public CompositeData getGetRecordFromReferenceTimeHistory() { return getTimeSeriesData(BLOB_GETRECFROMREF_TIME, "Blob Get Record From Reference per second"); }

    @Override
    public CompositeData getGetRecordForIdCountHistory() { return getTimeSeriesData(BLOB_GETRECFORID_COUNT, "Blob Get Record for ID Counts"); }

    @Override
    public CompositeData getGetRecordForIdErrorCountHistory() { return getTimeSeriesData(BLOB_GETRECFORID_ERROR_COUNT, "Blob Get Record for ID Error Counts"); }

    @Override
    public CompositeData getGetRecordForIdTimeHistory() { return getTimeSeriesData(BLOB_GETRECFORID_TIME, "Blob Get Record for ID per second"); }

    @Override
    public CompositeData getGetAllRecordsCountHistory() { return getTimeSeriesData(BLOB_GETALLRECORDS_COUNT, "Blob Get All Records Counts"); }

    @Override
    public CompositeData getGetAllRecordsTimeHistory() { return getTimeSeriesData(BLOB_GETALLRECORDS_TIME, "Blob Get All Records per second"); }

    @Override
    public CompositeData getListIdsCountHistory() { return getTimeSeriesData(BLOB_LISTIDS_COUNT, "Blob Get All Identifiers Counts"); }

    @Override
    public CompositeData getListIdsTimeHistory() { return getTimeSeriesData(BLOB_LISTIDS_TIME, "Blob Get All Identifiers per second");}

    @Override
    public CompositeData getListIdsErrorCountHistory() { return getTimeSeriesData(BLOB_LISTIDS_ERROR_COUNT, "Blob Get All Identifiers Error Counts"); }

    private CompositeData getTimeSeriesData(String name, String desc){
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }

    private TimeSeries getAvgTimeSeries(String nameValue, String nameCounter){
        return new TimeSeriesAverage(getTimeSeries(nameValue),
                new UnitConvertingTimeSeries(getTimeSeries(nameCounter), recordedTimeUnit, TimeUnit.SECONDS));
    }

    /**
     * TimeSeries which converts a Nanonsecond based time to Seconds for
     * calculating bytes/sec rate for upload and download
     */
    private static class UnitConvertingTimeSeries implements TimeSeries {
        private final TimeSeries series;
        private final TimeUnit source;
        private final TimeUnit dest;

        public UnitConvertingTimeSeries(TimeSeries series, TimeUnit source, TimeUnit dest) {
            this.series = series;
            this.source = source;
            this.dest = dest;
        }

        @Override
        public long[] getValuePerSecond() {
            return convert(series.getValuePerSecond());
        }

        @Override
        public long[] getValuePerMinute() {
            return convert(series.getValuePerMinute());
        }

        @Override
        public long[] getValuePerHour() {
            return convert(series.getValuePerHour());
        }

        @Override
        public long[] getValuePerWeek() {
            return convert(series.getValuePerWeek());
        }

        @Override
        public long getMissingValue() {
            return 0;
        }

        private long[] convert(long[] timings){
            for (int i = 0; i < timings.length; i++) {
                timings[i] = dest.convert(timings[i], source);
            }
            return timings;
        }
    }
}
