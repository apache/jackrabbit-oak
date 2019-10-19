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

    private static final String BLOB_ADD_METADATA_RECORD_COUNT = "BLOB_ADD_METADATA_RECORD_COUNT";
    private static final String BLOB_ADD_METADATA_RECORD_TIME = "BLOB_ADD_METADATA_RECORD_TIME";
    private static final String BLOB_ADD_METADATA_RECORD_ERROR_COUNT = "BLOB_ADD_METADATA_RECORD_ERROR_COUNT";

    private static final String BLOB_GET_METADATA_RECORD_COUNT = "BLOB_GET_METADATA_RECORD_COUNT";
    private static final String BLOB_GET_METADATA_RECORD_TIME = "BLOB_GET_METADATA_RECORD_TIME";
    private static final String BLOB_GET_METADATA_RECORD_ERROR_COUNT = "BLOB_GET_METADATA_RECORD_ERROR_COUNT";

    private static final String BLOB_GET_ALL_METADATA_RECORDS_COUNT = "BLOB_GET_ALL_METADATA_RECORDS_COUNT";
    private static final String BLOB_GET_ALL_METADATA_RECORDS_TIME = "BLOB_GET_ALL_METADATA_RECORDS_TIME";
    private static final String BLOB_GET_ALL_METADATA_RECORDS_ERROR_COUNT = "BLOB_GET_ALL_METADATA_RECORDS_ERROR_COUNT";

    private static final String BLOB_METADATA_RECORD_EXISTS_COUNT = "BLOB_METADATA_RECORD_EXISTS_COUNT";
    private static final String BLOB_METADATA_RECORD_EXISTS_TIME = "BLOB_METADATA_RECORD_EXISTS_TIME";
    private static final String BLOB_METADATA_RECORD_EXISTS_ERROR_COUNT = "BLOB_METADATA_RECORD_EXISTS_ERROR_COUNT";

    private static final String BLOB_DELETE_METADATA_RECORD_COUNT = "BLOB_DELETE_METADATA_RECORD_COUNT";
    private static final String BLOB_DELETE_METADATA_RECORD_TIME = "BLOB_DELETE_METADATA_RECORD_TIME";
    private static final String BLOB_DELETE_METADATA_RECORD_ERROR_COUNT = "BLOB_DELETE_METADATA_RECORD_ERROR_COUNT";

    private static final String BLOB_DELETE_ALL_METADATA_RECORDS_COUNT = "BLOB_DELETE_ALL_METADATA_RECORDS_COUNT";
    private static final String BLOB_DELETE_ALL_METADATA_RECORDS_TIME = "BLOB_DELETE_ALL_METADATA_RECORDS_TIME";
    private static final String BLOB_DELETE_ALL_METADATA_RECORDS_ERROR_COUNT = "BLOB_DELETE_ALL_METADATA_RECORDS_ERROR_COUNT";

    private static final String BLOB_INIT_DIRECT_UPLOAD_COUNT = "BLOB_INIT_DIRECT_UPLOAD_COUNT";
    private static final String BLOB_INIT_DIRECT_UPLOAD_TIME = "BLOB_INIT_DIRECT_UPLOAD_TIME";
    private static final String BLOB_INIT_DIRECT_UPLOAD_ERROR_COUNT = "BLOB_INIT_DIRECT_UPLOAD_ERROR_COUNT";
    private static final String BLOB_COMPLETE_DIRECT_UPLOAD_COUNT = "BLOB_COMPLETE_DIRECT_UPLOAD_COUNT";
    private static final String BLOB_COMPLETE_DIRECT_UPLOAD_TIME = "BLOB_COMPLETE_DIRECT_UPLOAD_TIME";
    private static final String BLOB_COMPLETE_DIRECT_UPLOAD_ERROR_COUNT = "BLOB_COMPLETE_DIRECT_UPLOAD_ERROR_COUNT";
    private static final String BLOB_GET_DIRECT_DOWNLOAD_URI_COUNT = "BLOB_GET_DIRECT_DOWNLOAD_URI_COUNT";
    private static final String BLOB_GET_DIRECT_DOWNLOAD_URI_TIME = "BLOB_GET_DIRECT_DOWNLOAD_URI_TIME";
    private static final String BLOB_GET_DIRECT_DOWNLOAD_URI_ERROR_COUNT = "BLOB_DIRECT_DBA_DOWNLOAD_URI_ERROR_COUNT";



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

    private final MeterStats addMetadataRecordCount;
    private final MeterStats addMetadataRecordTimeSeries;
    private final MeterStats addMetadataRecordErrorCount;

    private final MeterStats getMetadataRecordCount;
    private final MeterStats getMetadataRecordTimeSeries;
    private final MeterStats getMetadataRecordErrorCount;
    private final MeterStats getAllMetadataRecordsCount;
    private final MeterStats getAllMetadataRecordsTimeSeries;
    private final MeterStats getAllMetadataRecordsErrorCount;

    private final MeterStats metadataRecordExistsCount;
    private final MeterStats metadataRecordExistsTimeSeries;
    private final MeterStats metadataRecordExistsErrorCount;

    private final MeterStats deleteMetadataRecordCount;
    private final MeterStats deleteMetadataRecordTimeSeries;
    private final MeterStats deleteMetadataRecordErrorCount;
    private final MeterStats deleteAllMetadataRecordsCount;
    private final MeterStats deleteAllMetadataRecordsTimeSeries;
    private final MeterStats deleteAllMetadataRecordsErrorCount;

    private final MeterStats getInitBlobUploadCount;
    private final MeterStats getInitBlobUploadTimeSeries;
    private final MeterStats getInitBlobUploadErrorCount;
    private final MeterStats getCompleteBlobUploadCount;
    private final MeterStats getCompleteBlobUploadTimeSeries;
    private final MeterStats getCompleteBlobUploadErrorCount;
    private final MeterStats getGetBlobDownloadURICount;
    private final MeterStats getGetBlobDownloadURITimeSeries;
    private final MeterStats getGetBlobDownloadURIErrorCount;

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

        this.addMetadataRecordCount = sp.getMeter(BLOB_ADD_METADATA_RECORD_COUNT, StatsOptions.DEFAULT);
        this.addMetadataRecordTimeSeries = sp.getMeter(BLOB_ADD_METADATA_RECORD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.addMetadataRecordErrorCount = sp.getMeter(BLOB_ADD_METADATA_RECORD_ERROR_COUNT, StatsOptions.DEFAULT);

        this.getMetadataRecordCount = sp.getMeter(BLOB_GET_METADATA_RECORD_COUNT, StatsOptions.DEFAULT);
        this.getMetadataRecordTimeSeries = sp.getMeter(BLOB_GET_METADATA_RECORD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getMetadataRecordErrorCount = sp.getMeter(BLOB_GET_METADATA_RECORD_ERROR_COUNT, StatsOptions.DEFAULT);
        this.getAllMetadataRecordsCount = sp.getMeter(BLOB_GET_ALL_METADATA_RECORDS_COUNT, StatsOptions.DEFAULT);
        this.getAllMetadataRecordsTimeSeries = sp.getMeter(BLOB_GET_ALL_METADATA_RECORDS_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getAllMetadataRecordsErrorCount = sp.getMeter(BLOB_GET_ALL_METADATA_RECORDS_ERROR_COUNT, StatsOptions.DEFAULT);

        this.metadataRecordExistsCount = sp.getMeter(BLOB_METADATA_RECORD_EXISTS_COUNT, StatsOptions.DEFAULT);
        this.metadataRecordExistsTimeSeries = sp.getMeter(BLOB_METADATA_RECORD_EXISTS_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.metadataRecordExistsErrorCount = sp.getMeter(BLOB_METADATA_RECORD_EXISTS_ERROR_COUNT, StatsOptions.DEFAULT);

        this.deleteMetadataRecordCount = sp.getMeter(BLOB_DELETE_METADATA_RECORD_COUNT, StatsOptions.DEFAULT);
        this.deleteMetadataRecordTimeSeries = sp.getMeter(BLOB_DELETE_METADATA_RECORD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.deleteMetadataRecordErrorCount = sp.getMeter(BLOB_DELETE_METADATA_RECORD_ERROR_COUNT, StatsOptions.DEFAULT);
        this.deleteAllMetadataRecordsCount = sp.getMeter(BLOB_DELETE_ALL_METADATA_RECORDS_COUNT, StatsOptions.DEFAULT);
        this.deleteAllMetadataRecordsTimeSeries = sp.getMeter(BLOB_DELETE_ALL_METADATA_RECORDS_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.deleteAllMetadataRecordsErrorCount = sp.getMeter(BLOB_DELETE_ALL_METADATA_RECORDS_ERROR_COUNT, StatsOptions.DEFAULT);

        this.getInitBlobUploadCount = sp.getMeter(BLOB_INIT_DIRECT_UPLOAD_COUNT, StatsOptions.DEFAULT);
        this.getInitBlobUploadTimeSeries = sp.getMeter(BLOB_INIT_DIRECT_UPLOAD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getInitBlobUploadErrorCount = sp.getMeter(BLOB_INIT_DIRECT_UPLOAD_ERROR_COUNT, StatsOptions.DEFAULT);
        this.getCompleteBlobUploadCount = sp.getMeter(BLOB_COMPLETE_DIRECT_UPLOAD_COUNT, StatsOptions.DEFAULT);
        this.getCompleteBlobUploadTimeSeries = sp.getMeter(BLOB_COMPLETE_DIRECT_UPLOAD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getCompleteBlobUploadErrorCount = sp.getMeter(BLOB_COMPLETE_DIRECT_UPLOAD_ERROR_COUNT, StatsOptions.DEFAULT);
        this.getGetBlobDownloadURICount = sp.getMeter(BLOB_GET_DIRECT_DOWNLOAD_URI_COUNT, StatsOptions.DEFAULT);
        this.getGetBlobDownloadURITimeSeries = sp.getMeter(BLOB_GET_DIRECT_DOWNLOAD_URI_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getGetBlobDownloadURIErrorCount = sp.getMeter(BLOB_GET_DIRECT_DOWNLOAD_URI_ERROR_COUNT, StatsOptions.DEFAULT);
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

    @Override
    public void metadataRecordAdded(long timeTaken, TimeUnit unit) {
        addMetadataRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Metadata record added - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void addMetadataRecordCompleted(String name) {
        addMetadataRecordCount.mark();
        opsLogger.debug("Add metadata record named {} completed", name);
    }

    @Override
    public void addMetadataRecordFailed(String name) {
        addMetadataRecordErrorCount.mark();
        opsLogger.debug("Add metadata record named {} failed", name);
    }

    @Override
    public void getMetadataRecordCalled(long timeTaken, TimeUnit unit) {
        getMetadataRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get metadata record called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getMetadataRecordCompleted(String name) {
        getMetadataRecordCount.mark();
        opsLogger.debug("Get metadata record completed");
    }

    @Override
    public void getMetadataRecordFailed(String name) {
        getMetadataRecordErrorCount.mark();
        opsLogger.debug("Get metadata record failed");
    }

    @Override
    public void getAllMetadataRecordsCalled(long timeTaken, TimeUnit unit) {
        getAllMetadataRecordsTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get all metadata records called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getAllMetadataRecordsCompleted(String prefix) {
        getAllMetadataRecordsCount.mark();
        opsLogger.debug("Get all metadata records for prefix {} completed", prefix);
    }

    @Override
    public void getAllMetadataRecordsFailed(String prefix) {
        getAllMetadataRecordsErrorCount.mark();
        opsLogger.debug("Get all metadata records for prefix {} failed", prefix);
    }

    @Override
    public void metadataRecordExistsCalled(long timeTaken, TimeUnit unit) {
        metadataRecordExistsTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Metadata record exists check - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void metadataRecordExistsCompleted(String name) {
        metadataRecordExistsCount.mark();
        opsLogger.debug("Metadata record exists check for {} completed", name);
    }

    @Override
    public void metadataRecordExistsFailed(String name) {
        metadataRecordExistsErrorCount.mark();
        opsLogger.debug("Metadata record exists check for {} failed", name);
    }

    @Override
    public void metadataRecordDeleted(long timeTaken, TimeUnit unit) {
        deleteMetadataRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Metadata record deleted - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void deleteMetadataRecordCompleted(String name) {
        deleteMetadataRecordCount.mark();
        opsLogger.debug("Metadata record name {} deleted", name);
    }

    @Override
    public void deleteMetadataRecordFailed(String name) {
        deleteMetadataRecordErrorCount.mark();
        opsLogger.debug("Metadata record name {} delete failed", name);
    }

    @Override
    public void allMetadataRecordsDeleted(long timeTaken, TimeUnit unit) {
        deleteAllMetadataRecordsTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Bulk metadata record delete - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void deleteAllMetadataRecordsCompleted(String prefix) {
        deleteAllMetadataRecordsCount.mark();
        opsLogger.debug("Bulk metadata record delete with prefix {} completed", prefix);
    }

    @Override
    public void deleteAllMetadataRecordsFailed(String prefix) {
        deleteAllMetadataRecordsErrorCount.mark();
        opsLogger.debug("Bulk metadata record delete with prefix {} failed", prefix);
    }

    @Override
    public void initiateBlobUpload(long timeTaken, TimeUnit unit, long maxSize, int maxUris) {
        getInitBlobUploadTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Initiate blob upload called with size {} and # uris {} - {} ms", maxSize, maxSize, unit.toMillis(timeTaken));
    }

    @Override
    public void initiateBlobUploadCompleted() {
        getInitBlobUploadCount.mark();
        opsLogger.debug("Initiate blob upload completed");
    }

    @Override
    public void initiateBlobUploadFailed() {
        getInitBlobUploadErrorCount.mark();
        opsLogger.debug("Initiate blob upload failed");
    }

    @Override
    public void completeBlobUpload(long timeTaken, TimeUnit unit) {
        getCompleteBlobUploadTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Complete blob upload called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void completeBlobUploadCompleted(String id) {
        getCompleteBlobUploadCount.mark();
        opsLogger.debug("Complete blob upload completed - id {}", id);
    }

    @Override
    public void completeBlobUploadFailed() {
        getCompleteBlobUploadErrorCount.mark();
        opsLogger.debug("Complete blob upload failed");
    }

    @Override
    public void getDownloadURICalled(long timeTaken, TimeUnit unit, String id) {
        getGetBlobDownloadURITimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get download URI called for id {} - {} ms", id, unit.toMillis(timeTaken));
    }

    @Override
    public void getDownloadURICompleted(String uri) {
        getGetBlobDownloadURICount.mark();
        opsLogger.debug("Get download URI completed - uri {}", uri);
    }

    @Override
    public void getDownloadURIFailed() {
        getGetBlobDownloadURIErrorCount.mark();
        opsLogger.debug("Get download URI failed");
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
    public long getAddMetadataRecordCount() { return addMetadataRecordCount.getCount(); }

    @Override
    public long getAddMetadataRecordErrorCount() { return addMetadataRecordErrorCount.getCount(); }

    @Override
    public long getGetMetadataRecordCount() { return getMetadataRecordCount.getCount(); }

    @Override
    public long getGetMetadataRecordErrorCount() { return getMetadataRecordErrorCount.getCount(); }

    @Override
    public long getGetAllMetadataRecordsCount() { return getAllMetadataRecordsCount.getCount(); }

    @Override
    public long getGetAllMetadataRecordsErrorCount() { return getAllMetadataRecordsErrorCount.getCount(); }

    @Override
    public long getMetadataRecordExistsCount() { return metadataRecordExistsCount.getCount(); }

    @Override
    public long getMetadataRecordExistsErrorCount() { return metadataRecordExistsErrorCount.getCount(); }

    @Override
    public long getDeleteMetadataRecordCount() { return deleteMetadataRecordCount.getCount(); }

    @Override
    public long getDeleteMetadataRecordErrorCount() { return deleteMetadataRecordErrorCount.getCount(); }

    @Override
    public long getDeleteAllMetadataRecordsCount() { return deleteAllMetadataRecordsCount.getCount(); }

    @Override
    public long getDeleteAllMetadataRecordsErrorCount() { return deleteAllMetadataRecordsErrorCount.getCount(); }

    @Override
    public long getInitBlobUploadCount() { return getInitBlobUploadCount.getCount(); }

    @Override
    public long getInitBlobUploadErrorCount() { return getInitBlobUploadErrorCount.getCount(); }

    @Override
    public long getCompleteBlobUploadCount() { return getCompleteBlobUploadCount.getCount(); }

    @Override
    public long getCompleteBlobUploadErrorCount() { return getCompleteBlobUploadErrorCount.getCount(); }

    @Override
    public long getGetBlobDownloadURICount() { return getGetBlobDownloadURICount.getCount(); }

    @Override
    public long getGetBlobDownloadURIErrorCount() { return getGetBlobDownloadURIErrorCount.getCount(); }

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
    public CompositeData getDeleteTimeHistory() { return getTimeSeriesData(BLOB_DELETE_TIME, "Blob Delete Times"); }

    @Override
    public CompositeData getDeleteErrorCountHistory() { return getTimeSeriesData(BLOB_DELETE_ERROR_COUNT, "Blob Delete Error Counts"); }

    @Override
    public CompositeData getDeleteByDateCountHistory() { return getTimeSeriesData(BLOB_DELETE_BY_DATE_COUNT, "Blob Delete By Date Counts"); }

    @Override
    public CompositeData getDeleteByDateTimeHistory() { return getTimeSeriesData(BLOB_DELETE_BY_DATE_TIME, "Blob Delete By Date Times"); }

    @Override
    public CompositeData getDeleteByDateErrorCountHistory() { return getTimeSeriesData(BLOB_DELETE_BY_DATE_ERROR_COUNT, "Blob Delete By Date Error Counts"); }

    @Override
    public CompositeData getAddRecordCountHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_COUNT, "Blob Add Record Counts"); }

    @Override
    public CompositeData getAddRecordErrorCountHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_ERROR_COUNT, "Blob Add Record Error Counts"); }

    @Override
    public CompositeData getAddRecordSizeHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_SIZE, "Blob Add Record (bytes)"); }

    @Override
    public CompositeData getAddRecordRateHistory() { return TimeSeriesStatsUtil.asCompositeData(addRecordRateSeries, "Blob Add Record Times"); }

    @Override
    public long getAddRecordErrorCount() { return addRecordErrorCount.getCount(); }

    @Override
    public CompositeData getGetRecordCountHistory() { return getTimeSeriesData(BLOB_GETREC_COUNT, "Blob Get Record Counts"); }

    @Override
    public CompositeData getGetRecordErrorCountHistory() { return getTimeSeriesData(BLOB_GETREC_ERROR_COUNT, "Blob Get Record Error Counts"); }

    @Override
    public CompositeData getGetRecordTimeHistory() { return getTimeSeriesData(BLOB_GETREC_TIME, "Blob Get Record Times"); }

    @Override
    public CompositeData getGetRecordIfStoredCountHistory() { return getTimeSeriesData(BLOB_GETRECIFSTORED_COUNT, "Blob Get Record If Stored Counts"); }

    @Override
    public CompositeData getGetRecordIfStoredErrorCountHistory() { return getTimeSeriesData(BLOB_GETRECIFSTORED_ERROR_COUNT, "Blob Get Record If Stored Error Counts"); }

    @Override
    public CompositeData getGetRecordIfStoredTimeHistory() { return getTimeSeriesData(BLOB_GETRECIFSTORED_TIME, "Blob Get Record If Stored Times"); }

    @Override
    public CompositeData getGetRecordFromReferenceCountHistory() { return getTimeSeriesData(BLOB_GETRECFROMREF_COUNT, "Blob Get Record From Reference Counts"); }

    @Override
    public CompositeData getGetRecordFromReferenceErrorCountHistory() { return getTimeSeriesData(BLOB_GETRECFROMREF_ERROR_COUNT, "Blob Get Record From Reference Error Counts"); }

    @Override
    public CompositeData getGetRecordFromReferenceTimeHistory() { return getTimeSeriesData(BLOB_GETRECFROMREF_TIME, "Blob Get Record From Reference Times"); }

    @Override
    public CompositeData getGetRecordForIdCountHistory() { return getTimeSeriesData(BLOB_GETRECFORID_COUNT, "Blob Get Record for ID Counts"); }

    @Override
    public CompositeData getGetRecordForIdErrorCountHistory() { return getTimeSeriesData(BLOB_GETRECFORID_ERROR_COUNT, "Blob Get Record for ID Error Counts"); }

    @Override
    public CompositeData getGetRecordForIdTimeHistory() { return getTimeSeriesData(BLOB_GETRECFORID_TIME, "Blob Get Record for ID Times"); }

    @Override
    public CompositeData getGetAllRecordsCountHistory() { return getTimeSeriesData(BLOB_GETALLRECORDS_COUNT, "Blob Get All Records Counts"); }

    @Override
    public CompositeData getGetAllRecordsTimeHistory() { return getTimeSeriesData(BLOB_GETALLRECORDS_TIME, "Blob Get All Records Times"); }

    @Override
    public CompositeData getListIdsCountHistory() { return getTimeSeriesData(BLOB_LISTIDS_COUNT, "Blob Get All Identifiers Counts"); }

    @Override
    public CompositeData getListIdsTimeHistory() { return getTimeSeriesData(BLOB_LISTIDS_TIME, "Blob Get All Identifiers Times");}

    @Override
    public CompositeData getListIdsErrorCountHistory() { return getTimeSeriesData(BLOB_LISTIDS_ERROR_COUNT, "Blob Get All Identifiers Error Counts"); }

    @Override
    public CompositeData getAddMetadataRecordCountHistory() { return getTimeSeriesData(BLOB_ADD_METADATA_RECORD_COUNT, "Blob Add Metadata Record Counts"); }

    @Override
    public CompositeData getAddMetadataRecordTimeHistory() { return getTimeSeriesData(BLOB_ADD_METADATA_RECORD_TIME, "Blob Add Metadata Record Times"); }

    @Override
    public CompositeData getAddMetadataRecordErrorCountHistory() { return getTimeSeriesData(BLOB_ADD_METADATA_RECORD_ERROR_COUNT, "Blob Add Metadata Record Error Counts"); }

    @Override
    public CompositeData getGetMetadataRecordCountHistory() { return getTimeSeriesData(BLOB_GET_METADATA_RECORD_COUNT, "Blob Get Metadata Record Counts"); }

    @Override
    public CompositeData getGetMetadataRecordTimeHistory() { return getTimeSeriesData(BLOB_GET_METADATA_RECORD_TIME, "Blob Get Metadata Record Times"); }

    @Override
    public CompositeData getGetMetadataRecordErrorCountHistory() { return getTimeSeriesData(BLOB_GET_METADATA_RECORD_ERROR_COUNT, "Blob Get Metadata Record Error Counts"); }

    @Override
    public CompositeData getGetAllMetadataRecordsCountHistory() { return getTimeSeriesData(BLOB_GET_ALL_METADATA_RECORDS_COUNT, "Blob Get All Metadata Records Counts"); }

    @Override
    public CompositeData getGetAllMetadataRecordsTimeHistory() { return getTimeSeriesData(BLOB_GET_ALL_METADATA_RECORDS_TIME, "Blob Get All Metadata Records Times"); }

    @Override
    public CompositeData getGetAllMetadataRecordsErrorCountHistory() { return getTimeSeriesData(BLOB_GET_ALL_METADATA_RECORDS_ERROR_COUNT, "Blob Get All Metadata Records Error Counts"); }

    @Override
    public CompositeData getMetadataRecordExistsCountHistory() { return getTimeSeriesData(BLOB_METADATA_RECORD_EXISTS_COUNT, "Blob Metadata Record Exists Counts"); }

    @Override
    public CompositeData getMetadataRecordExistsTimeHistory() { return getTimeSeriesData(BLOB_METADATA_RECORD_EXISTS_TIME, "Blob Metadata Record Exists Times"); }

    @Override
    public CompositeData getMetadataRecordExistsErrorCountHistory() { return getTimeSeriesData(BLOB_METADATA_RECORD_EXISTS_ERROR_COUNT, "Blob Metadata Record Exists Error Counts"); }

    @Override
    public CompositeData getDeleteMetadataRecordCountHistory() { return getTimeSeriesData(BLOB_DELETE_METADATA_RECORD_COUNT, "Blob Delete Metadata Record Counts"); }

    @Override
    public CompositeData getDeleteMetadataRecordTimeHistory() { return getTimeSeriesData(BLOB_DELETE_METADATA_RECORD_TIME, "Blob Delete Metadata Record Times"); }

    @Override
    public CompositeData getDeleteMetadataRecordErrorCountHistory() { return getTimeSeriesData(BLOB_DELETE_METADATA_RECORD_ERROR_COUNT, "Blob Delete Metadata Record Error Counts"); }

    @Override
    public CompositeData getDeleteAllMetadataRecordsCountHistory() { return getTimeSeriesData(BLOB_DELETE_ALL_METADATA_RECORDS_COUNT, "Blob Delete All Metadata Records Counts"); }

    @Override
    public CompositeData getDeleteAllMetadataRecordsTimeHistory() { return getTimeSeriesData(BLOB_DELETE_ALL_METADATA_RECORDS_TIME, "Blob Delete All Metadata Records Times"); }

    @Override
    public CompositeData getDeleteAllMetadataRecordsErrorCountHistory() { return getTimeSeriesData(BLOB_DELETE_ALL_METADATA_RECORDS_ERROR_COUNT, "Blob Delete All Metadata Records Error Counts"); }

    @Override
    public CompositeData getInitBlobUploadCountHistory() { return getTimeSeriesData(BLOB_INIT_DIRECT_UPLOAD_COUNT, "Blob Initiate Direct Upload Counts"); }

    @Override
    public CompositeData getInitBlobUploadTimeHistory() { return getTimeSeriesData(BLOB_INIT_DIRECT_UPLOAD_TIME, "Blob Initiate Direct Upload Times"); }

    @Override
    public CompositeData getInitBlobUploadErrorCountHistory() { return getTimeSeriesData(BLOB_INIT_DIRECT_UPLOAD_ERROR_COUNT, "Blob Initiate Direct Upload Error Counts"); }

    @Override
    public CompositeData getCompleteBlobUploadCountHistory() { return getTimeSeriesData(BLOB_COMPLETE_DIRECT_UPLOAD_COUNT, "Blob Complete Direct Upload Counts"); }

    @Override
    public CompositeData getCompleteBlobUploadTimeHistory() { return getTimeSeriesData(BLOB_COMPLETE_DIRECT_UPLOAD_TIME, "Blob Complete Direct Upload Times"); }

    @Override
    public CompositeData getCompleteBlobUploadErrorCountHistory() { return getTimeSeriesData(BLOB_COMPLETE_DIRECT_UPLOAD_ERROR_COUNT, "Blob Complete Direct Upload Error Counts"); }

    @Override
    public CompositeData getGetBlobDownloadURICountHistory() { return getTimeSeriesData(BLOB_GET_DIRECT_DOWNLOAD_URI_COUNT, "Blob Get Direct Download URI Counts"); }

    @Override
    public CompositeData getGetBlobDownloadURITimeHistory() { return getTimeSeriesData(BLOB_GET_DIRECT_DOWNLOAD_URI_TIME, "Blob Get Direct Download URI Times"); }

    @Override
    public CompositeData getGetBlobDownloadURIErrorCountHistory() { return getTimeSeriesData(BLOB_GET_DIRECT_DOWNLOAD_URI_ERROR_COUNT, "Blob Get Direct Download URI Error Counts"); }


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
