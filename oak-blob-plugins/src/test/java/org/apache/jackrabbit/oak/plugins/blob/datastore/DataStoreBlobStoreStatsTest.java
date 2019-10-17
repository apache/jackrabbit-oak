/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import javax.jcr.RepositoryException;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DataStoreBlobStoreStatsTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);
    private BlobStoreStats stats = new BlobStoreStats(statsProvider);

    private static int BLOB_LEN = 20*1024;

    @After
    public void shutDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testDSBSReadBlobStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay());

        String blobId = dsbs.writeBlob(getTestInputStream());

        long downloadCount = stats.getDownloadCount();
        long downloadTotalSize = stats.getDownloadTotalSize();
        long downloadCountLastMinute = getLastMinuteStats(stats.getDownloadCountHistory());
        long downloadAmountLastMinute = getLastMinuteStats(stats.getDownloadSizeHistory());
        long downloadTimeLastMinute = getLastMinuteStats(stats.getDownloadRateHistory());

        byte[] buffer = new byte[BLOB_LEN];
        dsbs.readBlob(blobId, 0, buffer, 0, BLOB_LEN);

        assertEquals(downloadCount + 1, stats.getDownloadCount());
        assertEquals(downloadTotalSize + BLOB_LEN, stats.getDownloadTotalSize());
        assertEquals(downloadCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDownloadCountHistory()),
                        stats, 1L, 0L).longValue());
        assertEquals(downloadAmountLastMinute + BLOB_LEN,
                waitForMetric(input -> getLastMinuteStats(input.getDownloadSizeHistory()),
                        stats, (long) BLOB_LEN, 0L).longValue());
        // TODO fix
//        assertTrue(downloadTimeLastMinute <
//                waitForNonzeroMetric(input -> sum((long[])input.getDownloadRateHistory().get("per second")), stats));
    }

    @Ignore
    @Test
    public void testDSBSReadBlobNotFoundStats() throws IOException, RepositoryException {
        // BLOB_DOWNLOAD_NOT_FOUND_COUNT
    }

    @Test
    public void testDSBSReadBlobErrorStats() {
        // BLOB_DOWNLOAD_ERROR_COUNT
    }

    @Test
    public void testDSBSWriteBlobStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withWriteDelay());

        long uploadCount = stats.getUploadCount();
        long uploadTotalSize = stats.getUploadTotalSize();
        long uploadCountLastMinute = getLastMinuteStats(stats.getUploadCountHistory());
        long uploadAmountLastMinute = getLastMinuteStats(stats.getUploadSizeHistory());
        long uploadTimeLastMinute = getLastMinuteStats(stats.getUploadRateHistory());

        dsbs.writeBlob(getTestInputStream());

        assertEquals(uploadCount + 1, stats.getUploadCount());
        assertEquals(uploadTotalSize + BLOB_LEN, stats.getUploadTotalSize());
        assertEquals(uploadCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getUploadCountHistory()),
                        stats, 1L, 0L).longValue());
        assertEquals(uploadAmountLastMinute + BLOB_LEN,
                waitForMetric(input -> getLastMinuteStats(input.getUploadSizeHistory()),
                        stats, (long) BLOB_LEN, 0L).longValue());
        // TODO fix
//        assertTrue(uploadTimeLastMinute <
//                waitForNonzeroMetric(input -> sum((long[])input.getUploadRateHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSWriteBlobErrorStats() throws IOException, RepositoryException {
        // BLOB_UPLOAD_ERROR_COUNT

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnAddRecord());

        long writeBlobErrorCount = stats.getUploadErrorCount();
        long writeBlobErrorsLastMinute = getLastMinuteStats(stats.getUploadErrorCountHistory());

        try { dsbs.writeBlob(getTestInputStream()); }
        catch (IOException e) { }
        try { dsbs.writeBlob(getTestInputStream(), new BlobOptions()); }
        catch (IOException e) { }
        try {
            File f = folder.newFile();
            try (OutputStream out = new FileOutputStream(f)) {
                IOUtils.copy(getTestInputStream(), out);
            }
            dsbs.writeBlob(f.getAbsolutePath());
        }
        catch (IOException e) { }

        assertEquals(writeBlobErrorCount + 3, stats.getUploadErrorCount());
        assertEquals(writeBlobErrorsLastMinute + 3,
                waitForMetric(input -> getLastMinuteStats(input.getUploadErrorCountHistory()),
                        stats, 3L, 0L).longValue());
    }

    @Test
    public void testDSBSGetInputStreamStats() {
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetInputStreamNotFoundStats() {
        // BLOB_DOWNLOAD_NOT_FOUND
    }

    @Test
    public void testDSBSGetInputStreamErrorStats() {
        // BLOB_DOWNLOAD_ERRORS
    }

    @Test
    public void testDSBSAddRecordStats() throws IOException, RepositoryException {
        // BLOB_ADD_RECORD_COUNT, BLOB_ADD_RECORD_SIZE, BLOB_ADD_RECORD_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withWriteDelay());

        long addRecordCount = stats.getAddRecordCount();
        long addRecordSize = stats.getAddRecordTotalSize();
        long addRecordCountLastMinute = getLastMinuteStats(stats.getAddRecordCountHistory());
        long addRecordSizeLastMinute = getLastMinuteStats(stats.getAddRecordSizeHistory());
        long addRecordTimeLastMinute = getLastMinuteStats(stats.getAddRecordRateHistory());

        dsbs.addRecord(getTestInputStream());
        dsbs.addRecord(getTestInputStream(), new BlobOptions());

        assertEquals(addRecordCount + 2, stats.getAddRecordCount());
        assertEquals(addRecordSize + BLOB_LEN*2, stats.getAddRecordTotalSize());
        assertEquals(addRecordCountLastMinute + 2,
                waitForMetric(input -> getLastMinuteStats(input.getAddRecordCountHistory()),
                        stats, 2L, 0L).longValue());
        assertEquals(addRecordSizeLastMinute + BLOB_LEN*2,
                waitForMetric(input -> getLastMinuteStats(input.getAddRecordSizeHistory()),
                        stats, (long) BLOB_LEN*2, 0L).longValue());
        //TODO Fix this assertion
//        assertTrue(addRecordTimeLastMinute <
//                waitForNonzeroMetric(input -> sum((long[])input.getAddRecordRateHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSAddRecordErrorStats() throws IOException, RepositoryException {
        // BLOB_ADD_RECORD_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnAddRecord());

        long addRecordErrorCount = stats.getAddRecordErrorCount();
        long addRecordErrorCountLastMinute = getLastMinuteStats(stats.getAddRecordErrorCountHistory());

        try { dsbs.addRecord(getTestInputStream()); }
        catch (DataStoreException e) { }
        try { dsbs.addRecord(getTestInputStream(), new BlobOptions()); }
        catch (DataStoreException e) { }

        assertEquals(addRecordErrorCount + 2, stats.getAddRecordErrorCount());
        assertEquals(addRecordErrorCountLastMinute + 2,
                waitForMetric(input -> getLastMinuteStats(input.getAddRecordErrorCountHistory()),
                        stats, 2L, 0L).longValue());
    }

    @Test
    public void testDSBSGetRecordStats() throws IOException, RepositoryException {
        // BLOB_GETREC_COUNT, BLOB_GETREC_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecCount = stats.getGetRecordCount();
        long getRecCountLastMinute = getLastMinuteStats(stats.getGetRecordCountHistory());
        long getRecTimeLastMinute = getLastMinuteStats(stats.getGetRecordTimeHistory());

        dsbs.getRecord(rec.getIdentifier());

        assertEquals(getRecCount+1, stats.getGetRecordCount());
        assertEquals(getRecCountLastMinute,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordCountHistory()),
                        stats, 2L, 0L).longValue());
        assertTrue(getRecTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetRecordTimeHistory()), stats));
    }

    @Test
    public void testDSBSGetRecordNotFoundStats() {
        // BLOB_GETREC_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordErrorStats() throws IOException, RepositoryException {
        // BLOB_GETREC_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecErrorCount = stats.getGetRecordErrorCount();
        long getRecErrorCountLastMinute = getLastMinuteStats(stats.getGetRecordErrorCountHistory());

        try { dsbs.getRecord(rec.getIdentifier()); }
        catch (DataStoreException e) { }

        assertEquals(getRecErrorCount + 1, stats.getGetRecordErrorCount());
        assertEquals(getRecErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSGetRecordIfStoredStats() throws IOException, RepositoryException {
        // BLOB_GETRECIFSTORED_COUNT, BLOB_GETRECIFSTORED_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecIfStoredCount = stats.getGetRecordIfStoredCount();
        long getRecIfStoredCountLastMinute = getLastMinuteStats(stats.getGetRecordIfStoredCountHistory());
        long getRecIfStoredTimeLastMinute = getLastMinuteStats(stats.getGetRecordIfStoredTimeHistory());

        dsbs.getRecordIfStored(rec.getIdentifier());

        assertEquals(getRecIfStoredCount + 1, stats.getGetRecordIfStoredCount());
        assertEquals(getRecIfStoredCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordIfStoredCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getRecIfStoredTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetRecordIfStoredTimeHistory()), stats));
    }

    @Test
    public void testDSBSGetRecordIfStoredErrorStats() throws IOException, RepositoryException {
        // BLOB_GETRECIFSTORED_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecIfStoredErrorCount = stats.getGetRecordIfStoredErrorCount();
        long getRecIfStoredErrorCountLastMinute = getLastMinuteStats(stats.getGetRecordIfStoredErrorCountHistory());

        try { dsbs.getRecordIfStored(rec.getIdentifier()); }
        catch (DataStoreException e) { }

        assertEquals(getRecIfStoredErrorCount + 1, stats.getGetRecordIfStoredErrorCount());
        assertEquals(getRecIfStoredErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordIfStoredErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSGetRecordByReferenceStats() throws IOException, RepositoryException {
        // BLOB_GETRECBYREF_COUNT, BLOB_GET_RECBYREF_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecFromRefCount = stats.getGetRecordFromReferenceCount();
        long getRecFromRefCountLastMinute = getLastMinuteStats(stats.getGetRecordFromReferenceCountHistory());
        long getRecFromRefTimeLastMinute = getLastMinuteStats(stats.getGetRecordFromReferenceTimeHistory());

        dsbs.getRecordFromReference(rec.getReference());

        assertEquals(getRecFromRefCount + 1, stats.getGetRecordFromReferenceCount());
        assertEquals(getRecFromRefCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordFromReferenceCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getRecFromRefTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetRecordFromReferenceTimeHistory()), stats));
    }

    @Test
    public void testDSBSGetRecordByReferenceNotFoundStats() {
        // BLOB_GETRECBYREF_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordByReferenceErrorStats() throws IOException, RepositoryException {
        // BLOB_GETRECBYREF_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecFromRefErrorCount = stats.getGetRecordFromReferenceErrorCount();
        long getRecFromRefErrorCountLastMinute = getLastMinuteStats(stats.getGetRecordFromReferenceErrorCountHistory());

        try { dsbs.getRecordFromReference(rec.getReference()); }
        catch (DataStoreException e) { }

        assertEquals(getRecFromRefErrorCount + 1, stats.getGetRecordFromReferenceErrorCount());
        assertEquals(getRecFromRefErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordFromReferenceErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSGetRecordForIdStats() throws IOException, RepositoryException {
        // BLOB_GETRECFORID_COUNT, BLOB_GETRECFORID_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecForIdCount = stats.getGetRecordForIdCount();
        long getRecForIdCountLastMinute = getLastMinuteStats(stats.getGetRecordForIdCountHistory());
        long getRecForIdTimeLastMinute = getLastMinuteStats(stats.getGetRecordForIdTimeHistory());

        dsbs.getRecordForId(rec.getIdentifier());

        assertEquals(getRecForIdCount + 1, stats.getGetRecordForIdCount());
        assertEquals(getRecForIdCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordForIdCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getRecForIdTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetRecordForIdTimeHistory()), stats));
    }

    @Test
    public void testDSBSGetRecordForIdNotFoundStats() {
        // BLOB_GETRECFORID_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordForIdErrorStats() throws IOException, RepositoryException {
        // BLOB_GETRECFORID_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord());
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecForIdErrorCount = stats.getGetRecordForIdErrorCount();
        long getRecForIdErrorCountLastMinute = getLastMinuteStats(stats.getGetRecordForIdErrorCountHistory());

        try { dsbs.getRecordForId(rec.getIdentifier()); }
        catch (DataStoreException e) { }

        assertEquals(getRecForIdErrorCount + 1, stats.getGetRecordForIdErrorCount());
        assertEquals(getRecForIdErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordForIdErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSGetAllRecordsStats() throws IOException, RepositoryException {
        // BLOB_GETALLRECS_COUNT, BLOB_GETALLRECS_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay());

        long getAllRecordsCount = stats.getGetAllRecordsCount();
        long getAllRecordsCountLastMinute = getLastMinuteStats(stats.getGetAllRecordsCountHistory());
        long getAllRecordsTimeLastMinute = getLastMinuteStats(stats.getGetAllRecordsTimeHistory());

        dsbs.getAllRecords();

        assertEquals(getAllRecordsCount + 1, stats.getGetAllRecordsCount());
        assertEquals(getAllRecordsCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetAllRecordsCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getAllRecordsTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetAllRecordsTimeHistory()), stats));
    }

    @Test
    public void testDSBSDeleteRecordStats() throws Exception {
        // BLOB_DELETE_COUNT, BLOB_DELETE_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withDeleteRecordDelay(1010));
        DataRecord record = dsbs.addRecord(getTestInputStream());
        List<String> chunkIds = Lists.newArrayList(record.getIdentifier().toString());
        long modifiedBefore = tomorrow();

        long deleteCount = stats.getDeleteCount();
        long deleteCountLastMinute = getLastMinuteStats(stats.getDeleteCountHistory());
        long deleteTimeLastMinute = getLastMinuteStats(stats.getDeleteTimeHistory());

        assertTrue(idInDsbs(record.getIdentifier(), dsbs));
        assertTrue(dsbs.deleteChunks(chunkIds, modifiedBefore));
        assertFalse(idInDsbs(record.getIdentifier(), dsbs));

        assertEquals(deleteCount+1, stats.getDeleteCount());
        assertEquals(deleteCountLastMinute+1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(deleteTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getDeleteTimeHistory()), stats));
    }

    @Test
    public void testDSBSDeleteRecordErrorStats() throws Exception {
        // BLOB_DELETE_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnDeleteRecord());
        DataRecord record = dsbs.addRecord(getTestInputStream());
        List<String> chunkIds = Lists.newArrayList(record.getIdentifier().toString());
        long modifiedBefore = tomorrow();

        long deleteErrorCount = stats.getDeleteErrorCount();
        long deleteErrorCountLastMinute = getLastMinuteStats(stats.getDeleteErrorCountHistory());

        try {
            dsbs.deleteChunks(chunkIds, modifiedBefore);
        }
        catch (Exception e) { }

        assertEquals(deleteErrorCount + 1, stats.getDeleteErrorCount());
        assertEquals(deleteErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSDeleteAllOlderThanStats() throws Exception {
        // BLOB_DELETEBYDATE_COUNT, BLOB_DELETEBYDATE_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withDeleteRecordDelay(1010));
        DataRecord record = dsbs.addRecord(getTestInputStream());
        long modifiedBefore = tomorrow();

        long deleteByDateCount = stats.getDeleteByDateCount();
        long deleteByDateCountLastMinute = getLastMinuteStats(stats.getDeleteByDateCountHistory());
        long deleteByDateTimeLastMinute = getLastMinuteStats(stats.getDeleteByDateTimeHistory());

        assertTrue(idInDsbs(record.getIdentifier(), dsbs));
        assertEquals(1, dsbs.deleteAllOlderThan(modifiedBefore));
        assertFalse(idInDsbs(record.getIdentifier(), dsbs));

        assertEquals(deleteByDateCount+1, stats.getDeleteByDateCount());
        assertEquals(deleteByDateCountLastMinute+1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteByDateCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(deleteByDateTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getDeleteByDateTimeHistory()), stats));
    }

    @Test
    public void testDSBSDeleteAllOlderThanErrorStats() throws IOException, RepositoryException {
        // BLOB_DELETEBYDATE_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnDeleteRecord());
        DataRecord record = dsbs.addRecord(getTestInputStream());
        long modifiedBefore = tomorrow();

        long deleteByDateErrorCount = stats.getDeleteByDateErrorCount();
        long deleteByDateErrorCountLastMinute = getLastMinuteStats(stats.getDeleteByDateErrorCountHistory());

        try {
            dsbs.deleteAllOlderThan(modifiedBefore);
        }
        catch (Exception e) { }

        assertEquals(deleteByDateErrorCount + 1, stats.getDeleteByDateErrorCount());
        assertEquals(deleteByDateErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteByDateErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSListIdsStats() {
        // BLOB_LISTIDS_COUNT, BLOB_LISTIDS_TIME
    }

    @Test
    public void testDSBSListIdsErrorStats() {
        // BLOB_LISTIDS_ERRORS
    }

    @Test
    public void testDSBSAddMetaRecStats() {
        // BLOB_METADATA_ADD_COUNT, BLOB_METADATA_ADD_TIME, BLOB_METADATA_ADD_SIZE
    }

    @Test
    public void testDSBSAddMetaRecErrorStats() {
        // BLOB_METADATA_ADD_ERRORS
    }

    @Test
    public void testDSBSGetMetaRecStats() {
        // BLOB_METADATA_GET_COUNT, BLOB_METADATA_GET_TIME

        // Then read the stream from the rec and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetMetaRecNotFoundStats() {
        // BLOB_METADATA_GET_NOT_FOUND
    }

    @Test
    public void testDSBSGetMetaRecErrorStats() {
        // BLOB_METADATA_GET_ERRORS
    }

    @Test
    public void testDSBSGetAllMetaRecsStats() {
        // BLOB_METADATA_GETALL_COUNT, BLOB_METADATA_GETALL_TIME

        // Then read the stream from one of the recs and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetAllMetaRecsErrorStats() {
        // BLOB_METADATA_GETALL_ERRORS
    }

    @Test
    public void testDSBSMetaRecExistsStats() {
        // BLOB_METADATA_EXISTS_COUNT, BLOB_METADATA_EXISTS_TIME
    }

    @Test
    public void testDSBSMetaRecExistsErrorStats() {
        // BLOB_METADATA_EXISTS_ERRORS
    }

    @Test
    public void testDSBSMetaDeleteStats() {
        // BLOB_METADATA_DELETE_COUNT, BLOB_METADATA_DELETE_TIME
    }

    @Test
    public void testDSBSMetaDeleteNotFoundStats() {
        // BLOB_METADATA_DELETE_NOT_FOUND
    }

    @Test
    public void testDSBSMetaDeleteErrorStats() {
        // BLOB_METADATA_DELETE_ERRORS
    }

    @Test
    public void testDSBSMetaDeleteAllStats() {
        // BLOB_METADATA_DELETEALL_COUNT, BLOB_METDATA_DELETEALL_TIME
    }

    @Test
    public void testDSBSMetaDeleteAllErrorStats() {
        // BLOB_METADATA_DELETEALL_ERRORS
    }

    @Test
    public void testDSBSInitUploadDBAStats() {
        // BLOB_DBA_UPLOAD_INIT_COUNT, BLOB_DBA_UPLOAD_INIT_TIME
    }

    @Test
    public void testDSBSInitUploadDBAErrorStats() {
        // BLOB_DBA_UPLOAD_INIT_ERRORS
    }

    @Test
    public void testDSBSCompleteUploadDBAStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_COUNT, BLOB_DBA_UPLOAD_COMPLETE_TIME
    }

    @Test
    public void testDSBSCompleteUploadDBAExistsStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_EXISTS
    }

    @Test
    public void testDSBSCompleteUploadDBANotFoundStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_NOT_FOUND
    }

    @Test
    public void testDSBSCompleteUploadDBAErrorStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_ERRORS
    }

    @Test
    public void testDSBSDownloadGetUriDBAStats() {
        // BLOB_DBA_DOWNLOAD_GETURI_COUNT, BLOB_DBA_DOWNLOAD_GETURI_TIME
    }

    @Test
    public void testDSBSDownloadGetURIDBANotFoundStats() {
        // BLOB_DBA_DOWNLOAD_GETURI_NOT_FOUND
    }

    @Test
    public void testDSBSDownloadGetURIDBAErrorStats() {
        // BLOB_DBA_DOWNLOAD_GETURI_ERRORS
    }


    private InputStream getTestInputStream() {
        return new RandomInputStream(System.currentTimeMillis(), BLOB_LEN);
    }

    private DataStore setupDS() throws IOException, RepositoryException {
        return setupDS(new DataStoreBuilder());
    }

    private DataStore setupDS(DataStoreBuilder dsBuilder) throws IOException, RepositoryException {
        DataStore ds = dsBuilder.build();
        File homeDir = folder.newFolder();
        ds.init(homeDir.getAbsolutePath());
        return ds;
    }

    private long sum(long[] d) {
        long result = 0L;
        for (Long l : d) {
            result += l;
        }
        return result;
    }

    private long getLastMinuteStats(CompositeData data) {
        return sum((long[]) data.get("per second"));
    }

    private <T, R> R waitForMetric(Function<T, R> f, T input, R expected, R defaultValue) {
        return waitForMetric(f, input, expected, defaultValue, 100, 1000);
    }

    private <T, R> R waitForMetric(Function<T, R> f, T input, R expected, R defaultValue, int intervalMilliseconds, int waitMilliseconds) {
        long end = System.currentTimeMillis() + waitMilliseconds;
        R output = f.apply(input);
        if (null != output && output.equals(expected)) {
            return output;
        }
        do {
            try {
                Thread.sleep(intervalMilliseconds);
            }
            catch (InterruptedException e) { }
            output = f.apply(input);
            if (null != output && output.equals(expected)){
                return output;
            }
        }
        while (System.currentTimeMillis() < end);
        return defaultValue;
    }

    private <T> Long waitForNonzeroMetric(Function<T, Long> f, T input) {
        return waitForNonzeroMetric(f, input, 100, 1000);
    }

    private <T> Long waitForNonzeroMetric(Function<T, Long> f, T input, int intervalMilliseconds, int waitMilliseconds) {
        long end = System.currentTimeMillis() + waitMilliseconds;
        Long output = f.apply(input);
        if (null != output && output > 0L) {
            return output;
        }
        do {
            try {
                Thread.sleep(intervalMilliseconds);
            }
            catch (InterruptedException e) { }
            output = f.apply(input);
            if (null != output && output > 0L) {
                return output;
            }
        }
        while (System.currentTimeMillis() < end);
        return 0L;
    }

    private boolean idInDsbs(DataIdentifier id, DataStoreBlobStore dsbs) throws DataStoreException {
        Iterator<DataIdentifier> iter = dsbs.getAllIdentifiers();
        while (iter.hasNext()) {
            if (iter.next().equals(id)) {
                return true;
            }
        }
        return false;
    }

    private long tomorrow() {
        return Instant.now().plusSeconds(86400).toEpochMilli();
    }

    private DataStoreBlobStore setupDSBS(DataStoreBuilder dsBuilder) throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = new DataStoreBlobStore(setupDS(dsBuilder));
        dsbs.setBlobStatsCollector(stats);
        return dsbs;
    }


    private static class DataStoreBuilder {
        private int readDelay = 0;
        private int writeDelay = 0;
        private int deleteRecordDelay = 0;

        private boolean generateErrorOnAddRecord = false;
        private boolean generateErrorOnGetRecord = false;
        private boolean generateErrorOnDeleteRecord = false;

        DataStoreBuilder withReadDelay() {
            return withReadDelay(1000);
        }

        DataStoreBuilder withReadDelay(int delay) {
            readDelay = delay;
            return this;
        }

        DataStoreBuilder withWriteDelay() {
            return withWriteDelay(1000);
        }

        DataStoreBuilder withWriteDelay(int delay) {
            writeDelay = delay;
            return this;
        }

        DataStoreBuilder withDeleteRecordDelay() {
            return withDeleteRecordDelay(1000);
        }

        DataStoreBuilder withDeleteRecordDelay(int delay) {
            deleteRecordDelay = delay;
            return this;
        }

        DataStoreBuilder withErrorOnGetRecord() {
            return withErrorOnGetRecord(true).withReadDelay(1000);
        }

        DataStoreBuilder withErrorOnGetRecord(boolean withError) {
            generateErrorOnGetRecord = withError;
            return this;
        }

        DataStoreBuilder withErrorOnAddRecord() {
            return withErrorOnAddRecord(true).withWriteDelay(1000);
        }

        DataStoreBuilder withErrorOnAddRecord(boolean withError) {
            generateErrorOnAddRecord = withError;
            return this;
        }

        DataStoreBuilder withErrorOnDeleteRecord() {
            return withErrorOnDeleteRecord(true).withDeleteRecordDelay(1000);
        }

        DataStoreBuilder withErrorOnDeleteRecord(boolean withError) {
            generateErrorOnDeleteRecord = withError;
            return this;
        }

        OakFileDataStore build() {
            if (readDelay > 0) {
                return generateErrorOnGetRecord ?
                        new GetRecordErrorDataStore(readDelay) :
                        new ReadDelayedDataStore(readDelay);
            }
            else if (writeDelay > 0) {
                return generateErrorOnAddRecord ?
                        new AddRecordErrorDataStore(writeDelay) :
                        new WriteDelayedDataStore(writeDelay);
            }
            else if (deleteRecordDelay > 0) {
                return generateErrorOnDeleteRecord ?
                        new DeleteRecordErrorDataStore(deleteRecordDelay) :
                        new DeleteRecordDelayedDataStore(deleteRecordDelay);
            }
            return new OakFileDataStore();
        }
    }

    private static class TestableFileDataStore extends OakFileDataStore {
        private int delay;
        private boolean withError;
        private DataStoreException ex = new DataStoreException("Test-generated Exception");

        TestableFileDataStore(int delay) {
            this(delay, false);
        }

        TestableFileDataStore(int delay, boolean withError) {
            super();
            this.delay = delay;
            this.withError = withError;
        }

        private void delay() {
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                }
            }
        }

        private void err() throws DataStoreException {
            if (withError) throw ex;
        }

        DataRecord _addRec(InputStream is) throws DataStoreException {
            delay();
            err();
            return super.addRecord(is);
        }

        void _delRec(DataIdentifier identifier) throws DataStoreException {
            delay();
            err();
            super.deleteRecord(identifier);
        }

        DataRecord _getRec(DataIdentifier identifier) throws DataStoreException {
            delay();
            err();
            return super.getRecord(identifier);
        }

        int _delAllOlderThan(long min) throws DataStoreException {
            delay();
            err();
            return super.deleteAllOlderThan(min);
        }

        DataRecord _getRecIfStored(DataIdentifier identifier) throws DataStoreException {
            delay();
            err();
            return super.getRecordIfStored(identifier);
        }

        DataRecord _getRecFromRef(String reference) throws DataStoreException {
            delay();
            err();
            return super.getRecordFromReference(reference);
        }

        DataRecord _getRecForId(DataIdentifier identifier) throws DataStoreException {
            delay();
            err();
            return super.getRecordForId(identifier);
        }

        Iterator<DataRecord> _getAllRecs() {
            delay();
            return super.getAllRecords();
        }
    }

    private static class ReadDelayedDataStore extends TestableFileDataStore {
        ReadDelayedDataStore(int delay) {
            super(delay);
        }

        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return _getRec(identifier);
        }

        @Override
        public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
            return _getRecIfStored(identifier);
        }

        @Override
        public DataRecord getRecordFromReference(String reference) throws DataStoreException {
            return _getRecFromRef(reference);
        }

        @Override
        public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
            return _getRecForId(identifier);
        }

        @Override
        public Iterator<DataRecord> getAllRecords() {
            return _getAllRecs();
        }
    }

    private static class WriteDelayedDataStore extends TestableFileDataStore {
        WriteDelayedDataStore(int delay) {
            super(delay);
        }

        @Override
        public DataRecord addRecord(InputStream is) throws DataStoreException {
            return _addRec(is);
        }
    }

    private static class DeleteRecordDelayedDataStore extends TestableFileDataStore {
        DeleteRecordDelayedDataStore(int delay) {
            super(delay);
        }

        @Override
        public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            _delRec(identifier);
        }
    }

    private static class AddRecordErrorDataStore extends TestableFileDataStore {
        AddRecordErrorDataStore(int delay) {
            super(delay, true);
        }

        @Override
        public DataRecord addRecord(InputStream is) throws DataStoreException {
            return _addRec(is);
        }
    }

    private static class GetRecordErrorDataStore extends TestableFileDataStore {
        GetRecordErrorDataStore(int delay) {
            super(delay, true);
        }

        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return _getRec(identifier);
        }

        @Override
        public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
            return _getRecIfStored(identifier);
        }

        @Override
        public DataRecord getRecordFromReference(String reference) throws DataStoreException {
            return _getRecFromRef(reference);
        }

        @Override
        public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
            return _getRecForId(identifier);
        }
    }

    private static class DeleteRecordErrorDataStore extends TestableFileDataStore {
        DeleteRecordErrorDataStore(int delay) {
            super(delay, true);
        }

        @Override
        public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            _delRec(identifier);
        }

        @Override
        public int deleteAllOlderThan(long min) {
            try {
                return _delAllOlderThan(min);
            }
            catch (DataStoreException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
