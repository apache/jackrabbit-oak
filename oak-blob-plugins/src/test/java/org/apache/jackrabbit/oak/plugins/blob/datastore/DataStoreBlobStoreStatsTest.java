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
        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withReadDelay(1000));

        String blobId = dsbs.writeBlob(getTestInputStream());

        long downloadCount = stats.getDownloadCount();
        long downloadTotalSize = stats.getDownloadTotalSize();
        long downloadCountLastMinute = sum((long[]) stats.getDownloadCountHistory().get("per second"));
        long downloadAmountLastMinute = sum((long[]) stats.getDownloadSizeHistory().get("per second"));
        long downloadTimeLastMinute = sum((long[]) stats.getDownloadRateHistory().get("per second"));

        byte[] buffer = new byte[BLOB_LEN];
        dsbs.readBlob(blobId, 0, buffer, 0, BLOB_LEN);

        assertEquals(downloadCount + 1, stats.getDownloadCount());
        assertEquals(downloadTotalSize + BLOB_LEN, stats.getDownloadTotalSize());
        assertEquals(downloadCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getDownloadCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertEquals(downloadAmountLastMinute + BLOB_LEN,
                waitForMetric(input -> sum((long[])input.getDownloadSizeHistory().get("per second")),
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
        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withWriteDelay(1000));

        long uploadCount = stats.getUploadCount();
        long uploadTotalSize = stats.getUploadTotalSize();
        long uploadCountLastMinute = sum((long[]) stats.getUploadCountHistory().get("per second"));
        long uploadAmountLastMinute = sum((long[]) stats.getUploadSizeHistory().get("per second"));
        long uploadTimeLastMinute = sum((long[]) stats.getUploadRateHistory().get("per second"));

        dsbs.writeBlob(getTestInputStream());

        assertEquals(uploadCount + 1, stats.getUploadCount());
        assertEquals(uploadTotalSize + BLOB_LEN, stats.getUploadTotalSize());
        assertEquals(uploadCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getUploadCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertEquals(uploadAmountLastMinute + BLOB_LEN,
                waitForMetric(input -> sum((long[])input.getUploadSizeHistory().get("per second")),
                        stats, (long) BLOB_LEN, 0L).longValue());
        // TODO fix
//        assertTrue(uploadTimeLastMinute <
//                waitForNonzeroMetric(input -> sum((long[])input.getUploadRateHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSWriteBlobErrorStats() throws IOException, RepositoryException {
        // BLOB_UPLOAD_ERROR_COUNT

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnAddRecord(true));

        long writeBlobErrorCount = stats.getUploadErrorCount();

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

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withWriteDelay(1000));

        long addRecordCount = stats.getAddRecordCount();
        long addRecordSize = stats.getAddRecordTotalSize();
        long addRecordCountLastMinute = sum((long[]) stats.getAddRecordCountHistory().get("per second"));
        long addRecordSizeLastMinute = sum((long[]) stats.getAddRecordSizeHistory().get("per second"));
        long addRecordTimeLastMinute = sum((long[]) stats.getAddRecordRateHistory().get("per second"));

        dsbs.addRecord(getTestInputStream());
        dsbs.addRecord(getTestInputStream(), new BlobOptions());

        assertEquals(addRecordCount + 2, stats.getAddRecordCount());
        assertEquals(addRecordSize + BLOB_LEN*2, stats.getAddRecordTotalSize());
        assertEquals(addRecordCountLastMinute + 2,
                waitForMetric(input -> sum((long[])input.getAddRecordCountHistory().get("per second")),
                        stats, 2L, 0L).longValue());
        assertEquals(addRecordSizeLastMinute + BLOB_LEN*2,
                waitForMetric(input -> sum((long[])input.getAddRecordSizeHistory().get("per second")),
                        stats, (long) BLOB_LEN*2, 0L).longValue());
        //TODO Fix this assertion
//        assertTrue(addRecordTimeLastMinute <
//                waitForNonzeroMetric(input -> sum((long[])input.getAddRecordRateHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSAddRecordErrorStats() throws IOException, RepositoryException {
        // BLOB_ADD_RECORD_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnAddRecord(true));

        long addRecordErrorCount = stats.getAddRecordErrorCount();

        try { dsbs.addRecord(getTestInputStream()); }
        catch (DataStoreException e) { }
        try { dsbs.addRecord(getTestInputStream(), new BlobOptions()); }
        catch (DataStoreException e) { }

        assertEquals(addRecordErrorCount + 2, stats.getAddRecordErrorCount());
    }

    @Test
    public void testDSBSGetRecordStats() throws IOException, RepositoryException {
        // BLOB_GETREC_COUNT, BLOB_GETREC_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withGetRecordDelay(1000));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecCount = stats.getGetRecordCount();
        long getRecCountLastMinute = sum((long[])stats.getGetRecordCountHistory().get("per second"));
        long getRecTimeLastMinute = sum((long[])stats.getGetRecordTimeHistory().get("per second"));

        dsbs.getRecord(rec.getIdentifier());

        assertEquals(getRecCount+1, stats.getGetRecordCount());
        assertEquals(getRecCountLastMinute,
                waitForMetric(input -> sum((long[])input.getGetRecordCountHistory().get("per second")),
                        stats, 2L, 0L).longValue());
        assertTrue(getRecTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getGetRecordTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSGetRecordNotFoundStats() {
        // BLOB_GETREC_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordErrorStats() throws IOException, RepositoryException {
        // BLOB_GETREC_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord(true));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecErrorCount = stats.getGetRecordErrorCount();

        try { dsbs.getRecord(rec.getIdentifier()); }
        catch (DataStoreException e) { }

        assertEquals(getRecErrorCount + 1, stats.getGetRecordErrorCount());
    }

    @Test
    public void testDSBSGetRecordIfStoredStats() throws IOException, RepositoryException {
        // BLOB_GETRECIFSTORED_COUNT, BLOB_GETRECIFSTORED_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withGetRecordDelay(1000));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecIfStoredCount = stats.getGetRecordIfStoredCount();
        long getRecIfStoredCountLastMinute = sum((long[])stats.getGetRecordIfStoredCountHistory().get("per second"));
        long getRecIfStoredTimeLastMinute = sum((long[])stats.getGetRecordIfStoredTimeHistory().get("per second"));

        dsbs.getRecordIfStored(rec.getIdentifier());

        assertEquals(getRecIfStoredCount + 1, stats.getGetRecordIfStoredCount());
        assertEquals(getRecIfStoredCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getGetRecordIfStoredCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertTrue(getRecIfStoredTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getGetRecordIfStoredTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSGetRecordIfStoredErrorStats() throws IOException, RepositoryException {
        // BLOB_GETRECIFSTORED_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord(true));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecIfStoredErrorCount = stats.getGetRecordIfStoredErrorCount();

        try { dsbs.getRecordIfStored(rec.getIdentifier()); }
        catch (DataStoreException e) { }

        assertEquals(getRecIfStoredErrorCount + 1, stats.getGetRecordIfStoredErrorCount());
    }

    @Test
    public void testDSBSGetRecordByReferenceStats() throws IOException, RepositoryException {
        // BLOB_GETRECBYREF_COUNT, BLOB_GET_RECBYREF_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withGetRecordDelay(1000));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecFromRefCount = stats.getGetRecordFromReferenceCount();
        long getRecFromRefCountLastMinute = sum((long[])stats.getGetRecordFromReferenceCountHistory().get("per second"));
        long getRecFromRefTimeLastMinute = sum((long[])stats.getGetRecordFromReferenceTimeHistory().get("per second"));

        dsbs.getRecordFromReference(rec.getReference());

        assertEquals(getRecFromRefCount + 1, stats.getGetRecordFromReferenceCount());
        assertEquals(getRecFromRefCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getGetRecordFromReferenceCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertTrue(getRecFromRefTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getGetRecordFromReferenceTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSGetRecordByReferenceNotFoundStats() {
        // BLOB_GETRECBYREF_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordByReferenceErrorStats() throws IOException, RepositoryException {
        // BLOB_GETRECBYREF_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord(true));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecFromRefErrorCount = stats.getGetRecordFromReferenceErrorCount();

        try { dsbs.getRecordFromReference(rec.getReference()); }
        catch (DataStoreException e) { }

        assertEquals(getRecFromRefErrorCount + 1, stats.getGetRecordFromReferenceErrorCount());
    }

    @Test
    public void testDSBSGetRecordForIdStats() throws IOException, RepositoryException {
        // BLOB_GETRECFORID_COUNT, BLOB_GETRECFORID_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withGetRecordDelay(1000));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecForIdCount = stats.getGetRecordForIdCount();
        long getRecForIdCountLastMinute = sum((long[])stats.getGetRecordForIdCountHistory().get("per second"));
        long getRecForIdTimeLastMinute = sum((long[])stats.getGetRecordForIdTimeHistory().get("per second"));

        dsbs.getRecordForId(rec.getIdentifier());

        assertEquals(getRecForIdCount + 1, stats.getGetRecordForIdCount());
        assertEquals(getRecForIdCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getGetRecordForIdCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertTrue(getRecForIdTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getGetRecordForIdTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSGetRecordForIdNotFoundStats() {
        // BLOB_GETRECFORID_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordForIdErrorStats() throws IOException, RepositoryException {
        // BLOB_GETRECFORID_ERRORS

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withErrorOnGetRecord(true));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecForIdErrorCount = stats.getGetRecordForIdErrorCount();

        try { dsbs.getRecordForId(rec.getIdentifier()); }
        catch (DataStoreException e) { }

        assertEquals(getRecForIdErrorCount + 1, stats.getGetRecordForIdErrorCount());
    }

    @Test
    public void testDSBSGetAllRecordsStats() throws IOException, RepositoryException {
        // BLOB_GETALLRECS_COUNT, BLOB_GETALLRECS_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withGetRecordDelay(1000));

        long getAllRecordsCount = stats.getGetAllRecordsCount();
        long getAllRecordsCountLastMinute = sum((long[]) stats.getGetAllRecordsCountHistory().get("per second"));
        long getAllRecordsTimeLastMinute = sum((long[]) stats.getGetAllRecordsTimeHistory().get("per second"));

        dsbs.getAllRecords();

        assertEquals(getAllRecordsCount + 1, stats.getGetAllRecordsCount());
        assertEquals(getAllRecordsCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getGetAllRecordsCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertTrue(getAllRecordsTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getGetAllRecordsTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSDeleteRecordStats() throws Exception {
        // BLOB_DELETE_COUNT, BLOB_DELETE_TIME

        DataStoreBlobStore dsbs = setupDSBS(new DataStoreBuilder().withDeleteRecordDelay(1010));
        DataRecord record = dsbs.addRecord(getTestInputStream());
        List<String> chunkIds = Lists.newArrayList(record.getIdentifier().toString());
        long modifiedBefore = Instant.now().plusSeconds(86400).toEpochMilli();

        long deleteCount = stats.getDeleteCount();
        long deleteCountLastMinute = sum((long[]) stats.getDeleteCountHistory().get("per second"));
        long deleteTimeLastMinute = sum((long[]) stats.getDeleteTimeHistory().get("per second"));

        assertTrue(idInDsbs(record.getIdentifier(), dsbs));
        assertTrue(dsbs.deleteChunks(chunkIds, modifiedBefore));
        assertFalse(idInDsbs(record.getIdentifier(), dsbs));

        assertEquals(deleteCount+1, stats.getDeleteCount());
        assertEquals(deleteCountLastMinute+1,
                waitForMetric(input -> sum((long[])input.getDeleteCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertTrue(deleteTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getDeleteTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSDeleteRecordErrorStats() {
        // BLOB_DELETE_ERRORS
    }

    @Test
    public void testDSBSDeleteAllOlderThanStats() {
        // BLOB_DELETEBYDATE_COUNT, BLOB_DELETEBYDATE_TIME
    }

    @Test
    public void testDSBSDeleteAllOlderThanErrorStats() {
        // BLOB_DELETEBYDATE_ERRORS
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

    private DataStoreBlobStore setupDSBS(DataStoreBuilder dsBuilder) throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = new DataStoreBlobStore(setupDS(dsBuilder));
        dsbs.setBlobStatsCollector(stats);
        return dsbs;
    }


    private static class DataStoreBuilder {
        private int readDelay = 0;
        private int writeDelay = 0;
        private int deleteRecordDelay = 0;
        private int getRecordDelay = 0;

        private boolean generateErrorOnAddRecord = false;
        private boolean generateErrorOnGetRecord = false;

        DataStoreBuilder withReadDelay(int delay) {
            readDelay = delay;
            return this;
        }

        DataStoreBuilder withWriteDelay(int delay) {
            writeDelay = delay;
            return this;
        }

        DataStoreBuilder withDeleteRecordDelay(int delay) {
            deleteRecordDelay = delay;
            return this;
        }

        DataStoreBuilder withGetRecordDelay(int delay) {
            getRecordDelay = delay;
            return this;
        }

        DataStoreBuilder withErrorOnAddRecord(boolean doError) {
            generateErrorOnAddRecord = doError;
            return this;
        }

        DataStoreBuilder withErrorOnGetRecord(boolean doError) {
            generateErrorOnGetRecord = doError;
            return this;
        }

        OakFileDataStore build() {
            if (readDelay > 0) {
                return new ReadDelayedDataStore(readDelay);
            }
            else if (writeDelay > 0) {
                return new WriteDelayedDataStore(writeDelay);
            }
            else if (deleteRecordDelay > 0) {
                return new DeleteRecordDelayedDataStore(deleteRecordDelay);
            }
            else if (getRecordDelay > 0) {
                return new GetRecordDelayedDataStore(getRecordDelay);
            }
            else if (generateErrorOnAddRecord) {
                return new AddRecordErrorDataStore();
            }
            else if (generateErrorOnGetRecord) {
                return new GetRecordErrorDataStore();
            }
            return new OakFileDataStore();
        }
    }

    private static class DelayableFileDataStore extends OakFileDataStore {
        private int delay;

        DelayableFileDataStore(int delay) {
            super();
            this.delay = delay;
        }

        private void delay() {
            try { Thread.sleep(delay); } catch (InterruptedException e) { }
        }

        DataRecord addRecordDelayed(InputStream is) throws DataStoreException {
            delay();
            return super.addRecord(is);
        }

        void deleteRecordDelayed(DataIdentifier identifier) throws DataStoreException {
            delay();
            super.deleteRecord(identifier);
        }

        DataRecord getRecordDelayed(DataIdentifier identifier) throws DataStoreException {
            delay();
            return super.getRecord(identifier);
        }

        DataRecord getRecordIfStoredDelayed(DataIdentifier identifier) throws DataStoreException {
            delay();
            return super.getRecordIfStored(identifier);
        }

        DataRecord getRecordFromReferenceDelayed(String reference) throws DataStoreException {
            delay();
            return super.getRecordFromReference(reference);
        }

        DataRecord getRecordForIdDelayed(DataIdentifier identifier) throws DataStoreException {
            delay();
            return super.getRecordForId(identifier);
        }

        Iterator<DataRecord> getAllRecordsDelayed() {
            delay();
            return super.getAllRecords();
        }
    }

    private static class ReadDelayedDataStore extends DelayableFileDataStore {
        ReadDelayedDataStore(int delay) {
            super(delay);
        }

        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return getRecordDelayed(identifier);
        }
    }

    private static class WriteDelayedDataStore extends DelayableFileDataStore {
        WriteDelayedDataStore(int delay) {
            super(delay);
        }
    }

    private static class DeleteRecordDelayedDataStore extends DelayableFileDataStore {
        DeleteRecordDelayedDataStore(int delay) {
            super(delay);
        }

        @Override
        public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            deleteRecordDelayed(identifier);
        }
    }

    private static class GetRecordDelayedDataStore extends DelayableFileDataStore {
        GetRecordDelayedDataStore(int delay) {
            super(delay);
        }

        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return getRecordDelayed(identifier);
        }

        @Override
        public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
            return getRecordIfStoredDelayed(identifier);
        }

        @Override
        public DataRecord getRecordFromReference(String reference) throws DataStoreException {
            return getRecordFromReferenceDelayed(reference);
        }

        @Override
        public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
            return getRecordForIdDelayed(identifier);
        }

        @Override
        public Iterator<DataRecord> getAllRecords() {
            return getAllRecordsDelayed();
        }
    }

    private static class ErrorGeneratingFileDataStore extends OakFileDataStore {
        private DataStoreException ex = new DataStoreException("Test-generated exception");

        DataRecord addRecordWithError(InputStream is) throws DataStoreException {
            throw ex;
        }

        DataRecord getRecordWithError(DataIdentifier identifier) throws DataStoreException {
            throw ex;
        }

        DataRecord getRecordIfStoredWithError(DataIdentifier identifier) throws DataStoreException {
            throw ex;
        }

        DataRecord getRecordFromReferenceWithError(String reference) throws DataStoreException {
            throw ex;
        }

        DataRecord getRecordForIdWithError(DataIdentifier identifier) throws DataStoreException {
            throw ex;
        }
    }

    private static class AddRecordErrorDataStore extends ErrorGeneratingFileDataStore {
        @Override
        public DataRecord addRecord(InputStream is) throws DataStoreException {
            return addRecordWithError(is);
        }
    }

    private static class GetRecordErrorDataStore extends ErrorGeneratingFileDataStore {
        @Override
        public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
            return getRecordWithError(identifier);
        }

        @Override
        public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
            return getRecordIfStoredWithError(identifier);
        }

        @Override
        public DataRecord getRecordFromReference(String reference) throws DataStoreException {
            return getRecordFromReferenceWithError(reference);
        }

        @Override
        public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
            return getRecordForIdWithError(identifier);
        }
    }
}
