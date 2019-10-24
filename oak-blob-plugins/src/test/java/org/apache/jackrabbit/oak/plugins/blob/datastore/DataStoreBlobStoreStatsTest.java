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
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay(500));

        String blobId1 = dsbs.writeBlob(getTestInputStream());
        String blobId2 = dsbs.writeBlob(getTestInputStream());

        long downloadCount = stats.getDownloadCount();
        long downloadTotalSize = stats.getDownloadTotalSize();
        long downloadCountLastMinute = getLastMinuteStats(stats.getDownloadCountHistory());
        long downloadAmountLastMinute = getLastMinuteStats(stats.getDownloadSizeHistory());
        long downloadTimeLastMinute = getLastMinuteStats(stats.getDownloadRateHistory());

        byte[] buffer = new byte[BLOB_LEN];
        dsbs.readBlob(blobId1, 0, buffer, 0, BLOB_LEN);
        dsbs.getInputStream(blobId2);

        assertEquals(downloadCount + 2, stats.getDownloadCount());
        assertEquals(downloadTotalSize + (BLOB_LEN*2), stats.getDownloadTotalSize());
        assertEquals(downloadCountLastMinute + 2,
                waitForMetric(input -> getLastMinuteStats(input.getDownloadCountHistory()),
                        stats, 2L, 0L).longValue());
        assertEquals(downloadAmountLastMinute + (BLOB_LEN*2),
                waitForMetric(input -> getLastMinuteStats(input.getDownloadSizeHistory()),
                        stats, (long) (BLOB_LEN*2), 0L).longValue());
        assertTrue(downloadTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getDownloadRateHistory()), stats));
    }

    @Test
    public void testDSBSReadBlobNotFoundStats() throws IOException, RepositoryException {
    }

    @Test
    public void testDSBSReadBlobErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());
        String blobId1 = dsbs.writeBlob(getTestInputStream());
        String blobId2 = dsbs.writeBlob(getTestInputStream());

        long downloadErrorCount = stats.getDownloadErrorCount();
        long downloadErrorCountLastMinute = getLastMinuteStats(stats.getDownloadErrorCountHistory());

        byte[] buffer = new byte[BLOB_LEN];
        try {
            dsbs.readBlob(blobId1, 0, buffer, 0, BLOB_LEN);
        }
        catch (IOException e) { }
        try {
            dsbs.getInputStream(blobId2);
        }
        catch (IOException e) { }

        assertEquals(downloadErrorCount + 2, stats.getDownloadErrorCount());
        assertEquals(downloadErrorCountLastMinute + 2,
                waitForMetric(input -> getLastMinuteStats(input.getDownloadErrorCountHistory()),
                        stats, 2L, 0L).longValue());
    }

    @Test
    public void testDSBSWriteBlobStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withWriteDelay(1000));

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
        assertTrue(uploadTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getUploadRateHistory()), stats));
    }

    @Test
    public void testDSBSWriteBlobErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnAddRecord());

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
    public void testDSBSAddRecordStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withWriteDelay(1000));

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
        assertTrue(addRecordTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getAddRecordRateHistory()), stats));
    }

    @Test
    public void testDSBSAddRecordErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnAddRecord());

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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay().withStatsCollector(stats));
        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getRecCount = stats.getGetRecordCount();
        long getRecCountLastMinute = getLastMinuteStats(stats.getGetRecordCountHistory());
        long getRecTimeLastMinute = getLastMinuteStats(stats.getGetRecordTimeHistory());

        long downloadTimeLastMinute = getLastMinuteStats(stats.getDownloadRateHistory());

        DataRecord record = dsbs.getRecord(rec.getIdentifier());

        assertEquals(getRecCount+1, stats.getGetRecordCount());
        assertEquals(getRecCountLastMinute,
                waitForMetric(input -> getLastMinuteStats(input.getGetRecordCountHistory()),
                        stats, 2L, 0L).longValue());
        assertTrue(getRecTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetRecordTimeHistory()), stats));

        // At this point the download time should not have changed.
        assertEquals(downloadTimeLastMinute, getLastMinuteStats(stats.getDownloadRateHistory()));

        // Consume the record's input stream
        consumeStream(record);

        assertTrue(downloadTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(stats.getDownloadRateHistory()), stats));
    }

    @Test
    public void testDSBSGetRecordNotFoundStats() {
    }

    @Test
    public void testDSBSGetRecordErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());
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
    public void testGetRecordStreamRecordsStreamStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder());

        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long downloadsLastMinute = stats.getDownloadCount();
        consumeStream(dsbs.getRecord(rec.getIdentifier()));
        consumeStream(dsbs.getRecordIfStored(rec.getIdentifier()));
        consumeStream(dsbs.getRecordFromReference(rec.getReference()));
        consumeStream(dsbs.getRecordForId(rec.getIdentifier()));

        assertEquals(downloadsLastMinute + 4, stats.getDownloadCount());
    }

    @Test
    public void testDSBSGetRecordIfStoredStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay());
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay());
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
    }

    @Test
    public void testDSBSGetRecordByReferenceErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay());
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
    }

    @Test
    public void testDSBSGetRecordForIdErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withListDelay());

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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withDeleteDelay(1010));
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnDeleteRecord());
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withDeleteDelay(1010));
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
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnDeleteRecord());
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
    public void testDSBSListIdsStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withListDelay());

        long listIdsCount = stats.getListIdsCount();
        long listIdsCountLastMinute = getLastMinuteStats(stats.getListIdsCountHistory());
        long listIdsTimeLastMinute = getLastMinuteStats(stats.getListIdsTimeHistory());

        dsbs.getAllIdentifiers();

        assertEquals(listIdsCount + 1, stats.getListIdsCount());
        assertEquals(listIdsCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getListIdsCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(listIdsTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getListIdsTimeHistory()), stats));
    }

    @Test
    public void testDSBSListIdsErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnList());

        long listIdsErrorCount = stats.getListIdsErrorCount();
        long listIdsErrorCountLastMinute = getLastMinuteStats(stats.getListIdsErrorCountHistory());

        try {
            dsbs.getAllIdentifiers();
        }
        catch (Exception e) { }

        assertEquals(listIdsErrorCount + 1, stats.getListIdsErrorCount());
        assertEquals(listIdsErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getListIdsErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSAddMetaRecStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withWriteDelay());
        File f = folder.newFile();
        try (OutputStream out = new FileOutputStream(f)) {
            IOUtils.copy(getTestInputStream(), out);
        }

        long addMetadataRecordCount = stats.getAddMetadataRecordCount();
        long addMetadataRecordCountLastMinute = getLastMinuteStats(stats.getAddMetadataRecordCountHistory());
        long addMetadataRecordTimeLastMinute = getLastMinuteStats(stats.getAddMetadataRecordTimeHistory());

        dsbs.addMetadataRecord(getTestInputStream(), "meta-1");
        dsbs.addMetadataRecord(f, "meta-1");

        assertEquals(addMetadataRecordCount + 2, stats.getAddMetadataRecordCount());
        assertEquals(addMetadataRecordCountLastMinute + 2,
                waitForMetric(input -> getLastMinuteStats(input.getAddMetadataRecordCountHistory()),
                        stats, 2L, 0L).longValue());
        assertTrue(addMetadataRecordTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getAddMetadataRecordTimeHistory()), stats));
    }

    @Test
    public void testDSBSAddMetaRecErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnAddRecord());
        File f = folder.newFile();
        try (OutputStream out = new FileOutputStream(f)) {
            IOUtils.copy(getTestInputStream(), out);
        }

        long addMetadataRecordErrorCount = stats.getAddMetadataRecordErrorCount();
        long addMetadataRecordErrorCountLastMinute = getLastMinuteStats(stats.getAddMetadataRecordErrorCountHistory());

        try { dsbs.addMetadataRecord(getTestInputStream(), "meta-1"); } catch (DataStoreException e) { }
        try { dsbs.addMetadataRecord(f, "meta-1"); } catch (DataStoreException e) { }

        assertEquals(addMetadataRecordErrorCount + 2, stats.getAddMetadataRecordErrorCount());
        assertEquals(addMetadataRecordErrorCountLastMinute + 2,
                waitForMetric(input -> getLastMinuteStats(input.getAddMetadataRecordErrorCountHistory()),
                        stats, 2L, 0L).longValue());
    }

    @Test
    public void testDSBSGetMetaRecStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay());
        String name = "meta-1";
        dsbs.addMetadataRecord(getTestInputStream(), name);

        long getMetadataRecordCount = stats.getGetMetadataRecordCount();
        long getMetadataRecordCountLastMinute = getLastMinuteStats(stats.getGetMetadataRecordCountHistory());
        long getMetadataRecordTimeLastMinute = getLastMinuteStats(stats.getGetMetadataRecordTimeHistory());

        dsbs.getMetadataRecord(name);

        assertEquals(getMetadataRecordCount + 1, stats.getGetMetadataRecordCount());
        assertEquals(getMetadataRecordCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetMetadataRecordCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getMetadataRecordTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetMetadataRecordTimeHistory()), stats));
    }

    @Test
    public void testDSBSGetMetaRecNotFoundStats() throws IOException, RepositoryException {
    }

    @Test
    public void testDSBSGetMetaRecErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());

        long getMetadataRecordErrorCount = stats.getGetMetadataRecordErrorCount();
        long getMetadataRecordErrorCountLastMinute = getLastMinuteStats(stats.getGetMetadataRecordErrorCountHistory());

        try {
            dsbs.getMetadataRecord("fake-name");
        }
        catch (Exception e) { }

        assertEquals(getMetadataRecordErrorCount + 1, stats.getGetMetadataRecordErrorCount());
        assertEquals(getMetadataRecordErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetMetadataRecordErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSGetAllMetaRecsStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withListDelay());

        long getAllMetadataRecordsCount = stats.getGetAllMetadataRecordsCount();
        long getAllMetadataRecordsCountLastMinute = getLastMinuteStats(stats.getGetAllMetadataRecordsCountHistory());
        long getAllMetadataRecordsTimeLastMinute = getLastMinuteStats(stats.getGetAllMetadataRecordsTimeHistory());

        dsbs.getAllMetadataRecords("prefix");

        assertEquals(getAllMetadataRecordsCount + 1, stats.getGetAllMetadataRecordsCount());
        assertEquals(getAllMetadataRecordsCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetAllMetadataRecordsCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getAllMetadataRecordsTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetAllMetadataRecordsTimeHistory()), stats));
    }

    @Test
    public void testDSBSGetAllMetaRecsErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnList());

        long getAllMetadataRecordsErrorCount = stats.getGetAllMetadataRecordsErrorCount();
        long getAllMetadataRecordsErrorCountLastMinute = getLastMinuteStats(stats.getGetAllMetadataRecordsErrorCountHistory());

        try {
            dsbs.getAllMetadataRecords("prefix");
        }
        catch (Exception e) { }

        assertEquals(getAllMetadataRecordsErrorCount + 1, stats.getGetAllMetadataRecordsErrorCount());
        assertEquals(getAllMetadataRecordsErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetAllMetadataRecordsErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSMetaRecExistsStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withReadDelay());

        long metadataRecordExistsCount = stats.getMetadataRecordExistsCount();
        long metadataRecordExistsCountLastMinute = getLastMinuteStats(stats.getMetadataRecordExistsCountHistory());
        long metadataRecordExistsTimeLastMinute = getLastMinuteStats(stats.getMetadataRecordExistsTimeHistory());

        dsbs.metadataRecordExists("fake-name");

        assertEquals(metadataRecordExistsCount + 1, stats.getMetadataRecordExistsCount());
        assertEquals(metadataRecordExistsCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getMetadataRecordExistsCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(metadataRecordExistsTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getMetadataRecordExistsTimeHistory()), stats));
    }

    @Test
    public void testDSBSMetaRecExistsErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetRecord());

        long metadataRecordExistsErrorCount = stats.getMetadataRecordExistsErrorCount();
        long metadataRecordExistsErrorCountLastMinute = getLastMinuteStats(stats.getMetadataRecordExistsErrorCountHistory());

        try {
            dsbs.metadataRecordExists("fake-name");
        }
        catch (Exception e) { }

        assertEquals(metadataRecordExistsErrorCount + 1, stats.getMetadataRecordExistsErrorCount());
        assertEquals(metadataRecordExistsErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getMetadataRecordExistsErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSMetaDeleteStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withDeleteDelay());
        String name = "meta-1";
        dsbs.addMetadataRecord(getTestInputStream(), name);

        long deleteMetadataRecordCount = stats.getDeleteMetadataRecordCount();
        long deleteMetadataRecordCountLastMinute = getLastMinuteStats(stats.getDeleteMetadataRecordCountHistory());
        long deleteMetadataRecordTimeLastMinute = getLastMinuteStats(stats.getDeleteMetadataRecordTimeHistory());

        dsbs.deleteMetadataRecord(name);

        assertEquals(deleteMetadataRecordCount + 1, stats.getDeleteMetadataRecordCount());
        assertEquals(deleteMetadataRecordCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteMetadataRecordCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(deleteMetadataRecordTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getDeleteMetadataRecordTimeHistory()), stats));
    }

    @Test
    public void testDSBSMetaDeleteNotFoundStats() throws IOException, RepositoryException {
    }

    @Test
    public void testDSBSMetaDeleteErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnDeleteRecord());
        String name = "meta-1";
        dsbs.addMetadataRecord(getTestInputStream(), name);

        long deleteMetadataRecordErrorCount = stats.getDeleteMetadataRecordErrorCount();
        long deleteMetadataRecordErrorCountLastMinute = getLastMinuteStats(stats.getDeleteMetadataRecordErrorCountHistory());

        try {
            dsbs.deleteMetadataRecord(name);
        }
        catch (Exception e) { }

        assertEquals(deleteMetadataRecordErrorCount + 1, stats.getDeleteMetadataRecordErrorCount());
        assertEquals(deleteMetadataRecordErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteMetadataRecordErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSMetaDeleteAllStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withDeleteDelay());
        String name = "meta-1";
        dsbs.addMetadataRecord(getTestInputStream(), name);

        long deleteAllMetadataRecordsCount = stats.getDeleteAllMetadataRecordsCount();
        long deleteAllMetadataRecordsCountLastMinute = getLastMinuteStats(stats.getDeleteAllMetadataRecordsCountHistory());
        long deleteAllMetadataRecordsTimeLastMinute = getLastMinuteStats(stats.getDeleteAllMetadataRecordsTimeHistory());

        dsbs.deleteAllMetadataRecords(name);

        assertEquals(deleteAllMetadataRecordsCount + 1, stats.getDeleteAllMetadataRecordsCount());
        assertEquals(deleteAllMetadataRecordsCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteAllMetadataRecordsCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(deleteAllMetadataRecordsTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getDeleteAllMetadataRecordsTimeHistory()), stats));
    }

    @Test
    public void testDSBSMetaDeleteAllErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnDeleteRecord());
        String name = "meta-1";
        dsbs.addMetadataRecord(getTestInputStream(), name);

        long deleteAllMetadataRecordsErrorCount = stats.getDeleteAllMetadataRecordsErrorCount();
        long deleteAllMetadataRecordsErrorCountLastMinute = getLastMinuteStats(stats.getDeleteAllMetadataRecordsErrorCountHistory());

        try {
            dsbs.deleteAllMetadataRecords(name);
        }
        catch (Exception e) { }

        assertEquals(deleteAllMetadataRecordsErrorCount + 1, stats.getDeleteAllMetadataRecordsErrorCount());
        assertEquals(deleteAllMetadataRecordsErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getDeleteAllMetadataRecordsErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSInitUploadDBAStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withInitBlobUploadDelay());

        long initBlobUploadCount = stats.getInitBlobUploadCount();
        long initBlobUploadCountLastMinute = getLastMinuteStats(stats.getInitBlobUploadCountHistory());
        long initBlobUploadTimeLastMinute = getLastMinuteStats(stats.getInitBlobUploadTimeHistory());

        dsbs.initiateBlobUpload(BLOB_LEN, 20);

        assertEquals(initBlobUploadCount + 1, stats.getInitBlobUploadCount());
        assertEquals(initBlobUploadCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getInitBlobUploadCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(initBlobUploadTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getInitBlobUploadTimeHistory()), stats));
    }

    @Test
    public void testDSBSInitUploadDBAErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnInitBlobUpload());

        long initBlobUploadErrorCount = stats.getInitBlobUploadErrorCount();
        long initBlobUploadErrorCountLastMinute = getLastMinuteStats(stats.getInitBlobUploadErrorCountHistory());

        try {
            dsbs.initiateBlobUpload(BLOB_LEN, 20);
        }
        catch (Exception e) { }

        assertEquals(initBlobUploadErrorCount + 1, stats.getInitBlobUploadErrorCount());
        assertEquals(initBlobUploadErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getInitBlobUploadErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSCompleteUploadDBAStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withCompleteBlobUploadDelay());

        long completeBlobUploadCount = stats.getCompleteBlobUploadCount();
        long completeBlobUploadCountLastMinute = getLastMinuteStats(stats.getCompleteBlobUploadCountHistory());
        long completeBlobUploadTimeLastMinute = getLastMinuteStats(stats.getCompleteBlobUploadTimeHistory());

        dsbs.completeBlobUpload("fake token");

        assertEquals(completeBlobUploadCount + 1, stats.getCompleteBlobUploadCount());
        assertEquals(completeBlobUploadCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getCompleteBlobUploadCountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(completeBlobUploadTimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getCompleteBlobUploadTimeHistory()), stats));
    }

    @Test
    public void testDSBSCompleteUploadDBAExistsStats() {
    }

    @Test
    public void testDSBSCompleteUploadDBANotFoundStats() {
    }

    @Test
    public void testDSBSCompleteUploadDBAErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnCompleteBlobUpload());

        long completeBlobUploadErrorCount = stats.getCompleteBlobUploadErrorCount();
        long completeBlobUploadErrorCountLastMinute = getLastMinuteStats(stats.getCompleteBlobUploadErrorCountHistory());

        try {
            dsbs.completeBlobUpload("fake token");
        }
        catch (IllegalArgumentException e) { }

        assertEquals(completeBlobUploadErrorCount + 1, stats.getCompleteBlobUploadErrorCount());
        assertEquals(completeBlobUploadErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getCompleteBlobUploadErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }

    @Test
    public void testDSBSDownloadGetUriDBAStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withGetDownloadURIDelay());

        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getBlobDownloadURICount = stats.getGetBlobDownloadURICount();
        long getBlobDownloadURICountLastMinute = getLastMinuteStats(stats.getGetBlobDownloadURICountHistory());
        long getBlobDownloadURITimeLastMinute = getLastMinuteStats(stats.getGetBlobDownloadURITimeHistory());

        dsbs.getDownloadURI(new BlobStoreBlob(dsbs, rec.getIdentifier().toString()), BlobDownloadOptions.DEFAULT);

        assertEquals(getBlobDownloadURICount + 1, stats.getGetBlobDownloadURICount());
        assertEquals(getBlobDownloadURICountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetBlobDownloadURICountHistory()),
                        stats, 1L, 0L).longValue());
        assertTrue(getBlobDownloadURITimeLastMinute <
                waitForNonzeroMetric(input -> getLastMinuteStats(input.getGetBlobDownloadURITimeHistory()), stats));
    }

    @Test
    public void testDSBSDownloadGetURIDBANotFoundStats() {
    }

    @Test
    public void testDSBSDownloadGetURIDBAErrorStats() throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = setupDSBS(getDSBuilder().withErrorOnGetDownloadURI());

        DataRecord rec = dsbs.addRecord(getTestInputStream());

        long getBlobDownloadURIErrorCount = stats.getGetBlobDownloadURIErrorCount();
        long getBlobDownloadURIErrorCountLastMinute = getLastMinuteStats(stats.getGetBlobDownloadURIErrorCountHistory());

        dsbs.getDownloadURI(new BlobStoreBlob(dsbs, rec.getIdentifier().toString()), BlobDownloadOptions.DEFAULT);

        assertEquals(getBlobDownloadURIErrorCount + 1, stats.getGetBlobDownloadURIErrorCount());
        assertEquals(getBlobDownloadURIErrorCountLastMinute + 1,
                waitForMetric(input -> getLastMinuteStats(input.getGetBlobDownloadURIErrorCountHistory()),
                        stats, 1L, 0L).longValue());
    }


    private InputStream getTestInputStream() {
        return new RandomInputStream(System.currentTimeMillis(), BLOB_LEN);
    }

    private DataStore setupDS(BlobStoreStatsTestableFileDataStore.BlobStoreStatsTestableFileDataStoreBuilder dsBuilder)
            throws IOException, RepositoryException {
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

    private BlobStoreStatsTestableFileDataStore.BlobStoreStatsTestableFileDataStoreBuilder getDSBuilder() {
        return BlobStoreStatsTestableFileDataStore.getBuilder();
    }

    private DataStoreBlobStore setupDSBS(BlobStoreStatsTestableFileDataStore.BlobStoreStatsTestableFileDataStoreBuilder dsBuilder)
            throws IOException, RepositoryException {
        DataStoreBlobStore dsbs = new DataStoreBlobStore(setupDS(dsBuilder));
        dsbs.setBlobStatsCollector(stats);
        return dsbs;
    }

    private void consumeStream(DataRecord record) throws IOException, DataStoreException{
        try (InputStream recordStream = record.getStream()) {
            while (recordStream.available() > 0) {
                recordStream.read();
            }
        }
    }
}
