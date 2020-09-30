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

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.spi.blob.stats.StatsCollectingStreams;
import org.jetbrains.annotations.NotNull;

/**
 * Class used to test BlobStoreStats in {@link DataStoreBlobStoreStatsTest}.
 *
 * This class creates a data store that can generate delays in data store calls
 * or generate errors from data store calls as needed.  This allows for more
 * complete testing of metrics that are being measured in
 * {@link DataStoreBlobStore}.
 *
 * Use the {@link BlobStoreStatsTestableFileDataStoreBuilder} to build an
 * instance.  The provided "wither" methods should be used with the builder to
 * specify which delay or error behaviors are needed, if any.  Then use the
 * builder to build an {@link OakFileDataStore} subclass that behaves just like
 * a regular OakFileDataStore but introduces the requested delays and/or errors
 * for certain types of behaviors (e.g. reads, writes, etc.).
 */
public class BlobStoreStatsTestableFileDataStore extends OakFileDataStore implements DataRecordAccessProvider, TypedDataStore {
    private int readDelay = 0;
    private int writeDelay = 0;
    private int deleteDelay = 0;
    private int listDelay = 0;
    private int initUploadDelay = 0;
    private int completeUploadDelay = 0;
    private int getDownloadDelay = 0;

    private boolean withReadError = false;
    private boolean withWriteError = false;
    private boolean withDeleteError = false;
    private boolean withListError = false;
    private boolean withInitUploadError = false;
    private boolean withCompleteUploadError = false;
    private boolean withGetDownloadError = false;

    private BlobStatsCollector stats = null;

    private DataStoreException ex = new DataStoreException("Test-generated Exception");

    private BlobStoreStatsTestableFileDataStore(int readDelay, int writeDelay, int deleteDelay, int listDelay,
                                                int initUploadDelay, int completeUploadDelay, int getDownloadDelay,
                                                boolean withReadError, boolean withWriteError,
                                                boolean withDeleteError, boolean withListError,
                                                boolean withInitUploadError, boolean withCompleteUploadError,
                                                boolean withGetDownloadError, BlobStatsCollector stats) {
        this.readDelay = readDelay;
        this.writeDelay = writeDelay;
        this.deleteDelay = deleteDelay;
        this.listDelay = listDelay;
        this.initUploadDelay = initUploadDelay;
        this.completeUploadDelay = completeUploadDelay;
        this.getDownloadDelay = getDownloadDelay;
        this.withReadError= withReadError;
        this.withWriteError = withWriteError;
        this.withDeleteError = withDeleteError;
        this.withListError = withListError;
        this.withInitUploadError = withInitUploadError;
        this.withCompleteUploadError = withCompleteUploadError;
        this.withGetDownloadError = withGetDownloadError;
        this.stats = stats;
    }

    public static BlobStoreStatsTestableFileDataStoreBuilder getBuilder() {
        return new BlobStoreStatsTestableFileDataStoreBuilder();
    }

    public static class BlobStoreStatsTestableFileDataStoreBuilder {
        private static final int DELAY_DEFAULT = 50;

        private int readDelay = 0;
        private int writeDelay = 0;
        private int deleteDelay = 0;
        private int listDelay = 0;
        private int initBlobUploadDelay = 0;
        private int completeBlobUploadDelay = 0;
        private int getDownloadURIDelay = 0;

        private boolean generateErrorOnAddRecord = false;
        private boolean generateErrorOnGetRecord = false;
        private boolean generateErrorOnDeleteRecord = false;
        private boolean generateErrorOnListIds = false;
        private boolean generateErrorOnInitBlobUpload = false;
        private boolean generateErrorOnCompleteBlobUpload = false;
        private boolean generateErrorOnGetDownloadURI = false;

        private BlobStatsCollector stats = null;

        BlobStoreStatsTestableFileDataStoreBuilder withReadDelay() {
            return withReadDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withReadDelay(int delay) {
            readDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withWriteDelay() {
            return withWriteDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withWriteDelay(int delay) {
            writeDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withDeleteDelay() {
            return withDeleteDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withDeleteDelay(int delay) {
            deleteDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withListDelay() {
            return withListDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withListDelay(int delay) {
            listDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withInitBlobUploadDelay() {
            return withInitBlobUploadDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withInitBlobUploadDelay(int delay) {
            initBlobUploadDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withCompleteBlobUploadDelay() {
            return withCompleteBlobUploadDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withCompleteBlobUploadDelay(int delay) {
            completeBlobUploadDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withGetDownloadURIDelay() {
            return withGetDownloadURIDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withGetDownloadURIDelay(int delay) {
            getDownloadURIDelay = delay;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnGetRecord() {
            return withErrorOnGetRecord(true).withReadDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnGetRecord(boolean withError) {
            generateErrorOnGetRecord = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnAddRecord() {
            return withErrorOnAddRecord(true).withWriteDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnAddRecord(boolean withError) {
            generateErrorOnAddRecord = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnDeleteRecord() {
            return withErrorOnDeleteRecord(true).withDeleteDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnDeleteRecord(boolean withError) {
            generateErrorOnDeleteRecord = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnList() {
            return withErrorOnList(true).withListDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnList(boolean withError) {
            generateErrorOnListIds = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnInitBlobUpload() {
            return withErrorOnInitBlobUpload(true).withInitBlobUploadDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnInitBlobUpload(boolean withError) {
            generateErrorOnInitBlobUpload = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnCompleteBlobUpload() {
            return withErrorOnCompleteBlobUpload(true).withCompleteBlobUploadDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnCompleteBlobUpload(boolean withError) {
            generateErrorOnCompleteBlobUpload = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnGetDownloadURI() {
            return withErrorOnGetDownloadURI(true).withGetDownloadURIDelay(DELAY_DEFAULT);
        }

        BlobStoreStatsTestableFileDataStoreBuilder withErrorOnGetDownloadURI(boolean withError) {
            generateErrorOnGetDownloadURI = withError;
            return this;
        }

        BlobStoreStatsTestableFileDataStoreBuilder withStatsCollector(BlobStatsCollector stats) {
            this.stats = stats;
            return this;
        }

        OakFileDataStore build() {
            if (0 == readDelay &&
                    0 == writeDelay &&
                    0 == deleteDelay &&
                    0 == listDelay &&
                    0 == initBlobUploadDelay &&
                    0 == completeBlobUploadDelay &&
                    0 == getDownloadURIDelay &&
                    ! generateErrorOnAddRecord &&
                    ! generateErrorOnGetRecord &&
                    ! generateErrorOnDeleteRecord &&
                    ! generateErrorOnListIds &&
                    ! generateErrorOnInitBlobUpload &&
                    ! generateErrorOnCompleteBlobUpload &&
                    ! generateErrorOnGetDownloadURI &&
                    null == stats) {
                return new OakFileDataStore();
            }
            return new BlobStoreStatsTestableFileDataStore(readDelay,
                    writeDelay,
                    deleteDelay,
                    listDelay,
                    initBlobUploadDelay,
                    completeBlobUploadDelay,
                    getDownloadURIDelay,
                    generateErrorOnGetRecord,
                    generateErrorOnAddRecord,
                    generateErrorOnDeleteRecord,
                    generateErrorOnListIds,
                    generateErrorOnInitBlobUpload,
                    generateErrorOnCompleteBlobUpload,
                    generateErrorOnGetDownloadURI,
                    stats);
        }
    }

    protected void delay(int delay) {
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
            }
        }
    }

    protected void err(boolean withError) throws DataStoreException {
        if (withError) throw ex;
    }

    protected void forceErr(boolean withError) {
        if (withError) throw new RuntimeException(ex);
    }

    public static class ReadDelayedDataRecord implements DataRecord {
        private DataRecord internalRecord;
        private BlobStatsCollector stats;
        private long startNanos;

        private ReadDelayedDataRecord(DataRecord record, BlobStatsCollector stats, long startNanos) {
            this.internalRecord = record;
            this.stats = stats;
            this.startNanos = startNanos;
        }

        public static ReadDelayedDataRecord wrap(DataRecord record, BlobStatsCollector stats, long startNanos) {
            return new ReadDelayedDataRecord(record, stats, startNanos);
        }

        @Override public DataIdentifier getIdentifier() { return internalRecord.getIdentifier(); }
        @Override public String getReference() { return internalRecord.getReference(); }
        @Override public long getLength() throws DataStoreException { return internalRecord.getLength(); }
        @Override public long getLastModified() { return internalRecord.getLastModified(); }

        @Override
        public InputStream getStream() throws DataStoreException {
            return null != stats ?
                    StatsCollectingStreams.wrap(stats, internalRecord.getIdentifier().toString(), internalRecord.getStream(), startNanos) :
                    internalRecord.getStream();
        }
    }

    @Override
    public DataRecord addRecord(InputStream is) throws DataStoreException {
        delay(writeDelay);
        err(withWriteError);
        return super.addRecord(is);
    }

    @Override
    public DataRecord addRecord(InputStream is, BlobOptions options) throws DataStoreException {
        delay(writeDelay);
        err(withWriteError);
        return super.addRecord(is);
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        delay(deleteDelay);
        err(withDeleteError);
        super.deleteRecord(identifier);
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        delay(readDelay);
        err(withReadError);
        return super.getRecord(identifier);
    }

    @Override
    public int deleteAllOlderThan(long min) {
        delay(deleteDelay);
        forceErr(withDeleteError);
        return super.deleteAllOlderThan(min);
    }

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        long start = System.nanoTime();
        delay(readDelay);
        err(withReadError);
        return ReadDelayedDataRecord.wrap(super.getRecordIfStored(identifier), stats, start);
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        delay(readDelay);
        err(withReadError);
        return super.getRecordFromReference(reference);
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
        delay(readDelay);
        err(withReadError);
        return super.getRecordForId(identifier);
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        delay(listDelay);
        return super.getAllRecords();
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() {
        delay(listDelay);
        forceErr(withListError);
        return super.getAllIdentifiers();
    }

    @Override
    public void addMetadataRecord(InputStream is, String name) throws DataStoreException {
        delay(writeDelay);
        err(withWriteError);
        super.addMetadataRecord(is, name);
    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        delay(writeDelay);
        err(withWriteError);
        super.addMetadataRecord(f, name);
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        delay(readDelay);
        forceErr(withReadError);
        return super.getMetadataRecord(name);
    }

    @Override
    public boolean metadataRecordExists(String name) {
        delay(readDelay);
        forceErr(withReadError);
        return super.metadataRecordExists(name);
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        delay(listDelay);
        forceErr(withListError);
        return super.getAllMetadataRecords(prefix);
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        delay(deleteDelay);
        forceErr(withDeleteError);
        return super.deleteMetadataRecord(name);
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        delay(deleteDelay);
        forceErr(withDeleteError);
        super.deleteAllMetadataRecords(prefix);
    }

    @Override
    public DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs) throws IllegalArgumentException {
        return initiateDataRecordUpload(maxUploadSizeInBytes, maxNumberOfURIs, DataRecordUploadOptions.DEFAULT);
    }

    @Override
    public DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final DataRecordUploadOptions options)
            throws IllegalArgumentException {
        delay(initUploadDelay);
        if (withInitUploadError) throw new IllegalArgumentException();
        return new DataRecordUpload() {
            @Override public @NotNull String getUploadToken() { return null; }
            @Override public long getMinPartSize() { return 0; }
            @Override public long getMaxPartSize() { return 0; }
            @Override public @NotNull
            Collection<URI> getUploadURIs() { return null; }
        };
    }

    @NotNull
    @Override
    public DataRecord completeDataRecordUpload(String uploadToken) throws IllegalArgumentException {
        delay(completeUploadDelay);
        if (withCompleteUploadError) throw new IllegalArgumentException();
        return InMemoryDataRecord.getInstance("fake record".getBytes());
    }

    @Override
    public URI getDownloadURI(DataIdentifier identifier, DataRecordDownloadOptions downloadOptions) {
        delay(getDownloadDelay);
        if (withGetDownloadError) return null;
        return URI.create("https://jackrabbit.apache.org/oak/docs/index.html");
    }
}
