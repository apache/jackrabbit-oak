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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;

/**
 * Test in memory DS to store the contents with an increasing time
 */
public class TimeLapsedDataStore implements DataStore, MultiDataStoreAware, SharedDataStore, DataRecordAccessProvider {
    public static final int MIN_RECORD_LENGTH = 50;

    private final long startTime;
    private Clock clock;
    Map<String, DataRecord> store;
    Map<String, DataRecord> metadata;
    Map<String, String> uploadTokens;

    public TimeLapsedDataStore(Clock clock) {
        this.startTime = clock.getTime();
        this.clock = clock;
        store = Maps.newHashMap();
        metadata = Maps.newHashMap();
        uploadTokens = Maps.newHashMap();
    }

    protected Clock getClock() {
        return clock;
    }

    @Override public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        if (store.containsKey(identifier.toString())) {
            return getRecord(identifier);
        }
        return null;
    }

    @Override public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        return store.get(identifier.toString());
    }

    @Override public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        return getRecord(new DataIdentifier(reference));
    }

    @Override public DataRecord addRecord(InputStream stream) throws DataStoreException {
        try {
            byte[] data = IOUtils.toByteArray(stream);
            String id = getIdForInputStream(new ByteArrayInputStream(data));
            TestRecord rec = new TestRecord(id, data, clock.getTime());
            store.put(id, rec);
            BlobGCTest.log.info("Blob created {} with timestamp {}", rec.id, rec.lastModified);
            return rec;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }

    }

    @Override public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return  Iterators.transform(store.keySet().iterator(), input -> new DataIdentifier(input));
    }

    @Override public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        store.remove(identifier.toString());
    }

    /***************************************** SharedDataStore ***************************************/

    @Override public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        try {
            byte[] data = IOUtils.toByteArray(stream);
            TestRecord rec = new TestRecord(name, data, clock.getTime());
            metadata.put(name, rec);
            BlobGCTest.log.info("Metadata created {} with timestamp {}", rec.id, rec.lastModified);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override public void addMetadataRecord(File f, String name) throws DataStoreException {
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream(f);
            addMetadataRecord(fstream, name);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(fstream);
        }
    }

    @Override public DataRecord getMetadataRecord(String name) {
        return metadata.get(name);
    }

    @Override public boolean metadataRecordExists(String name) {
        return metadata.containsKey(name);
    }

    @Override public List<DataRecord> getAllMetadataRecords(String prefix) {
        List<DataRecord> recs = new ArrayList<>();
        Iterator<Map.Entry<String, DataRecord>> iter = metadata.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, DataRecord> entry = iter.next();
            if (entry.getKey().startsWith(prefix)) {
                recs.add(entry.getValue());
            }
        }
        return recs;
    }

    @Override public boolean deleteMetadataRecord(String name) {
        metadata.remove(name);
        if (!metadata.containsKey(name)) {
            return true;
        }
        return false;
    }

    @Override public void deleteAllMetadataRecords(String prefix) {
        List<String> recs = new ArrayList<>();
        Iterator<Map.Entry<String, DataRecord>> iter = metadata.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, DataRecord> entry = iter.next();
            if (entry.getKey().startsWith(prefix)) {
                recs.add(entry.getKey());
            }
        }

        for(String key: recs) {
            metadata.remove(key);
        }
    }

    @Override public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        return store.values().iterator();
    }

    @Override public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
        return store.get(id.toString());
    }

    @Override public Type getType() {
        return Type.SHARED;
    }

    /**************************** DataRecordAccessProvider *************************/

    @Override public @Nullable URI getDownloadURI(@NotNull DataIdentifier identifier,
        @NotNull DataRecordDownloadOptions downloadOptions) {
        return null;
    }

    @Override
    public @Nullable DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordUploadException {
        return initiateDataRecordUpload(maxUploadSizeInBytes, maxNumberOfURIs, DataRecordUploadOptions.DEFAULT);
    }

    @Override
    public @Nullable DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final DataRecordUploadOptions options)
            throws IllegalArgumentException, DataRecordUploadException {
        String upToken = UUID.randomUUID().toString();
        Random rand = new Random();
        InputStream stream = randomStream(rand.nextInt(1000), 100);
        byte[] data = new byte[0];
        try {
            data = IOUtils.toByteArray(stream);
        } catch (IOException e) {
            throw new DataRecordUploadException(e);
        }
        TestRecord rec = new TestRecord(upToken, data, clock.getTime());
        store.put(upToken, rec);

        DataRecordUpload uploadRec = new DataRecordUpload() {
            @Override public @NotNull String getUploadToken() {
                return upToken;
            }

            @Override public long getMinPartSize() {
                return maxUploadSizeInBytes;
            }

            @Override public long getMaxPartSize() {
                return maxUploadSizeInBytes;
            }

            @Override public @NotNull Collection<URI> getUploadURIs() {
                return Collections.EMPTY_LIST;
            }
        };
        return uploadRec;
    }

    @Override public @NotNull DataRecord completeDataRecordUpload(@NotNull String uploadToken)
        throws IllegalArgumentException, DataRecordUploadException, DataStoreException {
        return store.get(uploadToken);
    }

    class TestRecord implements DataRecord {
        String id;
        byte[] data;
        long lastModified;

        public TestRecord(String id, byte[] data, long lastModified) {
            this.id = id;
            this.data = data;
            this.lastModified = lastModified;
        }

        @Override public DataIdentifier getIdentifier() {
            return new DataIdentifier(id);
        }

        @Override public String getReference() {
            return id;
        }

        @Override public long getLength() throws DataStoreException {
            return data.length;
        }

        @Override public InputStream getStream() throws DataStoreException {
            return new ByteArrayInputStream(data);
        }

        @Override public long getLastModified() {
            return lastModified;
        }
    }

    private String getIdForInputStream(final InputStream in)
        throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
        try {
            IOUtils.copyLarge(in, output);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(in);
        }
        return encodeHexString(digest.digest());
    }

    /*************************************** No Op ***********************/
    @Override public void init(String homeDir) throws RepositoryException {
    }

    @Override public void updateModifiedDateOnAccess(long before) {
    }

    @Override public int deleteAllOlderThan(long min) throws DataStoreException {
        return 0;
    }

    @Override public int getMinRecordLength() {
        return MIN_RECORD_LENGTH;
    }

    @Override public void close() throws DataStoreException {
    }

    @Override public void clearInUse() {
    }
}
