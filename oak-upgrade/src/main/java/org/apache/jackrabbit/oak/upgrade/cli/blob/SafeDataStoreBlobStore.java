/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade.cli.blob;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.InMemoryDataRecord;
import org.apache.jackrabbit.oak.spi.blob.stats.StatsCollectingStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This implementation of the DataStoreBlobStore won't throw an exception if
 * it can't find blob with given id. The WARN message will be emitted instead
 * and the empty InputStream will be returned.
 */
public class SafeDataStoreBlobStore extends DataStoreBlobStore {

    private static final Logger log = LoggerFactory.getLogger(SafeDataStoreBlobStore.class);

    public SafeDataStoreBlobStore(DataStore delegate) {
        super(delegate);
    }

    @Override
    public String getReference(@Nonnull String encodedBlobId) {
        checkNotNull(encodedBlobId);
        String blobId = extractBlobId(encodedBlobId);
        //Reference are not created for in memory record
        if (InMemoryDataRecord.isInstance(blobId)) {
            return null;
        }

        DataRecord record;
        try {
            record = delegate.getRecordIfStored(new DataIdentifier(blobId));
            if (record != null) {
                return record.getReference();
            } else {
                log.debug("No blob found for id [{}]", blobId);
            }
        } catch (DataStoreException e) {
            log.warn("Unable to access the blobId for  [{}]", blobId, e);
        }
        return  null;
    }


    @Override
    protected InputStream getStream(String blobId) throws IOException {
        try {
            DataRecord record = getDataRecord(blobId);
            if (record == null) {
                log.warn("No blob found for id [{}]", blobId);
                return new ByteArrayInputStream(new byte[0]);
            }
            InputStream in = getDataRecord(blobId).getStream();
            if (!(in instanceof BufferedInputStream)){
                in = new BufferedInputStream(in);
            }
            return StatsCollectingStreams.wrap(stats, blobId, in);
        } catch (DataStoreException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected DataRecord getDataRecord(String blobId) throws DataStoreException {
        DataRecord id;
        if (InMemoryDataRecord.isInstance(blobId)) {
            id = InMemoryDataRecord.getInstance(blobId);
        } else {
            id = delegate.getRecordIfStored(new DataIdentifier(blobId));
        }
        return id;
    }
}