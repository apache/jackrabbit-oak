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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.tika.io.IOUtils;

import com.google.common.collect.Lists;

/**
 * {@link Backend} wrapper over Jackrabbit {@link DataStore} which enables using
 * a {@link CachingDataStore} for local file caching.
 */
public class DataStoreWrapperBackend implements Backend {

    /** The data store being wrapped. */
    private DataStore dataStore;

    /**
     * Instantiates a new data store wrapper backend.
     * 
     * @param dataStore
     *            the data store
     */
    public DataStoreWrapperBackend(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void init(CachingDataStore store, String homeDir, String config) throws DataStoreException {
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        return dataStore.getRecordIfStored(identifier).getStream();
    }

    @Override
    public long getLength(DataIdentifier identifier) throws DataStoreException {
        return dataStore.getRecord(identifier).getLength();
    }

    @Override
    public long getLastModified(DataIdentifier identifier) throws DataStoreException {
        return dataStore.getRecord(identifier).getLastModified();
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        InputStream stream = null;
        try {
            stream = new FileInputStream(file);
            dataStore.addRecord(stream);
        } catch (IOException io) {
            throw new DataStoreException("Error retrieving stream from : " + file.getAbsolutePath());
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return dataStore.getAllIdentifiers();
    }

    @Override
    public void touch(DataIdentifier identifier, long minModifiedDate) throws DataStoreException {
        // currently no-op
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        return (dataStore.getRecordIfStored(identifier) != null);
    }

    @Override
    public void close() throws DataStoreException {
        dataStore.close();
    }

    @Override
    public List<DataIdentifier> deleteAllOlderThan(long timestamp) throws DataStoreException {
        dataStore.deleteAllOlderThan(timestamp);
        return Lists.newArrayList();
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        if (dataStore instanceof MultiDataStoreAware) {
            ((MultiDataStoreAware) dataStore).deleteRecord(identifier);
        }
    }
}
