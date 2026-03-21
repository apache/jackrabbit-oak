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

package org.apache.jackrabbit.core.data;

import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Set;

/**
 * The interface defines the backend which can be plugged into
 * {@link CachingDataStore}.
 */
public interface Backend {

    /**
     * This method initialize backend with the configuration.
     * 
     * @param store
     *            {@link CachingDataStore}
     * @param homeDir
     *            path of repository home dir.
     * @param config
     *            path of config property file.
     * @throws DataStoreException
     */
    void init(CachingDataStore store, String homeDir, String config)
            throws DataStoreException;

    /**
     * Return inputstream of record identified by identifier.
     * 
     * @param identifier
     *            identifier of record.
     * @return inputstream of the record.
     * @throws DataStoreException
     *             if record not found or any error.
     */
    InputStream read(DataIdentifier identifier) throws DataStoreException;

    /**
     * Return length of record identified by identifier.
     * 
     * @param identifier
     *            identifier of record.
     * @return length of the record.
     * @throws DataStoreException
     *             if record not found or any error.
     */
    long getLength(DataIdentifier identifier) throws DataStoreException;

    /**
     * Return lastModified of record identified by identifier.
     * 
     * @param identifier
     *            identifier of record.
     * @return lastModified of the record.
     * @throws DataStoreException
     *             if record not found or any error.
     */
    long getLastModified(DataIdentifier identifier) throws DataStoreException;

    /**
     * Stores file to backend with identifier used as key. If key pre-exists, it
     * updates the timestamp of the key.
     * 
     * @param identifier
     *            key of the file
     * @param file
     *            file that would be stored in backend.
     * @throws DataStoreException
     *             for any error.
     */
    void write(DataIdentifier identifier, File file) throws DataStoreException;

    /**
     * Write file to backend in asynchronous mode.
     * 
     * @param identifier
     * @param file
     * @param callback
     *            Callback interface to called after upload succeed or failed.
     * @throws DataStoreException
     */
    void writeAsync(DataIdentifier identifier, File file,
            AsyncUploadCallback callback) throws DataStoreException;

    /**
     * Returns identifiers of all records that exists in backend.
     * 
     * @return iterator consisting of all identifiers
     * @throws DataStoreException
     */
    Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException;

    /**
     * This method check the existence of record in backend. Return true if
     * records exists else false. This method also touch record identified by
     * identifier if touch is true.
     * 
     * @param identifier
     * @throws DataStoreException
     */
    boolean exists(DataIdentifier identifier, boolean touch)
            throws DataStoreException;

    /**
     * This method check the existence of record in backend.
     * 
     * @param identifier
     *            identifier to be checked.
     * @return true if records exists else false.
     * @throws DataStoreException
     */
    boolean exists(DataIdentifier identifier) throws DataStoreException;
   
    /**
     * Update the lastModified of record if it's lastModified &lt; minModifiedDate.
     * 
     * @param identifier
     * @param minModifiedDate
     * @throws DataStoreException
     */
    void touch(final DataIdentifier identifier, long minModifiedDate)
            throws DataStoreException;
    
    /**
     * Update the lastModified of record if it's lastModified &lt; minModifiedDate
     * asynchronously. Result of update is passed using appropriate
     * {@link AsyncTouchCallback} methods. If identifier's lastModified &gt;
     * minModified {@link AsyncTouchCallback#onAbort(AsyncTouchResult)} is
     * called. Any exception is communicated through
     * {@link AsyncTouchCallback#onFailure(AsyncTouchResult)} . On successful
     * update of lastModified,
     * {@link AsyncTouchCallback#onSuccess(AsyncTouchResult)}
     * is invoked.
     * 
     * @param identifier
     * @param minModifiedDate
     * @param callback
     * @throws DataStoreException
     */
    void touchAsync(final DataIdentifier identifier, long minModifiedDate,
            final AsyncTouchCallback callback) throws DataStoreException;

    /**
     * Close backend and release resources like database connection if any.
     * 
     * @throws DataStoreException
     */
    void close() throws DataStoreException;

    /**
     * Delete all records which are older than timestamp.
     * 
     * @param timestamp
     * @return {@link Set} of identifiers which are deleted.
     * @throws DataStoreException
     */
    Set<DataIdentifier> deleteAllOlderThan(long timestamp)
            throws DataStoreException;

    /**
     * Delete record identified by identifier. No-op if identifier not found.
     * 
     * @param identifier
     * @throws DataStoreException
     */
    void deleteRecord(DataIdentifier identifier) throws DataStoreException;
}

