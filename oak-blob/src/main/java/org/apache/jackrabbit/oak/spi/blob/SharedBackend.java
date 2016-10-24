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
package org.apache.jackrabbit.oak.spi.blob;

import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;

/**
 */
public interface SharedBackend {
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
     * Gets the record with the specified identifier
     *
     * @param id the record identifier
     * @return the metadata DataRecord
     */
    DataRecord getRecord(DataIdentifier id) throws DataStoreException;


    /**
     * Returns identifiers of all records that exists in backend.
     *
     * @return iterator consisting of all identifiers
     * @throws DataStoreException
     */
    Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException;


    /**
     * Returns a list of all DataRecords
     *
     * @return iterator over DataRecords
     * @throws DataStoreException
     */
    Iterator<DataRecord> getAllRecords() throws DataStoreException;


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
     * Close backend and release resources like database connection if any.
     *
     * @throws DataStoreException
     */
    void close() throws DataStoreException;

    /**
     * Delete record identified by identifier. No-op if identifier not found.
     *
     * @param identifier
     * @throws DataStoreException
     */
    void deleteRecord(DataIdentifier identifier) throws DataStoreException;

    /**
     * Adds a metadata record with the specified name
     *
     * @param input the record input stream
     * @param name the name
     * @throws org.apache.jackrabbit.core.data.DataStoreException
     */
    void addMetadataRecord(final InputStream input, final String name) throws DataStoreException;

    /**
     * Adds a metadata record with the specified name
     *
     * @param input the record file
     * @param name the name
     * @throws org.apache.jackrabbit.core.data.DataStoreException
     */
    void addMetadataRecord(final File input, final String name) throws DataStoreException;

    /**
     * Gets the metadata of the specified name.
     *
     * @param name the name of the record
     * @return the metadata DataRecord
     */
    DataRecord getMetadataRecord(String name);

    /**
     * Gets all the metadata with a specified prefix.
     *
     * @param prefix the prefix of the records to retrieve
     * @return list of all the metadata DataRecords
     */
    List<DataRecord> getAllMetadataRecords(String prefix);

    /**
     * Deletes the metadata record with the specified name
     *
     * @param name the name of the record
     * @return boolean to indicate success of deletion
     */
    boolean deleteMetadataRecord(String name);

    /**
     * Deletes all the metadata records with the specified prefix.
     *
     * @param prefix the prefix of the record
     */
    void deleteAllMetadataRecords(String prefix);

    /**
     * Initialize
     */
    void init() throws DataStoreException;
}
