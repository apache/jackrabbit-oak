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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;

/**
 * Interface to be implemented by a shared data store.
 */
public interface SharedDataStore {
    /**
     * Explicitly identifies the type of the data store
     */
    enum Type {
        SHARED, DEFAULT
    }

    /**
     * Adds the root record.
     * 
     * @param stream the stream
     * @param name the name of the root record
     * @throws DataStoreException the data store exception
     */
    void addMetadataRecord(InputStream stream, String name)
            throws DataStoreException;

    /**
     * Adds the root record.
     *
     * @param f the file
     * @param name the name of the root record
     * @throws DataStoreException the data store exception
     */
    void addMetadataRecord(File f, String name)
        throws DataStoreException;

    /**
     * Retrieves the metadata record with the given name
     *
     * @param name the name of the record
     * @return
     */
    DataRecord getMetadataRecord(String name);

    /**
     * Gets the all root records.
     * 
     * @return the all root records
     */
    List<DataRecord> getAllMetadataRecords(String prefix);

    /**
     * Deletes the root record represented by the given parameters.
     * 
     * @param name the name of the root record
     * @return success/failure
     */
    boolean deleteMetadataRecord(String name);

    /**
     * Deletes all records matching the given prefix.
     * 
     * @param prefix metadata type identifier
     */
    void deleteAllMetadataRecords(String prefix);

    /**
     * Retrieved an iterator over all DataRecords.
     *
     * @return iterator over DataRecords
     */
    Iterator<DataRecord> getAllRecords() throws DataStoreException;

    /**
     * Retrieves the record for the given identifier
     *
     * @param id the if of the record
     * @return data record
     */
    DataRecord getRecordForId(DataIdentifier id) throws DataStoreException;

    /**
     * Gets the type.
     * 
     * @return the type
     */
    Type getType();
}

