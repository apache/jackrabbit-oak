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

package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;

import java.io.InputStream;
import java.util.List;

/**
 * Extension to the {@link org.apache.jackrabbit.core.data.Backend} for supporting adding meta data to the underlying
 * store.
 */
public interface SharedS3Backend extends Backend {
    /**
     * Adds a metadata record with the specified name
     *
     * @param input the record input stream
     * @param name the name
     * @throws org.apache.jackrabbit.core.data.DataStoreException
     */
    void addMetadataRecord(final InputStream input, final String name) throws DataStoreException;

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
}
