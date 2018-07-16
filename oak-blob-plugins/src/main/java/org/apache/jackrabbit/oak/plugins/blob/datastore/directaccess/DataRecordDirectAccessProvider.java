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
package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

import java.net.URI;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;

public interface DataRecordDirectAccessProvider {
    /**
     * Return a URI for directly reading the binary associated with the provided
     * {@link DataRecord}.
     *
     * @param identifier The {@link DataIdentifier} for the {@link DataRecord}
     *        containing the binary to be read via direct download.
     * @return A URI for directly reading the binary, or null if the URI cannot
     * be generated, for example if the capability is disabled by configuration
     * or if a service provider error occurs.
     */
    @Nullable
    URI getDownloadURI(DataIdentifier identifier);

    @Nullable
    URI getDownloadURI(DataIdentifier identifier, Properties downloadOptions);

    /**
     * Begin a transaction to perform a direct binary upload to the storage
     * location.
     *
     * @param maxUploadSizeInBytes - the largest size of the binary to be
     *        uploaded, in bytes, based on the caller's best guess.  If the
     *        actual size of the file to be uploaded is known, that value should
     *        be used.
     * @param maxNumberOfURIs - the maximum number of URIs the client is able to
     *        accept.  If the caller does not support multi-part uploading, this
     *        value should be 1.  Note that the implementing class is not
     *        required to support multi-part uploading so it may return only a
     *        single upload URI regardless of the value passed in for this
     *        parameter.  A caller may also pass in -1 to indicate that it is
     *        able to accept any number of URIs.  Any other negative number or
     *        0 may result in {@link IllegalArgumentException}.
     * @return A {@link DataRecordDirectUpload} referencing this direct upload,
     *         or {@code null} if the implementation does not support direct
     *         upload.
     * @throws {@link IllegalArgumentException} if the service provider or
     *         implementation cannot support the requested upload, or {@link
     *         DataRecordDirectUploadException} if the upload cannot be
     *         completed as requested.
     */
    @Nullable
    DataRecordDirectUpload initiateDirectUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordDirectUploadException;

    /**
     * Completes the transaction to perform a direct binary upload.  This method
     * verifies that the uploaded binary has been created and is now
     * referenceable.  For some providers doing multi-part upload, this also
     * completes the multi-part upload process if a multi-part upload was
     * performed.
     *
     * @param uploadToken The upload token identifying this direct upload
     *        transaction, as returned in the {@link DataRecordDirectUpload}
     *        object resulting from a call to {@link
     *        #initiateDirectUpload(long, int)}.
     * @return A {@link DataRecord} for the uploaded binary, or if
     * @throws {@link IllegalArgumentException} if the {@code uploadToken} is
     *         null, empty, or otherwise invalid, {@link
     *         DataRecordDirectUploadException} if the object written can't be
     *         found by the service provider, or {@link DataStoreException} if
     *         the object written can't be found by the DataStore.
     */
    @Nonnull
    DataRecord completeDirectUpload(String uploadToken)
            throws IllegalArgumentException, DataRecordDirectUploadException, DataStoreException;
}
