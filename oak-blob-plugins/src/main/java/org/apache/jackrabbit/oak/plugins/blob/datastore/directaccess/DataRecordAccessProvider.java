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

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface DataRecordAccessProvider {
    /**
     * Return a URI for directly reading the binary associated with the provided
     * {@link DataRecord}.
     * <p>
     * The caller must provide a {@link DataRecordDownloadOptions} instance that
     * will be used by the implementation to specify options on the download.
     * This can be obtained from a {@link BlobDownloadOptions} instance by
     * calling {@link
     * DataRecordDownloadOptions#fromBlobDownloadOptions(BlobDownloadOptions)},
     * or to accept the service provider default behavior a caller can simply
     * use {@link DataRecordDownloadOptions#DEFAULT}.
     *
     * @param identifier The {@link DataIdentifier} for the {@link DataRecord}
     *         containing the binary to be read via direct download.
     * @param downloadOptions A {@link DataRecordDownloadOptions} instance used
     *         to specify any download options that should be set on this
     *         download.
     * @return A URI for directly reading the binary, or {@code null} if the URI
     *         cannot be generated, for example if the capability is disabled by
     *         configuration or if a service provider error occurs.
     */
    @Nullable
    URI getDownloadURI(@NotNull DataIdentifier identifier,
                       @NotNull DataRecordDownloadOptions downloadOptions);

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
     * @return A {@link DataRecordUpload} referencing this direct upload,
     *         or {@code null} if the implementation does not support direct
     *         upload.
     * @throws IllegalArgumentException if the service provider or
     *         implementation cannot support the requested upload.
     * @throws DataRecordUploadException if the upload cannot be completed as
     *         requested.
     */
    @Nullable
    DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes,
                                              int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordUploadException;

    /**
     * Completes the transaction to perform a direct binary upload.  This method
     * verifies that the uploaded binary has been created and is now
     * referenceable.  For some providers doing multi-part upload, this also
     * completes the multi-part upload process if a multi-part upload was
     * performed.
     *
     * @param uploadToken The upload token identifying this direct upload
     *        transaction, as returned in the {@link DataRecordUpload}
     *        object resulting from a call to {@link
     *        #initiateDataRecordUpload(long, int)}.
     * @return A {@link DataRecord} for the uploaded binary.
     * @throws IllegalArgumentException if the {@code uploadToken} is null,
     *         empty, or otherwise invalid.
     * @throws DataRecordUploadException if the object written can't be found by
     *         the service provider.
     * @throws DataStoreException if the object written can't be found by the
     *         DataStore.
     */
    @NotNull
    DataRecord completeDataRecordUpload(@NotNull String uploadToken)
            throws IllegalArgumentException, DataRecordUploadException, DataStoreException;
}
