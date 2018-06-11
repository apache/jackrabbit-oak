/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.spi.blob;

import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface URLWritableDataStore extends DataStore {

    void setURLWritableBinaryExpirySeconds(int seconds);

    void setURLBinaryTransferAcceleration(boolean enabled);

    /**
     * Begin a transaction to perform a direct binary upload to the cloud storage.
     *
     * @param maxUploadSizeInBytes - the largest size of the binary to be uploaded,
     *                             in bytes, based on the caller's best guess.  If
     *                             the actual size of the file to be uploaded is known,
     *                             that value should be used.
     * @param maxNumberOfURLs - the maximum number of URLs the client is able to accept.
     *                        If the client does not support multi-part uploading, this
     *                        value should be 1.  Note that the implementing class is not
     *                        required to support multi-part uploading so it may return
     *                        only a single upload URL regardless of the value passed in
     *                        for this parameter.
     * @return A {@code URLWritableDataStoreUploadContext} referencing this direct upload.
     * @throws {@code DirectBinaryAccessException} if the upload cannot be completed as
     * requested.
     */
    @Nonnull
    URLWritableDataStoreUploadContext initDirectUpload(long maxUploadSizeInBytes, int maxNumberOfURLs) throws DirectBinaryAccessException;

    /**
     * Completes the transaction to perform a direct binary upload.  This method verifies
     * that the uploaded binary has been created and is now referenceable.  For some providers
     * doing multi-part upload, this also completes the multi-part upload process if a
     * multi-part upload was performed.
     *
     * @param uploadToken The upload token identifying this direct upload transaction, as
     *                    returned in the {@code URLWritableDataStoreUploadContext} object
     *                    resulting from a call to initDirectUpload().
     * @return A DataRecord for the uploaded binary.
     * @throws {@code DirectBinaryAccessException} if the object written can't be found by S3, or
     * {@code DataStoreException} if the object written can't be found by the {@code DataStore}.
     */
    @Nullable
    DataRecord completeDirectUpload(String uploadToken) throws DirectBinaryAccessException, DataStoreException;
}
