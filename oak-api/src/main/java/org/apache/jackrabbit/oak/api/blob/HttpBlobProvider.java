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
package org.apache.jackrabbit.oak.api.blob;

import java.net.URL;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;

public interface HttpBlobProvider {
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
     * @return A {@link HttpBlobUpload} referencing this direct upload.
     * @throws {@link IllegalHttpUploadArgumentsException}
     * if the upload cannot be completed as requested, due to a mismatch between the request
     * parameters and the capabilities of the service provider or the implementation.
     */
    @Nullable
    HttpBlobUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURLs)
        throws IllegalHttpUploadArgumentsException;

    /**
     * Complete a transaction for uploading a direct binary upload to cloud storage.
     *
     * This requires the {@code uploadToken} that can be obtained as the returned
     * {@link HttpBlobUpload}from a previous call to {@link #initiateHttpUpload(long, int)}.
     * It is required to complete the transaction for an upload to be valid and complete.
     *
     * @param uploadToken the upload token from a {@link HttpBlobUpload} object returned
     *                    from a previous call to {@link #initiateHttpUpload(long, int)}.
     * @return The {@link Blob} that was created, or {@code null} if the object could not
     * be created.
     */
    @Nullable
    Blob completeHttpUpload(String uploadToken);

    /**
     * Obtain a download URL for a blob ID.  This is usually a signed URL that can be used to
     * directly download the blob corresponding to the blob ID.
     *
     * @param blobId for the blob to be downloaded.
     * @return A URL to download the blob directly.
     */
    @Nullable
    URL getHttpDownloadURL(String blobId);
}
