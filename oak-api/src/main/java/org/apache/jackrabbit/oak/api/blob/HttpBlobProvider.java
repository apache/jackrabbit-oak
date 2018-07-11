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
     * Begin a transaction to perform a direct binary upload to the cloud
     * storage. This method will throw a {@link IllegalHttpUploadArgumentsException}
     * if no valid upload can be arranged with the arguments specified. E.g. the
     * max upload size specified divided by the number of URLs requested
     * indicates the minimum size of each upload. If that size exceeds the
     * maximum upload size supported by the service provider, a
     * IllegalHttpUploadArgumentsException is thrown.
     * <p>
     * Each service provider has specific limitations with regard to maximum
     * upload sizes, maximum overall binary sizes, numbers of URIs in multi-part
     * uploads, etc. which can lead to IllegalHttpUploadArgumentsException being
     * thrown. You should consult the documentation for your specific service
     * provider for details.
     * <p>
     * Beyond service provider limitations, the implementation may also choose
     * to enforce its own limitations and may throw this exception based on
     * those limitations. Configuration may also be used to set limitations so
     * this exception may be thrown when configuration parameters are exceeded.
     *
     * @param maxUploadSizeInBytes the largest size of the binary to be
     *         uploaded, in bytes, based on the caller's best guess.  If the
     *         actual size of the file to be uploaded is known, that value
     *         should be used.
     * @param maxNumberOfURLs the maximum number of URLs the client is
     *         able to accept. If the client does not support multi-part
     *         uploading, this value should be 1. Note that the implementing
     *         class is not required to support multi-part uploading so it may
     *         return only a single upload URL regardless of the value passed in
     *         for this parameter.  If the client is able to accept any number
     *         of URLs, a value of -1 may be passed in to indicate that the
     *         implementation is free to return as many URLs as it desires.
     * @return A {@link HttpBlobUpload} referencing this direct upload, or
     *         {@code null} if the underlying implementation doesn't support
     *         direct HTTP uploading.
     * @throws IllegalArgumentException if {@code maxUploadSizeInBytes}
     *         or {@code maxNumberOfURLs} is not either a positive value or -1.
     * @throws IllegalHttpUploadArgumentsException if the upload cannot
     *         be completed as requested, due to a mismatch between the request
     *         parameters and the capabilities of the service provider or the
     *         implementation.
     */
    @Nullable
    HttpBlobUpload initiateHttpUpload(long maxUploadSizeInBytes,
                                      int maxNumberOfURLs)
            throws IllegalArgumentException,
            IllegalHttpUploadArgumentsException;

    /**
     * Complete a transaction for uploading a direct binary upload to cloud
     * storage.
     * <p>
     * This requires the {@code uploadToken} that can be obtained from the
     * returned {@link HttpBlobUpload} from a previous call to {@link
     * #initiateHttpUpload(long, int)}. This token is required to complete
     * the transaction for an upload to be valid and complete.  The token
     * includes encoded data about the transaction along with a signature
     * that will be verified by the implementation.
     *
     * @param uploadToken the upload token from a {@link HttpBlobUpload}
     *         object returned from a previous call to {@link
     *         #initiateHttpUpload(long, int)}.
     * @return The {@link Blob} that was created, or {@code null} if the object
     *         could not be created.
     * @throws IllegalArgumentException if the {@code uploadToken} cannot be
     *         parsed or is otherwise invalid, e.g. if the included signature
     *         does not match.
     */
    @Nullable
    Blob completeHttpUpload(String uploadToken);

    /**
     * Obtain a download URL for a {@link Blob). This is usually a signed URL
     * that can be used to directly download the blob corresponding to the
     * provided {@link Blob}.
     *
     * @param blob for the {@link Blob} to be downloaded.
     * @return A URL to download the blob directly or {@code null} if the blob
     *         cannot be downloaded directly.
     */
    @Nullable
    URL getHttpDownloadURL(Blob blob);
}
