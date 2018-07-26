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

import java.net.URI;

import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Extension interface applied to a class that indicates that the class
 * implements the direct upload and direct download feature for {@link Blob}s.
 */
@ProviderType
public interface BlobAccessProvider {

    /**
     * Begin a transaction to perform a direct blob upload to a storage
     * location. This method will throw {@link IllegalArgumentException} if no
     * valid upload can be arranged with the arguments specified. E.g. the max
     * upload size specified divided by the number of URIs requested indicates
     * the minimum size of each upload. If that size exceeds the maximum upload
     * size supported by the service provider, {@link IllegalArgumentException}
     * is thrown.
     * <p>
     * Each service provider has specific limitations with regard to maximum
     * upload sizes, maximum overall blob sizes, numbers of URIs in multi-part
     * uploads, etc. which can lead to {@link IllegalArgumentException} being
     * thrown. You should consult the documentation for your specific service
     * provider for details.
     * <p>
     * Beyond service provider limitations, the implementation may also choose
     * to enforce its own limitations and may throw this exception based on
     * those limitations. Configuration may also be used to set limitations so
     * this exception may be thrown when configuration parameters are exceeded.
     *
     * @param maxUploadSizeInBytes the largest size of the blob to be
     *         uploaded, in bytes, based on the caller's best guess.  If the
     *         actual size of the file to be uploaded is known, that value
     *         should be used.
     * @param maxNumberOfURIs the maximum number of URIs the client is able to
     *         accept. If the client does not support multi-part uploading, this
     *         value should be 1. Note that the implementing class is not
     *         required to support multi-part uploading so it may return only a
     *         single upload URI regardless of the value passed in for this
     *         parameter.  If the client is able to accept any number of URIs, a
     *         value of -1 may be passed in to indicate that the implementation
     *         is free to return as many URIs as it desires.
     * @return A {@link BlobUpload} referencing this direct upload, or
     *         {@code null} if the underlying implementation doesn't support
     *         direct uploading.
     * @throws IllegalArgumentException if {@code maxUploadSizeInBytes} is not
     *         a positive value, or if {@code maxNumberOfURIs} is not either a
     *         positive value or -1, or if the upload cannot be completed as
     *         requested, due to a mismatch between the request parameters and
     *         the capabilities of the service provider or the implementation.
     */
    @Nullable
    BlobUpload initiateBlobUpload(long maxUploadSizeInBytes,
                                  int maxNumberOfURIs)
            throws IllegalArgumentException;

    /**
     * Complete a transaction for uploading a blob to a storage location via
     * direct blob upload.
     * <p>
     * This requires an {@code uploadToken} that can be obtained from the
     * returned {@link BlobUpload} from a previous call to {@link
     * #initiateBlobUpload(long, int)}. This token is required to complete
     * the transaction for an upload to be valid and complete.  The token
     * includes encoded data about the transaction and may include a signature
     * that will be verified by the implementation.
     *
     * @param uploadToken the upload token from a {@link BlobUpload} object
     *         returned from a previous call to {@link
     *         #initiateBlobUpload(long, int)}.
     * @return The {@link Blob} that was created, or {@code null} if the object
     *         could not be created.
     * @throws IllegalArgumentException if the {@code uploadToken} is null,
     *         empty, or cannot be parsed or is otherwise invalid, e.g. if the
     *         included signature does not match.
     */
    @Nullable
    Blob completeBlobUpload(@NotNull String uploadToken)
            throws IllegalArgumentException;

    /**
     * Obtain a download URI for a {@link Blob}. This is usually a signed URI
     * that can be used to directly download the blob corresponding to the
     * provided {@link Blob}.
     * <p>
     * A caller must specify a {@link BlobDownloadOptions} instance.  The
     * implementation will attempt to apply the specified {@code
     * downloadOptions} to the subsequent download.  For example, if the caller
     * knows that the URI refers to a specific type of content, the caller can
     * specify that content type by setting it in the {@code downloadOptions}.
     * The caller may also use a default instance obtained via {@link
     * BlobDownloadOptions#DEFAULT} in which case the caller is indicating that
     * the default behavior of the service provider is acceptable.
     *
     * @param blob The {@link Blob} to be downloaded.
     * @param downloadOptions A {@link BlobDownloadOptions} instance that
     *         specifies any download options to be used for the download URI.
     * @return A URI to download the blob directly or {@code null} if the blob
     *         cannot be downloaded directly.
     */
    @Nullable
    URI getDownloadURI(@NotNull Blob blob,
                       @NotNull BlobDownloadOptions downloadOptions);
}
