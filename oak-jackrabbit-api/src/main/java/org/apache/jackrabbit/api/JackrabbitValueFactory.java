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
 *
 */

package org.apache.jackrabbit.api;

import java.io.InputStream;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryUploadOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Defines optional functionality that a {@link ValueFactory} may choose to
 * provide. A {@link ValueFactory} may also implement this interface without
 * supporting all of the capabilities in this interface. Each method of the
 * interface describes the behavior of that method if the underlying capability
 * is not available.
 *
 * <p>
 * This interface defines the following optional features:
 * <ul>
 *     <li>
 *         Direct Binary Access - enable a client to upload or download binaries
 *         directly to/from a storage location
 *     </li>
 * </ul>
 *
 * <p>
 * The features are described in more detail below.
 *
 * <h2>Direct Binary Access</h2>
 *
 * The Direct Binary Access feature provides the capability for a client to
 * upload or download binaries directly to/from a storage location. For example,
 * this might be a cloud storage providing high-bandwidth direct network access.
 * This API allows for requests to be authenticated and for access permission
 * checks to take place within the repository, but for clients to then access
 * the storage location directly.
 *
 * <p>
 * The feature consists of two parts, download and upload.
 *
 * <h3>Direct Binary Download</h3>
 *
 * This feature enables remote clients to download binaries directly from a
 * storage location without streaming the binary through the Jackrabbit-based
 * application.
 *
 * <p>
 * For an existing {@link Binary} value that implements {@link BinaryDownload},
 * a read-only URI (see {@link BinaryDownload#getURI(BinaryDownloadOptions)})
 * can be retrieved and passed to a remote client, such as a browser.
 *
 * <h3>Direct Binary Upload</h3>
 *
 * This feature enables remote clients to upload binaries directly to a storage
 * location.
 *
 * <p>
 * Note: When adding binaries already present on the same JVM/server as
 * the JCR repository, for example because they were generated locally, please
 * use the regular JCR API {@link ValueFactory#createBinary(InputStream)}
 * instead. This feature is solely designed for remote clients.
 *
 * <p>
 * The direct binary upload process is split into 3 phases:
 * <ol>
 *     <li>
 *         <b>Initialize</b>: A remote client makes request to the
 *         Jackrabbit-based application to request an upload, which calls {@link
 *         #initiateBinaryUpload(long, int)} and returns the resulting {@link
 *         BinaryUpload instructions} to the remote client.  A client may
 *         optionally choose to provide a {@link BinaryUploadOptions} via
 *         {@link #initiateBinaryUpload(long, int, BinaryUploadOptions)} if
 *         additional options must be specified.
 *     </li>
 *     <li>
 *         <b>Upload</b>: The remote client performs the actual binary upload
 *         directly to the binary storage provider. The {@link BinaryUpload}
 *         returned from the previous call to {@link
 *         #initiateBinaryUpload(long, int)} contains detailed instructions on
 *         how to complete the upload successfully.
 *     </li>
 *     <li>
 *         <b>Complete</b>: The remote client notifies the Jackrabbit-based
 *         application that step 2 is complete. The upload token returned in
 *         the first step (obtained by calling {@link
 *         BinaryUpload#getUploadToken()} is passed by the application to {@link
 *         #completeBinaryUpload(String)}. This will provide the application
 *         with a regular {@link Binary JCR Binary} that can then be used to
 *         write JCR content including the binary (such as an nt:file structure)
 *         and persist it using {@link Session#save}.
 *     </li>
 * </ol>
 */
@ProviderType
public interface JackrabbitValueFactory extends ValueFactory {

    /**
     * Initiate a transaction to upload a binary directly to a storage
     * location and return {@link BinaryUpload} instructions for a remote client.
     * Returns {@code null} if the feature is not available.
     *
     * <p>
     * {@link IllegalArgumentException} will be thrown if an upload
     * cannot be supported for the required parameters, or if the parameters are
     * otherwise invalid. Each service provider has specific limitations.
     *
     * @param maxSize The exact size of the binary to be uploaded or the
     *                estimated maximum size if the exact size is unknown.
     *                If the estimation was too small, the transaction
     *                should be restarted by invoking this method again
     *                using the correct size.
     * @param maxURIs The maximum number of upload URIs that the client can
     *                accept, for example due to message size limitations.
     *                A value of -1 indicates no limit.
     *                Upon a successful return, it is ensured that an upload
     *                of {@code maxSize} can be completed by splitting the
     *                binary into {@code maxURIs} parts, otherwise
     *                {@link IllegalArgumentException} will be thrown.
     *
     * @return A {@link BinaryUpload} providing the upload instructions,
     *         or {@code null} if the implementation does not support the direct
     *         upload feature.
     *
     * @throws IllegalArgumentException if the provided arguments are
     *         invalid or if an upload cannot be completed given the
     *         provided arguments. For example, if the value of {@code maxSize}
     *         exceeds the size limits for a single binary upload for the
     *         implementation or the service provider, or if the value of
     *         {@code maxSize} divided by {@code maxParts} exceeds the size
     *         limit for an upload or upload part.
     *
     * @throws AccessDeniedException if the session has insufficient
     *         permission to perform the upload.
     */
    @Nullable
    BinaryUpload initiateBinaryUpload(long maxSize, int maxURIs)
            throws IllegalArgumentException, AccessDeniedException;

    /**
     * Initiate a transaction to upload a binary directly to a storage
     * location and return {@link BinaryUpload} instructions for a remote client.
     * Returns {@code null} if the feature is not available.
     *
     * <p>
     * {@link IllegalArgumentException} will be thrown if an upload
     * cannot be supported for the required parameters, or if the parameters are
     * otherwise invalid. Each service provider has specific limitations.
     *
     * @param maxSize The exact size of the binary to be uploaded or the
     *                estimated maximum size if the exact size is unknown.
     *                If the estimation was too small, the transaction
     *                should be restarted by invoking this method again
     *                using the correct size.
     * @param maxURIs The maximum number of upload URIs that the client can
     *                accept, for example due to message size limitations.
     *                A value of -1 indicates no limit.
     *                Upon a successful return, it is ensured that an upload
     *                of {@code maxSize} can be completed by splitting the
     *                binary into {@code maxURIs} parts, otherwise
     *                {@link IllegalArgumentException} will be thrown.
     * @param options A {@link BinaryUploadOptions} instance containing any
     *                options for this call.
     *
     * @return A {@link BinaryUpload} providing the upload instructions,
     *         or {@code null} if the implementation does not support the direct
     *         upload feature.
     *
     * @throws IllegalArgumentException if the provided arguments are
     *         invalid or if an upload cannot be completed given the
     *         provided arguments. For example, if the value of {@code maxSize}
     *         exceeds the size limits for a single binary upload for the
     *         implementation or the service provider, or if the value of
     *         {@code maxSize} divided by {@code maxParts} exceeds the size
     *         limit for an upload or upload part.
     *
     * @throws AccessDeniedException if the session has insufficient
     *         permission to perform the upload.
     */
    @Nullable
    BinaryUpload initiateBinaryUpload(long maxSize, int maxURIs, BinaryUploadOptions options)
            throws IllegalArgumentException, AccessDeniedException;

    /**
     * Complete the transaction of uploading a binary directly to a storage
     * location and return a {@link Binary} to set as value for a binary
     * JCR property. The binary is not automatically associated with
     * any location in the JCR.
     *
     * <p>
     * The client must provide a valid upload token, obtained from
     * {@link BinaryUpload#getUploadToken()} when this transaction was initialized
     * using {@link #initiateBinaryUpload(long, int)}.
     * If the {@code uploadToken} is unreadable or invalid,
     * an {@link IllegalArgumentException} will be thrown.
     *
     * This method is idempotent.  Calling the method more than one time with the
     * same upload token must not cause an existing binary to be modified; thus
     * if a prior call with this upload token succeeded in creating the binary,
     * subsequent calls with the same upload token have no effect on the binary.
     * This method should always return the binary that was uploaded with this
     * upload token, even in subsequent calls with the same upload token
     * (in other words, calling this method twice with the same upload token
     * results in the same binary being returned both times).
     *
     * @param uploadToken A String identifying the upload transaction.
     *
     * @return The uploaded {@link Binary}, or {@code null} if the
     *         implementation does not support the direct upload feature.
     *
     * @throws IllegalArgumentException if the {@code uploadToken} is invalid or
     *         does not identify a known binary upload.
     * @throws RepositoryException if another error occurs.
     */
    @Nullable
    Binary completeBinaryUpload(@NotNull String uploadToken)
            throws IllegalArgumentException, RepositoryException;
}
