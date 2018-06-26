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
package org.apache.jackrabbit.oak.jcr.api.binary;

import java.net.URL;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.blob.IllegalHttpUploadArgumentsException;
import org.apache.jackrabbit.oak.api.blob.InvalidHttpUploadTokenException;

/**
 * Extension interface for {@link Session} that provides the capability to upload binary files
 * directly to the underlying binary storage. This might be a cloud storage providing high-bandwidth
 * direct network access.
 *
 * <p>
 * <b>Note:</b> When adding binaries already present on the same JVM or server as Oak, for example
 * because they were generated locally, please use the regular JCR API for
 * {@link javax.jcr.Property#setValue(Binary) adding binaries through input streams}
 * instead. This feature here is solely designed for remote clients.
 *
 * <p>
 * The process is split into 3 phases:
 * <ol>
 *     <li>
 *         <b>Initialize</b>: A remote client makes request to the Oak-based application to request an upload,
 *         which calls {@link #initiateHttpUpload(String, long, int)} and returns the resulting
 *         {@link HttpBinaryUpload information} to the remote client.
 *     </li>
 *     <li>
 *         <b>Upload</b>: The remote client performs the actual binary upload using that information,
 *         directly to the binary storage provider.
 *     </li>
 *     <li>
 *         <b>Complete</b>: The remote client notifies the Oak-based application about completing step 2.
 *         The upload token returned in the first step is passed back by the client or remembered by the application,
 *         and it then calls {@link #completeHttpUpload(String)}. This will provide the application with
 *         a regular {@link Binary JCR Binary} that it then uses to write JCR content including
 *         the binary (such as an nt:file structure) and {@link Session#save() persist} it.
 *     </li>
 * </ol>
 */
public interface HttpBinaryProvider {

    /**
     * Returns HTTP upload instructions for storing a binary. This is the initial call in the
     * {@link HttpBinaryProvider 3 phase process} to upload a binary directly through HTTP.
     * After a remote client has uploaded the binary in step 2, {@link #completeHttpUpload(String)}
     * must be called for step 3 using the same upload token returned from this method to retrieve
     * a {@link Binary} that can be added and persisted into the repository.
     *
     * <p>
     * If this feature is not available, the method will return {@code null}.
     *
     * <p>
     * If the binary size is known, the caller must provide that in {@code maxSize}. If it's yet
     * unknown, an upper estimated limit must be passed. It will not be possible to upload a file
     * that is larger than the provided {@code maxSize}. In that case the caller must initialize
     * the upload again with a higher or correct size.
     *
     * <p>
     * The caller needs to specify how many parts it can handle in {@code maxParts}. Each part
     * will be a URL of some length, which typically need to be transported to a remote client,
     * so there might be limitations on the overall number. Specifying a number too small might
     * lead to an error, depending on the underlying provider limitations regarding minimum and
     * maximum part size. The upper bound is dependent upon the service provider as well as the
     * underlying implementation, but should be at least ten thousand.  Specifying a number too
     * large may exceed implementation or service provider limitations and result in an error.
     * Specifying -1 as the value of {@code maxParts} is supported, which means the caller can
     * support any number of URLs being returned. The implementation will then choose the number
     * of URLs to return, which could be as high as the implementation or service provider limitation.
     *
     * <p>
     * The returned instructions will usually be time limited and cannot be shared with other users.
     * They must only be returned to authenticated requests corresponding to this session user
     * or trusted system components (service users).
     *
     * <p>
     * The caller must provide the path to the node where the resulting binary is intended to be
     * added. While calling this method itself should not result in any modification to the
     * underlying store (that should happen later, in {@link #completeHttpUpload(String)},
     * the path must be included in order to verify that the caller has permissions to add
     * a binary property at the specified path. If the current session has not enough permissions
     * to add binaries to the repository, an {@link AccessDeniedException} will be thrown.
     *
     * <p>
     * This method does not affect the transient space of the current session.
     *
     * <p>
     * If the upload fails or never happens before the URLs expire, no change will happen to
     * the repository.
     *
     * @param path the path of the node to which the binary will be added
     * @param maxSize the exact size of the binary to upload, if known,
     *                or the maximum estimated size (required, must be larger than zero)
     * @param maxParts maximum number of parts to return (required, must not be zero)
     *
     * @return HTTP upload instructions or {@code null} if the feature is not available
     *
     * @throws {@link AccessDeniedException} if the feature is available but the session
     * is not allowed to add binaries, {@link IllegalHttpUploadArgumentsException} if the
     * upload size or number of URLs requested will result in an upload that cannot be
     * supported by the implementation or service provider, or {@link RepositoryException}
     * if a more general repository error occurs or the feature is not available
     */
    @Nullable
    HttpBinaryUpload initiateHttpUpload(String path, long maxSize, int maxParts)
            throws AccessDeniedException, IllegalHttpUploadArgumentsException, RepositoryException;

    /**
     * Complete the HTTP upload of a binary and return a {@link Binary} that can be added to
     * the repository content. This is the final step of the {@link HttpBinaryProvider 3 phase process}
     * to upload a binary directly through HTTP.
     *
     * <p>
     * Unlike {@link #initiateHttpUpload(String, long, int)}, this will throw an exception if the feature is not
     * supported, as a client must only attempt to call this with a proper value returned from
     * {@link #initiateHttpUpload(String, long, int)}.
     *
     * @param uploadToken the token returned from {@link #initiateHttpUpload(String, long, int)},
     *                    available in {@link HttpBinaryUpload#getUploadToken()}
     *
     * @return a JCR binary to be used as property value
     *
     * @throws {@link InvalidHttpUploadTokenException} if the upload token is not parseable, signature
     * doesn't match, or is otherwise invalid; {@link RepositoryException} if binary upload is not supported
     */
    @Nonnull
    Binary completeHttpUpload(String uploadToken) throws InvalidHttpUploadTokenException, RepositoryException;

    /**
     * Returns a URL for downloading the binary using HTTP GET directly from the underlying binary storage.
     *
     * <p>
     * The URL will usually be time limited and cannot be shared with other users. It must
     * only be returned to authenticated requests corresponding to this session user
     * or trusted system components (service users).
     *
     * <p>
     * The URL will only grant access to the particular binary. The client cannot infer any semantics from
     * the URL structure and path names. It would typically include a cryptographic signature.
     * Any change to the URL will likely result in a failing request.
     *
     * @param binary existing, persisted binary for which to retrieve the HTTP URL
     *
     * @return a URL for retrieving the binary using HTTP GET or {@code null} if the feature is not available
     *         in general (e.g. the underlying data store doesn't have this capability)
     *         or for that particular binary (e.g. if the binary is stored in-lined in the node store)
     */
    @Nullable
    URL getHttpDownloadURL(Binary binary);
}
