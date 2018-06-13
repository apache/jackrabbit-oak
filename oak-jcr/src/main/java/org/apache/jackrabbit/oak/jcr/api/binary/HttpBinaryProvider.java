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

package org.apache.jackrabbit.oak.jcr.api.binary;

import java.net.URL;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

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
 *         which calls {@link #initializeHttpUpload(long, int)} and returns the resulting
 *         {@link BinaryHttpUpload information} to the remote client.
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
     * Returns HTTP upload instructions for storing a binary. After a remote client has uploaded the
     * binary, {@link #completeHttpUpload(String)} must be called using the same upload token returned
     * from this method to retrieve a {@link Binary} that can be added and persisted into the repository.
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
     * maximum part size.
     *
     * <p>
     * The returned instructions will usually be time limited and cannot be shared with other users.
     * They must only be returned to authenticated requests corresponding to this session user
     * or trusted system components (service users).
     *
     * <p>
     * If the current session has not enough permissions to add binaries to the repository,
     * an {@link AccessDeniedException} will be thrown.
     *
     * <p>
     * This method does not affect the transient space of the current session.
     *
     * <p>
     * If the upload fails or never happens before the URLs expire, no change will happen to
     * the repository.
     *
     * @param maxSize the exact size of the binary to upload, if known,
     *                or the maximum estimated size (required, must be larger than zero)
     * @param maxParts maximum number of parts to return (required)
     *
     * @return HTTP upload instructions or {@code null} if the feature is not available
     *
     * @throws AccessDeniedException if the feature is available but the session is not allowed to add binaries
     */
    // TODO: exceptions
    // TODO: in which api/package?
    @Nullable
    BinaryHttpUpload initializeHttpUpload(long maxSize, int maxParts) throws AccessDeniedException;

    // TODO document
    // TODO exceptions
    @Nonnull
    Binary completeHttpUpload(String uploadToken) throws RepositoryException;

    // TODO document
    @Nullable
    URL getDownloadURL(Binary binary) throws RepositoryException;
}
