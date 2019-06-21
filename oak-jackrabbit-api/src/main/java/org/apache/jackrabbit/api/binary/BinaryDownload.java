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

package org.apache.jackrabbit.api.binary;

import java.net.URI;

import javax.jcr.Binary;
import javax.jcr.RepositoryException;

import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * This extension interface provides a mechanism whereby a client can download
 * a {@link Binary} directly from a storage location.
 */
@ProviderType
public interface BinaryDownload extends Binary {
    /**
     *
     * Returns a URI for downloading this binary directly from the storage location.
     *
     * <p>
     * Using the {@code downloadOptions} parameter, some response headers of the
     * download request can be overwritten, if supported by the storage provider.
     * This is necessary to pass information that is only stored in the JCR in
     * application specific structures, and not reliably available in the binary
     * storage.
     *
     * {@link BinaryDownloadOptions} supports, but is not limited to:
     * <ul>
     *     <li>
     *         {@link BinaryDownloadOptions.BinaryDownloadOptionsBuilder#withMediaType(String) Content-Type media type}:
     *         typically available in a {@code jcr:mimeType} property
     *     </li>
     *     <li>
     *         {@link BinaryDownloadOptions.BinaryDownloadOptionsBuilder#withCharacterEncoding(String) Content-Type charset}:
     *         for media types defining a "charset", typically available in a {@code jcr:encoding} property
     *     </li>
     *     <li>
     *         {@link BinaryDownloadOptions.BinaryDownloadOptionsBuilder#withFileName(String) Content-Disposition filename}:
     *         download file name, typically taken from a JCR node name in the parent hierarchy, such as the nt:file node name
     *     </li>
     *     <li>
     *         {@link BinaryDownloadOptions.BinaryDownloadOptionsBuilder#withDispositionTypeAttachment() Content-Disposition type}:
     *         whether to show the content inline of a page (inline) or enforce a download/save as (attachment)
     *     </li>
     * </ul>
     *
     * Specifying {@link BinaryDownloadOptions#DEFAULT} will use mostly empty
     * defaults, relying on the storage provider attributes for this binary
     * (that might be empty or different from the information in the JCR).
     *
     * <p>
     * <b>Security considerations:</b>
     *
     * <ul>
     *     <li>
     *         The URI cannot be shared with other users. It must only be returned to
     *         authenticated requests corresponding to this session user or trusted system
     *         components.
     *     </li>
     *     <li>
     *         The URI must not be persisted for later use and will typically be time limited.
     *     </li>
     *     <li>
     *         The URI will only grant access to this particular binary.
     *     </li>
     *     <li>
     *         The client cannot infer any semantics from the URI structure and path names.
     *         It would typically include a cryptographic signature. Any change to the URI will
     *         likely result in a failing request.
     *     </li>
     *     <li>
     *         If the client is a browser, consider use of Content-Disposition type = attachment
     *         for executable media types such as HTML or Javascript if the content cannot be
     *         trusted.
     *     </li>
     * </ul>
     *
     * @param downloadOptions
     *            A {@link BinaryDownloadOptions} instance which is used to
     *            request specific options on the binary to be downloaded.
     *            {@link BinaryDownloadOptions#DEFAULT} should be used if the
     *            caller wishes to accept the storage provider's default
     *            behavior.
     * @return A URI for downloading the binary directly, or {@code null} if the
     *         binary cannot be downloaded directly or if the underlying
     *         implementation does not support this capability.
     * @throws RepositoryException if an error occurs trying to locate the
     *             binary.
     */
    @Nullable
    URI getURI(BinaryDownloadOptions downloadOptions)
            throws RepositoryException;
}
