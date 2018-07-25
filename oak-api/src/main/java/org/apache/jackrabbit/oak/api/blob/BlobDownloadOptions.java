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

import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Download options to be provided to a call to {@link
 * BlobAccessProvider#getDownloadURI(Blob, BlobDownloadOptions)}.
 * <p>
 * This object is an internal corollary to {@code
 * org.apache.jackrabbit.api.binary.BinaryDownloadOptions}.
 */
@ProviderType
public class BlobDownloadOptions {
    private static final String DISPOSITION_TYPE_INLINE = "inline";

    private final String mediaType;
    private final String characterEncoding;
    private final String fileName;
    private final String dispositionType;

    public static final BlobDownloadOptions DEFAULT = new BlobDownloadOptions();

    private BlobDownloadOptions() {
        this(null, null, null, DISPOSITION_TYPE_INLINE);
    }

    /**
     * Creates new download options.
     *
     * @param mediaType the internet media type for the blob.
     * @param characterEncoding the character encoding for the blob.
     * @param fileName the file name for the blob.
     * @param dispositionType the disposition type.
     */
    public BlobDownloadOptions(@Nullable String mediaType,
                               @Nullable String characterEncoding,
                               @Nullable String fileName,
                               @NotNull String dispositionType) {
        if (dispositionType == null) {
            throw new NullPointerException("dispositionType must not be null");
        }
        this.mediaType = mediaType;
        this.characterEncoding = characterEncoding;
        this.fileName = fileName;
        this.dispositionType = dispositionType;
    }

    /**
     * Returns the internet media type that should be assumed for the blob
     * that is to be downloaded. This value should be a valid {@code
     * jcr:mimeType}.
     *
     * @return The internet media type, or {@code null} if no type has been
     *         specified.
     */
    @Nullable
    public String getMediaType() {
        return mediaType;
    }

    /**
     * Returns the character encoding that should be assumed for the blob that
     * is to be downloaded. This value should be a valid {@code jcr:encoding}.
     *
     * @return The character encoding, or {@code null} if no encoding has been
     *         specified.
     */
    @Nullable
    public String getCharacterEncoding() {
        return characterEncoding;
    }

    /**
     * Returns the filename that should be assumed for the blob that is to be
     * downloaded.
     *
     * @return The file name, or {@code null} if no file name has been
     *         specified.
     */
    @Nullable
    public String getFileName() {
        return fileName;
    }

    /**
     * Returns the disposition type that should be assumed for the binary that
     * is to be downloaded. The default value of this setting is "inline".
     *
     * @return The disposition type.
     * @see <a href="https://tools.ietf.org/html/rfc6266#section-4.2">RFC
     *         6266, Section 4.2</a>
     */
    @NotNull
    public String getDispositionType() {
        return dispositionType;
    }
}
