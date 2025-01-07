/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.jackrabbit.guava.common.base.Strings;

/**
 * Contains download options for downloading a data record directly from a
 * storage location using the direct download feature.
 */
public class DataRecordDownloadOptions {
    static final String DISPOSITION_TYPE_INLINE = "inline";
    static final String DISPOSITION_TYPE_ATTACHMENT = "attachment";

    private static final char[] hex = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };
    private static final Set<Character> rfc5987AllowedChars = Set.of(
            '0','1','2','3','4','5','6','7','8','9',
                    'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
                    'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
                    '!','#','$','&','+','-','.','^','_','`','|','~'
    );

    /**
     * Create an instance of this class directly from a {@link
     * BlobDownloadOptions} instance.
     *
     * @param downloadOptions The download options to use to initialize this
     *         instance.
     * @return The new instance of this class.
     */
    public static DataRecordDownloadOptions fromBlobDownloadOptions(
            @NotNull BlobDownloadOptions downloadOptions) {
        return new DataRecordDownloadOptions(
                downloadOptions.getMediaType(),
                downloadOptions.getCharacterEncoding(),
                downloadOptions.getFileName(),
                downloadOptions.getDispositionType(),
                downloadOptions.isDomainOverrideIgnored()
        );
    }

    /**
     * Provides a default implementation of this class.  Clients should use this
     * instance when they have no options to specify and are willing to accept
     * the service provider default behavior.
     */
    public static DataRecordDownloadOptions DEFAULT =
            new DataRecordDownloadOptions(null,
                    null,
                    null,
                    DISPOSITION_TYPE_INLINE,
                    false);

    private final String mediaType;
    private final String characterEncoding;
    private final String fileName;
    private final String dispositionType;
    private boolean domainOverrideIgnored = false;

    private String contentTypeHeader = null;
    private String contentDispositionHeader = null;

    private DataRecordDownloadOptions(final String mediaType,
                                      final String characterEncoding,
                                      final String fileName,
                                      final String dispositionType,
                                      boolean domainOverrideIgnored) {
        this.mediaType = mediaType;
        this.characterEncoding = characterEncoding;
        this.fileName = fileName;
        this.dispositionType = Strings.isNullOrEmpty(dispositionType) ?
                DISPOSITION_TYPE_INLINE :
                dispositionType;
        this.domainOverrideIgnored = domainOverrideIgnored;
    }

    /**
     * Generate the correct HTTP {@code Content-Type} header value from the
     * {@link #mediaType} and {@link #characterEncoding} in this class, if set.
     * <p>
     * If {@link #mediaType} has not been given a value, this method will return
     * {@code null}.
     *
     * @return The correct value for a {@code Content-Type} header, or {@code
     *         null} if the {@link #mediaType} has not been specified.
     */
    @Nullable
    public String getContentTypeHeader() {
        if (Strings.isNullOrEmpty(contentTypeHeader)) {
            if (!Strings.isNullOrEmpty(mediaType)) {
                contentTypeHeader = Strings.isNullOrEmpty(characterEncoding) ?
                        mediaType :
                        mediaType + "; charset=" + characterEncoding;
            }
        }
        return contentTypeHeader;
    }

    /**
     * Generate the correct HTTP {@code Content-Disposition} header value from
     * the {@link #fileName} and {@link #dispositionType} in this class, if set.
     * <p>
     * A value will be returned if the file name has been set, OR if the
     * disposition type has been explicitly set to "attachment".  Otherwise
     * {@code null} will be returned.
     *
     * @return The correct value for a {@code Content-Disposition} header, or
     *         {@code null} if the {@link #fileName} has not been specified and
     *         the {@link #dispositionType} has not been set to "attachment".
     */
    @Nullable
    public String getContentDispositionHeader() {
        if (Strings.isNullOrEmpty(contentDispositionHeader)) {
            if (!Strings.isNullOrEmpty(fileName)) {
                String dispositionType = this.dispositionType;
                if (Strings.isNullOrEmpty(dispositionType)) {
                    dispositionType = DISPOSITION_TYPE_INLINE;
                }
                contentDispositionHeader = formatContentDispositionHeader(dispositionType, fileName, rfc8187Encode(fileName));
            }
            else if (DISPOSITION_TYPE_ATTACHMENT.equals(this.dispositionType)) {
                contentDispositionHeader = DISPOSITION_TYPE_ATTACHMENT;
            }
        }
        return contentDispositionHeader;
    }

    private String formatContentDispositionHeader(@NotNull final String dispositionType,
                                                  @NotNull final String fileName,
                                                  @Nullable final String rfc8187EncodedFileName) {
        Charset ISO_8859_1 = StandardCharsets.ISO_8859_1;
        String iso_8859_1_fileName = new String(
                ISO_8859_1.encode(fileName).array(),
                ISO_8859_1
        ).replace("\"", "\\\"");
        return null != rfc8187EncodedFileName ?
                String.format("%s; filename=\"%s\"; filename*=UTF-8''%s",
                        dispositionType, iso_8859_1_fileName, rfc8187EncodedFileName) :
                String.format("%s; filename=\"%s\"",
                        dispositionType, iso_8859_1_fileName);
    }

    private String rfc8187Encode(@NotNull final String input) {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            char c = (char) b;
            if (rfc5987AllowedChars.contains(c)) {
                sb.append(c);
            }
            else {
                sb.append('%');
                sb.append(hex[0x0F & (b >>> 4)]);
                sb.append(hex[b & 0x0F]);
            }
        }
        return sb.toString();
    }

    /**
     * Returns the media type of this instance.
     *
     * @return The media type, or {@code null} if it has not been set.
     */
    @Nullable
    public String getMediaType() {
        return mediaType;
    }

    /**
     * Returns the character encoding of this instance.
     *
     * @return The character encoding, or {@code null} if it has not been set.
     */
    @Nullable
    public String getCharacterEncoding() {
        return characterEncoding;
    }

    /**
     * Returns the file name of this instance.
     *
     * @return The file name, or {@code null} if it has not been set.
     */
    @Nullable
    public String getFileName() {
        return fileName;
    }

    /**
     * Returns the disposition type of this instance.
     *
     * @return The disposition type, or {@code null} if it has not been set.
     */
    @Nullable
    public String getDispositionType() {
        return dispositionType;
    }

    /**
     * Indicates whether the data store should ignore any configured download
     * domain override value when generating the signed download URI.
     *
     * @return true if the domain override should be ignored; false otherwise.
     */
    public boolean isDomainOverrideIgnored() { return domainOverrideIgnored; }
}
