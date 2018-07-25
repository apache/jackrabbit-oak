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

import java.nio.charset.StandardCharsets;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DataRecordDownloadOptions {
    static final String DISPOSITION_TYPE_INLINE = "inline";
    static final String DISPOSITION_TYPE_ATTACHMENT = "attachment";

    public static DataRecordDownloadOptions fromBlobDownloadOptions(
            @NotNull BlobDownloadOptions downloadOptions) {
        return new DataRecordDownloadOptions(
                downloadOptions.getMediaType(),
                downloadOptions.getCharacterEncoding(),
                downloadOptions.getFileName(),
                downloadOptions.getDispositionType()
        );
    }

    public static DataRecordDownloadOptions DEFAULT =
            new DataRecordDownloadOptions(null,
                    null,
                    null,
                    DISPOSITION_TYPE_INLINE);

    private final String mediaType;
    private final String characterEncoding;
    private final String fileName;
    private final String dispositionType;

    private String contentTypeHeader = null;
    private String contentDispositionHeader = null;

    private DataRecordDownloadOptions(final String mediaType,
                                      final String characterEncoding,
                                      final String fileName,
                                      final String dispositionType) {
        this.mediaType = mediaType;
        this.characterEncoding = characterEncoding;
        this.fileName = fileName;
        this.dispositionType = Strings.isNullOrEmpty(dispositionType) ?
                DISPOSITION_TYPE_INLINE :
                dispositionType;
    }

    @Nullable
    public String getContentTypeHeader() {
        if (Strings.isNullOrEmpty(contentTypeHeader)) {
            if (!Strings.isNullOrEmpty(mediaType)) {
                contentTypeHeader = Strings.isNullOrEmpty(characterEncoding) ?
                        mediaType :
                        Joiner.on("; charset=").join(mediaType, characterEncoding);
            }
        }
        return contentTypeHeader;
    }

    @Nullable
    public String getContentDispositionHeader() {
        if (Strings.isNullOrEmpty(contentDispositionHeader)) {
            if (!Strings.isNullOrEmpty(fileName)) {
                String dispositionType = this.dispositionType;
                if (Strings.isNullOrEmpty(dispositionType)) {
                    dispositionType = DISPOSITION_TYPE_INLINE;
                }
                contentDispositionHeader =
                        String.format("%s; filename=\"%s\"; filename*=UTF-8''%s",
                                dispositionType, fileName,
                                new String(fileName.getBytes(StandardCharsets.UTF_8))
                        );
            }
            else if (DISPOSITION_TYPE_ATTACHMENT.equals(this.dispositionType)) {
                contentDispositionHeader = DISPOSITION_TYPE_ATTACHMENT;
            }
        }
        return contentDispositionHeader;
    }

    @Nullable
    public String getMediaType() {
        return mediaType;
    }

    @Nullable
    public String getCharacterEncoding() {
        return characterEncoding;
    }

    @Nullable
    public String getFileName() {
        return fileName;
    }

    @Nullable
    public String getDispositionType() {
        return dispositionType;
    }
}
