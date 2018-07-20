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
import org.osgi.annotation.versioning.ProviderType;

/**
 * Download options to be provided to a call to {@link
 * BlobDirectAccessProvider#getDownloadURI(Blob, BlobDownloadOptions)}.
 * <p>
 * This object is an internal corrollary to {@link
 * org.apache.jackrabbit.api.binary.BinaryDownloadOptions}.
 */
@ProviderType
public class BlobDownloadOptions {
    private final String mimeType;
    private final String encoding;
    private final String fileName;
    private final String dispositionType;

    public static final BlobDownloadOptions DEFAULT = new BlobDownloadOptions();

    public BlobDownloadOptions() {
        this(null, null, null, null);
    }

    public BlobDownloadOptions(final String mimeType,
                               final String encoding,
                               final String fileName,
                               final String dispositionType) {
        this.mimeType = mimeType;
        this.encoding = encoding;
        this.fileName = fileName;
        this.dispositionType = dispositionType;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDispositionType() {
        return dispositionType;
    }
}
