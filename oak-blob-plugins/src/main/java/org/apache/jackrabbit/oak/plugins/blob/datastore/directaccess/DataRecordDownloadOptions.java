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

import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;

public class DataRecordDownloadOptions {
    public static DataRecordDownloadOptions fromBlobDownloadOptions(BlobDownloadOptions downloadOptions) {
        return new DataRecordDownloadOptions(
                downloadOptions.getContentType(),
                downloadOptions.getContentTypeEncoding(),
                downloadOptions.getFileName(),
                downloadOptions.getDispositionType()
        );
    }

    public static DataRecordDownloadOptions DEFAULT =
            new DataRecordDownloadOptions(null,
                    null,
                    null,
                    null);

    private final String contentType;
    private final String contentTypeEncoding;
    private final String fileName;
    private final String dispositionType;

    private DataRecordDownloadOptions(final String contentType,
                                      final String contentTypeEncoding,
                                      final String fileName,
                                      final String dispositionType) {
        this.contentType = contentType;
        this.contentTypeEncoding = contentTypeEncoding;
        this.fileName = fileName;
        this.dispositionType = dispositionType;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentTypeEncoding() {
        return contentTypeEncoding;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDispositionType() {
        return dispositionType;
    }
}
