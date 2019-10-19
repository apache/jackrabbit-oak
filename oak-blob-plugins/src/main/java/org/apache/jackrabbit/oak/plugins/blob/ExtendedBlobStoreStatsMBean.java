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

package org.apache.jackrabbit.oak.plugins.blob;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;

public interface ExtendedBlobStoreStatsMBean extends BlobStoreStatsMBean {
    String TYPE = "ExtendedBlobStoreStats";

    long getGetRecordForIdCount();

    long getGetRecordForIdErrorCount();

    long getGetAllRecordsCount();

    long getInitBlobUploadCount();

    long getInitBlobUploadErrorCount();

    long getCompleteBlobUploadCount();

    long getCompleteBlobUploadErrorCount();

    long getGetBlobDownloadURICount();

    long getGetBlobDownloadURIErrorCount();

    long getAddMetadataRecordCount();

    long getAddMetadataRecordErrorCount();

    long getGetMetadataRecordCount();

    long getGetMetadataRecordErrorCount();

    long getGetAllMetadataRecordsCount();

    long getGetAllMetadataRecordsErrorCount();

    long getMetadataRecordExistsCount();

    long getMetadataRecordExistsErrorCount();

    long getDeleteMetadataRecordCount();

    long getDeleteMetadataRecordErrorCount();

    long getDeleteAllMetadataRecordsCount();

    long getDeleteAllMetadataRecordsErrorCount();

    CompositeData getGetRecordForIdCountHistory();

    CompositeData getGetRecordForIdErrorCountHistory();

    CompositeData getGetRecordForIdTimeHistory();

    CompositeData getGetAllRecordsCountHistory();

    CompositeData getGetAllRecordsTimeHistory();

    CompositeData getInitBlobUploadCountHistory();

    CompositeData getInitBlobUploadTimeHistory();

    CompositeData getInitBlobUploadErrorCountHistory();

    CompositeData getCompleteBlobUploadCountHistory();

    CompositeData getCompleteBlobUploadTimeHistory();

    CompositeData getCompleteBlobUploadErrorCountHistory();

    CompositeData getGetBlobDownloadURICountHistory();

    CompositeData getGetBlobDownloadURITimeHistory();

    CompositeData getGetBlobDownloadURIErrorCountHistory();

    CompositeData getAddMetadataRecordCountHistory();

    CompositeData getAddMetadataRecordTimeHistory();

    CompositeData getAddMetadataRecordErrorCountHistory();

    CompositeData getGetMetadataRecordCountHistory();

    CompositeData getGetMetadataRecordTimeHistory();

    CompositeData getGetMetadataRecordErrorCountHistory();

    CompositeData getGetAllMetadataRecordsCountHistory();

    CompositeData getGetAllMetadataRecordsTimeHistory();

    CompositeData getGetAllMetadataRecordsErrorCountHistory();

    CompositeData getMetadataRecordExistsCountHistory();

    CompositeData getMetadataRecordExistsTimeHistory();

    CompositeData getMetadataRecordExistsErrorCountHistory();

    CompositeData getDeleteMetadataRecordCountHistory();

    CompositeData getDeleteMetadataRecordTimeHistory();

    CompositeData getDeleteMetadataRecordErrorCountHistory();

    CompositeData getDeleteAllMetadataRecordsCountHistory();

    CompositeData getDeleteAllMetadataRecordsTimeHistory();

    CompositeData getDeleteAllMetadataRecordsErrorCountHistory();
}
