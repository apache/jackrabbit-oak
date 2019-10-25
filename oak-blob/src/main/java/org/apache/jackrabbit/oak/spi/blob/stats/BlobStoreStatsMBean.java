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

package org.apache.jackrabbit.oak.spi.blob.stats;

import javax.management.openmbean.CompositeData;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface BlobStoreStatsMBean {
    String TYPE = "BlobStoreStats";

    long getUploadTotalSize();

    long getUploadCount();

    long getUploadTotalSeconds();

    long getUploadErrorCount();

    long getWriteBlobCount();

    long getWriteBlobTotalSize();

    long getWriteBlobErrorCount();

    long getDownloadTotalSize();

    long getDownloadCount();

    long getDownloadTotalSeconds();

    long getDownloadErrorCount();

    long getReadBlobCount();

    long getReadBlobErrorCount();

    long getDeleteCount();

    long getDeleteErrorCount();

    long getDeleteByDateCount();

    long getDeleteByDateErrorCount();

    long getAddRecordTotalSize();

    long getAddRecordCount();

    long getAddRecordErrorCount();

    long getGetRecordCount();

    long getGetRecordErrorCount();

    long getGetRecordIfStoredCount();

    long getGetRecordIfStoredErrorCount();

    long getGetRecordFromReferenceCount();

    long getGetRecordFromReferenceErrorCount();

    long getListIdsCount();

    long getListIdsErrorCount();

    String blobStoreInfoAsString();

    CompositeData getUploadSizeHistory();

    CompositeData getUploadRateHistory();

    CompositeData getUploadCountHistory();

    CompositeData getUploadErrorCountHistory();

    CompositeData getWriteBlobCountHistory();

    CompositeData getWriteBlobSizeHistory();

    CompositeData getWriteBlobRateHistory();

    CompositeData getWriteBlobErrorCountHistory();

    CompositeData getDownloadSizeHistory();

    CompositeData getDownloadCountHistory();

    CompositeData getDownloadRateHistory();

    CompositeData getDownloadErrorCountHistory();

    CompositeData getReadBlobCountHistory();

    CompositeData getReadBlobTimeHistory();

    CompositeData getReadBlobErrorCountHistory();

    CompositeData getDeleteCountHistory();

    CompositeData getDeleteErrorCountHistory();

    CompositeData getDeleteTimeHistory();

    CompositeData getDeleteByDateCountHistory();

    CompositeData getDeleteByDateErrorCountHistory();

    CompositeData getDeleteByDateTimeHistory();

    CompositeData getAddRecordSizeHistory();

    CompositeData getAddRecordCountHistory();

    CompositeData getAddRecordErrorCountHistory();

    CompositeData getAddRecordRateHistory();

    CompositeData getGetRecordCountHistory();

    CompositeData getGetRecordErrorCountHistory();

    CompositeData getGetRecordTimeHistory();

    CompositeData getGetRecordSizeHistory();

    CompositeData getGetRecordRateHistory();

    CompositeData getGetRecordIfStoredCountHistory();

    CompositeData getGetRecordIfStoredErrorCountHistory();

    CompositeData getGetRecordIfStoredTimeHistory();

    CompositeData getGetRecordIfStoredSizeHistory();

    CompositeData getGetRecordIfStoredRateHistory();

    CompositeData getGetRecordFromReferenceCountHistory();

    CompositeData getGetRecordFromReferenceErrorCountHistory();

    CompositeData getGetRecordFromReferenceTimeHistory();

    CompositeData getGetRecordFromReferenceSizeHistory();

    CompositeData getGetRecordFromReferenceRateHistory();

    CompositeData getListIdsCountHistory();

    CompositeData getListIdsTimeHistory();

    CompositeData getListIdsErrorCountHistory();
}
