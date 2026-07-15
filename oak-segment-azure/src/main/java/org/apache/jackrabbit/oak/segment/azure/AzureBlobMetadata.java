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
 */
package org.apache.jackrabbit.oak.segment.azure;

import org.apache.jackrabbit.oak.segment.azure.util.CaseInsensitiveKeysMapAccess;
import org.apache.jackrabbit.oak.segment.remote.RemoteBlobMetadata;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;

import java.util.Map;

/**
 * Provides access to the blob metadata.
 * <p>
 * In azure blob metadata keys are case-insensitive. A bug in the tool azcopy v10 make each key to start with
 * an uppercase letter.  To avoid future bugs we should be tolerant in what we read.
 * <p>
 * Azure Blobs metadata can not store multiple entries with the same key where only the case differs. Therefore it is
 * safe to use the same concept in java, see {@link CaseInsensitiveKeysMapAccess}
 */
public final class AzureBlobMetadata extends RemoteBlobMetadata {

    public static RemoteSegmentArchiveEntry toIndexEntry(Map<String, String> metadata, int length) {
        Map<String, String> caseInsensitiveMetadata = CaseInsensitiveKeysMapAccess.convert(metadata);

        return RemoteBlobMetadata.toIndexEntry(caseInsensitiveMetadata, length);
    }

    public static boolean isSegment(Map<String, String> metadata) {
        Map<String, String> caseInsensitiveMetadata = CaseInsensitiveKeysMapAccess.convert(metadata);

        return RemoteBlobMetadata.isSegment(caseInsensitiveMetadata);
    }

}
