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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class AzureBlobMetadata {

    private static final String METADATA_TYPE = "type";

    private static final String METADATA_SEGMENT_UUID = "uuid";

    private static final String METADATA_SEGMENT_POSITION = "position";

    private static final String METADATA_SEGMENT_GENERATION = "generation";

    private static final String METADATA_SEGMENT_FULL_GENERATION = "fullGeneration";

    private static final String METADATA_SEGMENT_COMPACTED = "compacted";

    private static final String TYPE_SEGMENT = "segment";

    public static HashMap<String, String> toSegmentMetadata(AzureSegmentArchiveEntry indexEntry) {
        HashMap<String, String> map = new HashMap<>();
        map.put(METADATA_TYPE, TYPE_SEGMENT);
        map.put(METADATA_SEGMENT_UUID, new UUID(indexEntry.getMsb(), indexEntry.getLsb()).toString());
        map.put(METADATA_SEGMENT_POSITION, String.valueOf(indexEntry.getPosition()));
        map.put(METADATA_SEGMENT_GENERATION, String.valueOf(indexEntry.getGeneration()));
        map.put(METADATA_SEGMENT_FULL_GENERATION, String.valueOf(indexEntry.getFullGeneration()));
        map.put(METADATA_SEGMENT_COMPACTED, String.valueOf(indexEntry.isCompacted()));
        return map;
    }

    public static AzureSegmentArchiveEntry toIndexEntry(Map<String, String> metadata, int length) {
        UUID uuid = UUID.fromString(metadata.get(METADATA_SEGMENT_UUID));
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        int position = Integer.parseInt(metadata.get(METADATA_SEGMENT_POSITION));
        int generation = Integer.parseInt(metadata.get(METADATA_SEGMENT_GENERATION));
        int fullGeneration = Integer.parseInt(metadata.get(METADATA_SEGMENT_FULL_GENERATION));
        boolean compacted = Boolean.parseBoolean(metadata.get(METADATA_SEGMENT_COMPACTED));
        return new AzureSegmentArchiveEntry(msb, lsb, position, length, generation, fullGeneration, compacted);
    }

    public static boolean isSegment(Map<String, String> metadata) {
        return metadata != null && TYPE_SEGMENT.equals(metadata.get(METADATA_TYPE));
    }

}
