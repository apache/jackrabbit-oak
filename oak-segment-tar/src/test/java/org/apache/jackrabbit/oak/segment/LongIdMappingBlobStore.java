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
package org.apache.jackrabbit.oak.segment;

public class LongIdMappingBlobStore extends IdMappingBlobStore {

    // Long blob ids shorter than MEDIUM_LIMIT are deduplicated by the segment writer's string cache
    public static final int DEDUPLICATED_ID_LENGTH = Segment.MEDIUM_LIMIT - 100;

    private static int next;

    private final int length;

    public LongIdMappingBlobStore() {
        this(Segment.BLOB_ID_SMALL_LIMIT * 20);
    }

    public LongIdMappingBlobStore(int length) {
        this.length = length;
    }

    @Override
    protected String generateId() {
        return "0".repeat(length) + next++;
    }
}
