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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * Utility class for {@link SharedDataStore}.
 */
public class SharedDataStoreUtils {
    /**
     * Checks if the blob store shared.
     *
     * @param blobStore the blob store
     * @return true if shared
     */
    public static boolean isShared(BlobStore blobStore) {
        return (blobStore instanceof SharedDataStore)
            && (((SharedDataStore) blobStore).getType() == SharedDataStore.Type.SHARED);
    }

    /**
     * Gets the earliest record of the available reference records.
     * 
     * @param recs the recs
     * @return the earliest record
     */
    public static DataRecord getEarliestRecord(List<DataRecord> recs) {
        return Ordering.natural().onResultOf(
                new Function<DataRecord, Long>() {
                    @Override
                    @Nullable
                    public Long apply(@Nonnull DataRecord input) {
                        return input.getLastModified();
                    }
                }).min(recs);
    }

    /**
     * Repositories from which marked references not available.
     * 
     * @param repos the repos
     * @param refs the refs
     * @return the sets the sets whose references not available
     */
    public static Set<String> refsNotAvailableFromRepos(List<DataRecord> repos,
            List<DataRecord> refs) {
        return Sets.difference(FluentIterable.from(repos).uniqueIndex(
                new Function<DataRecord, String>() {
                    @Override
                    @Nullable
                    public String apply(@Nonnull DataRecord input) {
                        return SharedStoreRecordType.REPOSITORY.getIdFromName(input.getIdentifier().toString());
                    }
                }).keySet(),
                FluentIterable.from(refs).uniqueIndex(
                        new Function<DataRecord, String>() {
                            @Override
                            @Nullable
                            public String apply(@Nonnull DataRecord input) {
                                return SharedStoreRecordType.REFERENCES.getIdFromName(input.getIdentifier().toString());
                            }
                        }).keySet());
    }

    /**
     * Encapsulates the different type of records at the data store root.
     */
    public enum SharedStoreRecordType {
        REFERENCES("references"),
        REPOSITORY("repository"),
        MARKED_START_MARKER("markedTimestamp"),
        BLOBREFERENCES("blob");

        private final String type;

        SharedStoreRecordType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getIdFromName(String name) {
            return Splitter.on(DELIIM).limit(2).splitToList(name).get(1);
        }

        public String getNameFromId(String id) {
            return Joiner.on(DELIIM).join(getType(), id);
        }

        static final String DELIIM = "-";
    }
}

