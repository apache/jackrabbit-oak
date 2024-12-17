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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.jackrabbit.guava.common.base.Splitter;
import org.apache.jackrabbit.guava.common.collect.FluentIterable;
import org.apache.jackrabbit.guava.common.collect.Ordering;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
                    public Long apply(@NotNull DataRecord input) {
                        return input.getLastModified();
                    }
                }::apply).min(recs);
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
        return Sets.difference(FluentIterable.from(repos)
                .uniqueIndex(input -> SharedStoreRecordType.REPOSITORY.getIdFromName(input.getIdentifier().toString())).keySet(),
                FluentIterable.from(refs)
                        .index(input -> SharedStoreRecordType.REFERENCES.getIdFromName(input.getIdentifier().toString())).keySet());
    }

    /**
     * Repositories from which marked references older than retention time are not available.
     *
     * @param repos the repos
     * @param refs the refs
     * @param referenceTime the retention time
     * @return the sets the sets whose references not available
     */
    public static Set<String> refsNotOld(List<DataRecord> repos,
        List<DataRecord> refs, long referenceTime) {

        // Filter records older than the retention time and group by the repository id
        Set<String> qualifyingRefs = refs.stream()
            .filter(dataRecord -> dataRecord.getLastModified() < referenceTime)
            .collect(Collectors
                .groupingBy(input -> SharedStoreRecordType.MARKED_START_MARKER.getIdFromName(input.getIdentifier().toString()),
                            Collectors.mapping(java.util.function.Function.identity(), Collectors.toList())))
            .keySet();

        Set<String> repoIds =
            repos.stream()
                .map(dataRecord -> SharedStoreRecordType.REPOSITORY.getIdFromName(dataRecord.getIdentifier().toString()))
                .collect(Collectors.toSet());

        repoIds.removeAll(qualifyingRefs);
        return repoIds;
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
            return Splitter.on("_").limit(2).splitToList(
                Splitter.on(DELIM).limit(2).splitToList(name).get(1)).get(0);
        }

        public String getNameFromId(String id) {
            return String.join(DELIM, getType(), id);
        }

        /**
         * Creates name from id and prefix. The format returned is of the form
         * references-id_prefix.
         *
         * @param id
         * @param prefix
         * @return
         */
        public String getNameFromIdPrefix(String id, String prefix) {
            return String.join("_",
                String.join(DELIM, getType(), id),
                prefix);
        }

        static final String DELIM = "-";
    }
}

