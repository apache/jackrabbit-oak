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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Implements a filter to decide if a given Mongo document should be processed or ignored based on its path. The filter has
 * two configuration parameters:
 *
 * <ul>
 * <li> filteredPath - The path where the filter is applied. Only the documents inside this path will be considered for filtering.
 *   Documents in other paths will all be accepted.
 * <li> suffixesToSkip - A list of suffixes to filter. That is, any document whose path ends in one of these suffixes will
 *   be filtered.
 * </ul>
 * <p>
 * The intent of this filter is to be applied as close as possible to the download/decoding of the documents from Mongo,
 * in order to filter unnecessary documents early and avoid spending resources processing them.
 */
public class MongoDocumentFilter {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentFilter.class);

    private final String filteredPath;
    private final List<String> suffixesToSkip;

    private final boolean filteringDisabled;

    // Statistics
    private final AtomicLong skippedFields = new AtomicLong(0);
    private final AtomicLong longPathSkipped = new AtomicLong(0);
    private final ConcurrentHashMap<String, MutableLong> filteredSuffixesCounts = new ConcurrentHashMap<>();

    public MongoDocumentFilter(String filteredPath, List<String> suffixesToSkip) {
        this.filteredPath = filteredPath;
        this.suffixesToSkip = suffixesToSkip;
        this.filteringDisabled = filteredPath.isBlank() || suffixesToSkip.isEmpty();
        if (filteringDisabled) {
            LOG.info("Mongo document filtering disabled.");
        }
    }

    /**
     * @param fieldName     Name of the Mongo document field. Expected to be either  _id or _path
     * @param idOrPathValue The value of the field
     * @return true if the document should be skipped, false otherwise
     */
    public boolean shouldSkip(String fieldName, String idOrPathValue) {
        if (filteringDisabled) {
            return false;
        }
        // Check if the NodeDocument should be considered for filtering, that is, if it starts with includePath.
        // If the value is for an _id, then we must find the start of the path section, that is, the position of the first
        // slash (3:/foo/bar/baz). If the value given is for a path, then it already contains only the path. In any case,
        // we look up for the first occurrence of /
        int idxOfFirstForwardSlash = idOrPathValue.indexOf('/');
        if (idxOfFirstForwardSlash < 0) {
            LOG.warn("Invalid field. {} = {}", fieldName, idOrPathValue);
            return false;
        }
        if (idOrPathValue.startsWith(filteredPath, idxOfFirstForwardSlash)) {
            // Match the include path. Check if it ends with any of the suffixes to skip.
            for (String suffix : suffixesToSkip) {
                if (idOrPathValue.endsWith(suffix)) {
                    // This node document should be skipped.
                    filteredSuffixesCounts.computeIfAbsent(suffix, k -> new MutableLong(0)).increment();
                    skippedFields.incrementAndGet();
                    if (fieldName.equals(NodeDocument.PATH)) {
                        longPathSkipped.incrementAndGet();
                    }
                    return true;
                }
            }
        }
        return false;
    }


    public boolean isFilteringDisabled() {
        return filteringDisabled;
    }

    public long getSkippedFields() {
        return skippedFields.get();
    }

    public long getLongPathSkipped() {
        return longPathSkipped.get();
    }

    public String formatTopK(int k) {
        return filteredSuffixesCounts.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(k)
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
