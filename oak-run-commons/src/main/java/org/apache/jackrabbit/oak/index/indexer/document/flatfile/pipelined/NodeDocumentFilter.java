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

public class NodeDocumentFilter {
    private static final Logger LOG = LoggerFactory.getLogger(NodeDocumentFilter.class);

    public static final String OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_INCLUDE_PATH = "oak.indexer.pipelined.nodeDocument.filter.includePath";
    public static final String OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP = "oak.indexer.pipelined.nodeDocument.filter.suffixesToSkip";
    private final String includePath = ConfigHelper.getSystemPropertyAsString(OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_INCLUDE_PATH, "");
    private final List<String> suffixesToSkip = ConfigHelper.getSystemPropertyAsStringList(OAK_INDEXER_PIPELINED_NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "", ";");

    private final boolean filteringDisabled;

    // Statistics
    private final AtomicLong skippedFields = new AtomicLong(0);
    private final AtomicLong longPathSkipped = new AtomicLong(0);
    private final ConcurrentHashMap<String, MutableLong> filteredSuffixesCounts = new ConcurrentHashMap<>();

    public NodeDocumentFilter() {
        this.filteringDisabled = includePath.isBlank() || suffixesToSkip.isEmpty();
        if (filteringDisabled) {
            LOG.info("Node document filtering disabled.");
        }
    }

    public long getSkippedFields() {
        return skippedFields.get();
    }

    public long getLongPathSkipped() {
        return longPathSkipped.get();
    }

    public ConcurrentHashMap<String, MutableLong> getFilteredSuffixesCounts() {
        return filteredSuffixesCounts;
    }

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
            LOG.info("Invalid field. {} = {}", fieldName, idOrPathValue);
            return false;
        }
        if (idOrPathValue.startsWith(includePath, idxOfFirstForwardSlash)) {
            // Match the include path. Check if it ends with any of the suffixes to skip.
            for (String suffix : suffixesToSkip) {
                if (idOrPathValue.endsWith(suffix)) {
                    // This node document should be skipped.
                    filteredSuffixesCounts.computeIfAbsent(suffix, k -> new MutableLong(0)).increment();
                    long skippedSoFar = skippedFields.incrementAndGet();
                    if (fieldName.equals(NodeDocument.PATH)) {
                        longPathSkipped.incrementAndGet();
                    }
                    if (skippedSoFar % 100_000 == 0) {
                        LOG.info("skippedSoFar: {}. Long path: {}, Doc: {}={}", skippedSoFar, longPathSkipped.get(), fieldName, idOrPathValue);
                    }
                    return true;
                }
            }
        }
        return false;
    }
}
