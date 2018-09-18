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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;

import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;

public interface LuceneIndexMBean {
    String TYPE = "LuceneIndex";

    TabularData getIndexStats() throws IOException;

    TabularData getBadIndexStats();

    TabularData getBadPersistedIndexStats();

    boolean isFailing();

    @Description("Determines the set of index paths upto given maxLevel. This can be used to determine the value for" +
            "[includedPaths]. For this to work you should have [evaluatePathRestrictions] set to true in your index " +
            "definition")
    String[] getIndexedPaths(
            @Description("Index path for which stats are to be determined")
            @Name("indexPath")
            String indexPath,
            @Name("maxLevel")
            @Description("Maximum depth to examine. E.g. 5. Stats calculation would " +
                    "break out after this limit")
            int maxLevel,
            @Description("Maximum number of unique paths to examine. E.g. 100. Stats " +
                    "calculation would break out after this limit")
            @Name("maxPathCount")
            int maxPathCount
            ) throws IOException;

    @Description("Retrieves the fields, and number of documents for each field, for an index. " +
            "This allows to investigate what is stored in the index.")
    String[] getFieldInfo(
            @Name("indexPath")
            @Description("The index path (empty for all indexes)")
                    String indexPath
    ) throws IOException;

    @Description("Retrieves the terms, and number of documents for each term, for an index. " +
            "This allows to closely investigate what is stored in the index.")
    String[] getFieldTermsInfo(
            @Name("indexPath")
            @Description("The index path (empty for all indexes)")
                    String indexPath,
            @Name("field")
            @Description("The field name (empty for all fields)")
                    String field,
            @Name("max")
            @Description("The maximum number of entries to return (e.g. 100)")
                    int max
    ) throws IOException;

    @Description("Retrieves the number of documents for a specific term, for an index. " +
            "This allows to closely investigate what is stored in the index.")
    String[] getFieldTermInfo(
            @Name("indexPath")
            @Description("The index path (empty for all indexes)")
                    String indexPath,
            @Name("field")
            @Description("The field name (empty for all fields)")
                    String field,
            @Name("term")
            @Description("The term")
                    String term
    ) throws IOException;

    @Description("Returns the stored index definition for index at given path in string form")
    String getStoredIndexDefinition(@Name("indexPath") String indexPath);

    @Description("Returns the diff of index definition for index at given path from the stored index definition in " +
            "string form")
    String diffStoredIndexDefinition(@Name("indexPath") String indexPath);

    @Description("Performs consistency check on given index")
    String checkConsistency(@Name("indexPath") String indexPath,
                            @Name("fullCheck")
                            @Description("If set to true a full check would be performed which can be slow as " +
                                    "it reads all index files. If set to false a quick check is performed to " +
                                    "check if all blobs referred in index files are present in BlobStore")
                                    boolean fullCheck) throws IOException;

    @Description("Performs consistency check for all Lucene indexes and reports in simple format")
    String[] checkAndReportConsistencyOfAllIndexes(@Name("fullCheck")
                                        @Description("If set to true a full check would be performed which can be slow as " +
                                                "it reads all index files. If set to false a quick check is performed to " +
                                                "check if all blobs referred in index files are present in BlobStore")
                                                boolean fullCheck) throws IOException;

    @Description("Performs consistency check for all Lucene indexes and reports true if all indexes are found " +
            "to be valid. False if any one of them was not found to be valid")
    boolean checkConsistencyOfAllIndexes(@Name("fullCheck")
                                          @Description("If set to true a full check would be performed which can be slow as " +
                                                  "it reads all index files. If set to false a quick check is performed to " +
                                                  "check if all blobs referred in index files are present in BlobStore")
                                                  boolean fullCheck) throws IOException;


    @Description("Performs any possible cleanup of the hybrid property indexes")
    String performPropertyIndexCleanup() throws CommitFailedException;


    @Description("Fetches hybrid property index info as json for index at given path")
    String getHybridIndexInfo(@Name("indexPath") String indexPath);

}
