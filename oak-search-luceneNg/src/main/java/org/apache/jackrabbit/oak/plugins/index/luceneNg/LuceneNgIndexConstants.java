/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;

/**
 * Constants for Lucene 9 index implementation.
 */
public interface LuceneNgIndexConstants extends FulltextIndexConstants {

    /**
     * Index type for Lucene 9 indexes.
     * Type identifier remains version-specific for index format compatibility.
     */
    String TYPE_LUCENE9 = "lucene9";

    /**
     * Base path for Lucene index storage in repository.
     * Version-agnostic path shared across Lucene versions.
     */
    String VAR_INDEXING_BASE_PATH = "/var/indexing/lucene";

    /**
     * Property for listing directory contents (file names).
     */
    String PROP_DIR_LISTING = "dirListing";

    /**
     * Property for blob size.
     */
    String PROP_BLOB_SIZE = "blobSize";
}
