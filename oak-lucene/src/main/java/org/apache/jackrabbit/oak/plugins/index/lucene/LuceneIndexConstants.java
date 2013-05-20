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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

public interface LuceneIndexConstants {

    String TYPE_LUCENE = "lucene";

    String INDEX_DATA_CHILD_NAME = ":data";

    Version VERSION = Version.LUCENE_42;

    Analyzer ANALYZER = new StandardAnalyzer(VERSION);

    /**
     * include only certain property types in the index
     */
    String INCLUDE_PROPERTY_TYPES = "includePropertyTypes";

    String PERSISTENCE_NAME = "persistence";

    String PERSISTENCE_OAK = "repository";

    String PERSISTENCE_FILE = "file";

    String PERSISTENCE_PATH = "path";

    String INDEX_PATH = "index";

    /**
     * Lucene writer timeout write lock setting
     */
    int TO_WRITE_LOCK_MS = 50;

    /**
     * Controls how many retries should happen when there is a writer lock
     * timeout
     */
    int TO_MAX_RETRIES = 3;

    /**
     * Controls how much sleep (ms) should happen when there is a writer lock
     * timeout
     */
    int TO_SLEEP_MS = 30;

}
