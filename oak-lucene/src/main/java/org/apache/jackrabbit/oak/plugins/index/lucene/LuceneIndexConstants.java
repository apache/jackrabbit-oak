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
import org.apache.lucene.util.Version;

public interface LuceneIndexConstants {

    String TYPE_LUCENE = "lucene";

    String INDEX_DATA_CHILD_NAME = ":data";

    Version VERSION = Version.LUCENE_47;

    Analyzer ANALYZER = new OakAnalyzer(VERSION);

    /**
     * include only certain property types in the index
     */
    String INCLUDE_PROPERTY_TYPES = "includePropertyTypes";

    /**
     * exclude certain properties by name
     */
    String EXCLUDE_PROPERTY_NAMES = "excludePropertyNames";

    String PERSISTENCE_NAME = "persistence";

    String PERSISTENCE_OAK = "repository";

    String PERSISTENCE_FILE = "file";

    String PERSISTENCE_PATH = "path";

    String INDEX_DATA_CHILD_NAME_FS = "data";

    /**
     * Experimental flag to control storage behavior: 'null' or 'true' means the content is stored
     */
    String EXPERIMENTAL_STORAGE = "oak.experimental.storage";

    /**
     * Determines if full text indexing is enabled for this index definition.
     * Default is true
     */
    String FULL_TEXT_ENABLED = "fulltextEnabled";

    /**
     * Only include properties with name in this set. If this property is defined
     * then {@code excludePropertyNames} would be ignored
     */
    String INCLUDE_PROPERTY_NAMES = "includePropertyNames";

    /**
     * Type of the property being indexed defined as part of property definition
     * under the given index definition. Refer to {@ling javax.jcr.PropertyType}
     * contants for the possible values
     */
    String PROP_TYPE = "propertyType";
}
