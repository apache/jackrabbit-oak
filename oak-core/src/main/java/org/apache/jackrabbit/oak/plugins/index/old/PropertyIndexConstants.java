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
package org.apache.jackrabbit.oak.plugins.index.old;

import org.apache.jackrabbit.oak.plugins.index.IndexUtils;

public interface PropertyIndexConstants {

    String INDEX_TYPE_PROPERTY = "property";

    /**
     * The root node of the index definition (configuration) nodes.
     */
    // TODO OAK-178 discuss where to store index config data
    String INDEX_CONFIG_PATH = IndexUtils.DEFAULT_INDEX_HOME + IndexUtils.INDEX_DEFINITIONS_NAME + "/indexes";
    // "/jcr:system/indexes";

    /**
     * For each index, the index content is stored relative to the index
     * definition below this node. There is also such a node just below the
     * index definition node, to store the last revision and for temporary data.
     */
    String INDEX_CONTENT = ":data";

    /**
     * The node name prefix of a prefix index.
     */
    String TYPE_PREFIX = "prefix@";

    /**
     * The node name prefix of a property index.
     */
    // TODO support multi-property indexes
    String TYPE_PROPERTY = "property@";

    /**
     * Marks a unique index.
     */
    String UNIQUE = "unique";

}
