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

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;


import org.apache.jackrabbit.oak.spi.mount.Mount;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;

public final class MultiplexersLucene {
    /**
     * Prefix used to decorate mount names to represent index directory
     */
    public static final String INDEX_DIR_SUFFIX = "-index-data";

    public static final String SUGGEST_DIR_SUFFIX = "-suggest-data";

    public static String getIndexDirName(Mount mount) {
        if (mount.isDefault()){
            return INDEX_DATA_CHILD_NAME;
        }
        String name = mount.getPathFragmentName();
        return ":" + name + INDEX_DIR_SUFFIX;
    }

    /**
     * Checks if the node name represent an index directory node name.
     * There may be nodes for other directory like for suggester etc also.
     * This method can be used to determine if node represents index
     * directory root

     * @param name node name
     */
    public static boolean isIndexDirName(String name) {
        return name.endsWith(INDEX_DIR_SUFFIX)
                || name.equals(INDEX_DATA_CHILD_NAME);
    }

    public static String getSuggestDirName(Mount mount) {
        if (mount.isDefault()){
            return SUGGEST_DATA_CHILD_NAME;
        }
        String name = mount.getPathFragmentName();
        return ":" + name + SUGGEST_DIR_SUFFIX;
    }

    public static boolean isSuggestIndexDirName(String name) {
        return name.endsWith(SUGGEST_DIR_SUFFIX)
                || name.equals(SUGGEST_DATA_CHILD_NAME);
    }
}
