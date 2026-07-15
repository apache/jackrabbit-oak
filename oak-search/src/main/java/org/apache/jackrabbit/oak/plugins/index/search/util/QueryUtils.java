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

package org.apache.jackrabbit.oak.plugins.index.search.util;

public class QueryUtils {

    public static final char WILDCARD_STRING = '*';
    public static final char WILDCARD_CHAR = '?';
    public static final char WILDCARD_ESCAPE = '\\';

    private static void replaceWildcard(StringBuilder sb, int position, char oldWildcard, char newWildcard) {
        if (sb.charAt(position) == oldWildcard) {
            int escapeCount = 0;
            for (int m = position - 1; m >= 0 && sb.charAt(m) == WILDCARD_ESCAPE; m--, escapeCount++) ;
            if (escapeCount % 2 == 0) {
                sb.setCharAt(position, newWildcard);
            }
        }
    }

    /**
     * Converts an SQL2 like pattern to a Lucene/Elastic pattern for wildcard queries. In particular, this method
     * converts any wildcard in the SQL2 like pattern (% and _) to Lucene wildcards (* and ?), handling escaped
     * characters (as in \% and \_).
     *
     * @param likePattern The match pattern operator in SQL syntax
     * @return The match pattern for Lucene/Elastic wildcard queries.
     */
    public static String sqlLikeToLuceneWildcardQuery(String likePattern) {
        StringBuilder firstBuilder = new StringBuilder(likePattern);
        for (int k = 0; k < firstBuilder.length(); k++) {
            replaceWildcard(firstBuilder, k, '%', WILDCARD_STRING);
            replaceWildcard(firstBuilder, k, '_', WILDCARD_CHAR);
        }
        return firstBuilder.toString();
    }

}
