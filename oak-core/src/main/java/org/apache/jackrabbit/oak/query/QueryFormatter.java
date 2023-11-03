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
package org.apache.jackrabbit.oak.query;

import java.util.Locale;

/**
 * Formatter for JCR queries in order to make them easier to read. Formatting is
 * done on a best-effort basis.
 * 
 * Warning: formatting is also done within e.g. string literals. So there is no
 * guarantee that the formatted query is semantically equal to the original one!
 */
public class QueryFormatter {

    /**
     * Detect whether the query is an XPath query.
     * 
     * @param query the query
     * @param language the language, if known, or null
     * @return true if xpath
     */
    public static boolean isXPath(String query, String language) {
        if (language != null) {
            return "xpath".equals(language);
        }
        query = query.trim().toLowerCase(Locale.ENGLISH);
        // explain queries
        if (query.startsWith("explain")) {
            query = query.substring("explain".length()).trim();
            if (query.startsWith("measure")) {
                query = query.substring("explain".length()).trim();
            }            
        }
        // union queries
        while (query.startsWith("(")) {
            query = query.substring("(".length()).trim();
        }
        if (query.startsWith("select")) {
            return false;
        }
        return true;
    }

    /**
     * Format the query into a more human-readable way, by adding newlines.
     * Warning: newlines are also added inside e.g. string literals.
     * 
     * @param query the query (may not be null)
     * @param language the query language, or null if unknown
     * @return the formatted query
     */
    public static String format(String query, String language) {
        boolean xpath = isXPath(query, language);
        if (xpath) {
            return formatXPath(query);
        } else {
            return formatSQL(query);
        }
    }

    private static String formatXPath(String query) {
        query = query.replaceAll("\\[", "\\[\n  ");
        for (String term : new String[] {
                "and ", "or ", "order by ", "option\\("
        }) {
            // xpath is case sensitive
            query = query.replaceAll(" (" + term + ")", "\n  $1");
        }
        // remove duplicate newlines
        query = query.replaceAll("\n+", "\n");
        return query;
    }
    
    private static String formatSQL(String query) {
        for (String term : new String[] {
                "union ", "from ", "where ", "and ", "or ", "order by ", "option\\("
        }) {
            // SQL is case insensitive, so we use (?i)
            query = query.replaceAll("(?i) (" + term + ")", "\n  $1");
        }
        // remove duplicate newlines
        query = query.replaceAll("\n+", "\n");
        return query;
    }
    
}
