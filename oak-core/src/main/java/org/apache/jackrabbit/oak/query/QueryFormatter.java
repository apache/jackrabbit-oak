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

import org.jetbrains.annotations.NotNull;

/**
 * Formatter for JCR queries in order to make them easier to read. Formatting is
 * done on a best-effort basis.
 *
 * Warning: Care was taken to not add newlines inside string literals and so on,
 * but there is still no guarantee that the formatted query is semantically
 * equal to the original one. It is recommended to run queries that are returned
 * by these methods.
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
                query = query.substring("measure".length()).trim();
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
    public static String format(@NotNull String query, String language) {
        boolean xpath = isXPath(query, language);
        if (xpath) {
            return formatXPath(query);
        } else {
            return formatSQL(query);
        }
    }

    private static String formatXPath(String query) {
        StringBuilder buff = new StringBuilder(query);
        for (int i = 0; i < buff.length(); i++) {
            char c = buff.charAt(i);
            if (c == '\'' || c == '"') {
                while (++i < buff.length() && buff.charAt(i) != c) {
                    // skip
                }
            } else if (c =='[') {
                if (i + 1 < buff.length() && buff.charAt(i + 1) > ' ') {
                    buff.insert(i + 1, "\n  ");
                    i += 3;
                }
            } else if (c == '\n') {
                // already formatted
                while (++i < buff.length() && buff.charAt(i) == ' ') {
                    // skip
                }
                i--;
            } else if (c == ' ') {
                String sub = buff.substring(i, Math.min(i + 10, buff.length()));
                if (sub.startsWith(" and ")
                        || sub.startsWith(" or ")
                        || sub.startsWith(" order by ")
                        || sub.startsWith(" option(")) {
                    buff.setCharAt(i, '\n');
                    buff.insert(i + 1, "  ");
                    // just skip over the whitespace - but that's OK
                    i += 2;
                }
            }
        }
        return buff.toString();
    }

    private static String formatSQL(String query) {
        StringBuilder buff = new StringBuilder(query);
        for (int i = 0; i < buff.length(); i++) {
            char c = buff.charAt(i);
            if (c == '\'' || c == '"') {
                while (++i < buff.length() && buff.charAt(i) != c) {
                    // skip
                }
            } else if (c == '\n') {
                // already formatted
                while (++i < buff.length() && buff.charAt(i) == ' ') {
                    // skip
                }
                i--;
            } else if (c == ' ') {
                String sub = buff.substring(i, Math.min(i + 10, buff.length()));
                if (startsWithIgnoreCase(sub, " and ")
                        || startsWithIgnoreCase(sub, " or ")
                        || startsWithIgnoreCase(sub, " union ")
                        || startsWithIgnoreCase(sub, " from ")
                        || startsWithIgnoreCase(sub, " where ")
                        || startsWithIgnoreCase(sub, " order by ")
                        || startsWithIgnoreCase(sub, " option(")) {
                    buff.setCharAt(i, '\n');
                    buff.insert(i + 1, "  ");
                    // just skip over the whitespace - but that's OK
                    i += 2;
                }
            }
        }
        return buff.toString();
    }

    private static boolean startsWithIgnoreCase(String s, String prefix) {
        return s.regionMatches(true, 0, prefix, 0, prefix.length());
    }

}
