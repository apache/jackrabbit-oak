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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.bson.Document;

final class MongotNativeQueryString {

    private MongotNativeQueryString() {
    }

    static Document operator(String query) {
        return new Document("queryString", new Document("defaultPath", MongoFieldNames.FULLTEXT)
                .append("query", rewriteFields(query)));
    }

    private static String rewriteFields(String query) {
        StringBuilder rewritten = new StringBuilder(query.length());
        boolean clauseStart = true;
        boolean quoted = false;
        boolean regex = false;
        boolean escaped = false;

        for (int i = 0; i < query.length();) {
            char current = query.charAt(i);
            if (escaped) {
                rewritten.append(current);
                escaped = false;
                i++;
                continue;
            }
            if (current == '\\') {
                rewritten.append(current);
                escaped = true;
                i++;
                continue;
            }
            if (regex) {
                rewritten.append(current);
                if (current == '/') {
                    regex = false;
                }
                i++;
                continue;
            }
            if (current == '"') {
                quoted = !quoted;
                rewritten.append(current);
                clauseStart = false;
                i++;
                continue;
            }
            if (quoted) {
                rewritten.append(current);
                i++;
                continue;
            }
            if (current == '/') {
                regex = true;
                rewritten.append(current);
                clauseStart = false;
                i++;
                continue;
            }
            if (Character.isWhitespace(current)) {
                rewritten.append(current);
                clauseStart = true;
                i++;
                continue;
            }
            if (current == '(') {
                rewritten.append(current);
                clauseStart = true;
                i++;
                continue;
            }
            if (clauseStart) {
                int fieldStart = current == '+' || current == '-' ? i + 1 : i;
                int fieldEnd = fieldEnd(query, fieldStart);
                if (fieldEnd >= 0) {
                    if (fieldStart > i) {
                        rewritten.append(current);
                    }
                    String propertyName = unescape(query.substring(fieldStart, fieldEnd));
                    rewritten.append(MongoFieldNames.ANALYZED)
                            .append('.')
                            .append(MongoFieldNames.encodeProperty(propertyName))
                            .append(':');
                    i = fieldEnd + 1;
                    clauseStart = false;
                    continue;
                }
            }
            rewritten.append(current);
            clauseStart = false;
            i++;
        }
        return rewritten.toString();
    }

    private static int fieldEnd(String query, int start) {
        if (start >= query.length()) {
            return -1;
        }
        boolean escaped = false;
        for (int i = start; i < query.length(); i++) {
            char current = query.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (current == '\\') {
                escaped = true;
                continue;
            }
            if (current == ':') {
                return i == start ? -1 : i;
            }
            if (Character.isWhitespace(current)
                    || current == '(' || current == ')'
                    || current == '[' || current == ']'
                    || current == '{' || current == '}'
                    || current == '"' || current == '/') {
                return -1;
            }
        }
        return -1;
    }

    private static String unescape(String fieldName) {
        StringBuilder unescaped = new StringBuilder(fieldName.length());
        boolean escaped = false;
        for (int i = 0; i < fieldName.length(); i++) {
            char current = fieldName.charAt(i);
            if (escaped) {
                unescaped.append(current);
                escaped = false;
            } else if (current == '\\') {
                escaped = true;
            } else {
                unescaped.append(current);
            }
        }
        if (escaped) {
            unescaped.append('\\');
        }
        return unescaped.toString();
    }
}
