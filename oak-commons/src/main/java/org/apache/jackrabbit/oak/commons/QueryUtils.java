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
package org.apache.jackrabbit.oak.commons;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.util.Text;

public final class QueryUtils {

    private QueryUtils() {}

    /**
     * Escape {@code string} for matching in jcr escaped node names
     *
     * @param string string to escape
     * @return escaped string
     */
    @Nonnull
    public static String escapeNodeName(@Nonnull String string) {
        StringBuilder result = new StringBuilder();

        int k = 0;
        int j;
        do {
            j = string.indexOf('%', k);
            if (j < 0) {
                // jcr escape trail
                result.append(Text.escapeIllegalJcrChars(string.substring(k)));
            } else if (j > 0 && string.charAt(j - 1) == '\\') {
                // literal occurrence of % -> jcr escape
                result.append(Text.escapeIllegalJcrChars(string.substring(k, j) + '%'));
            } else {
                // wildcard occurrence of % -> jcr escape all but %
                result.append(Text.escapeIllegalJcrChars(string.substring(k, j))).append('%');
            }

            k = j + 1;
        } while (j >= 0);

        return result.toString();
    }

    @Nonnull
    public static String escapeForQuery(@Nonnull String value) {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                ret.append("\\\\");
            } else if (c == '\'') {
                ret.append("''");
            } else {
                ret.append(c);
            }
        }
        return ret.toString();
    }
}