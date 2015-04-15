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
package org.apache.jackrabbit.oak.plugins.index.solr.util;

import javax.jcr.PropertyType;

/**
 * Solr utility class
 */
public class SolrUtils {

    /**
     * Escape a char sequence in order to make it usable within a Solr query
     *
     * @param s the String to escape
     * @return an escaped String
     */
    public static CharSequence partialEscape(CharSequence s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '!' || c == '(' || c == ')' ||
                    c == ':' || c == '^' || c == '[' || c == ']' || c == '/' ||
                    c == '{' || c == '}' || c == '~' || c == '*' || c == '?' ||
                    c == '-' || c == ' ') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb;
    }

    /**
     * Get the name of the field to be used for sorting a certain property
     *
     * @param tag          the {@code Type#tag} of the given property
     * @param propertyName the name of the given property
     * @return the name of the Solr field to be used for sorting on the given property
     */
    public static String getSortingField(int tag, String propertyName) {
        switch (tag) {
            case PropertyType.BINARY:
                return propertyName + "_binary_sort";
            case PropertyType.DOUBLE:
                return propertyName + "_double_sort";
            case PropertyType.DECIMAL:
                return propertyName + "_double_sort";
            default:
                return propertyName + "_string_sort";
        }
    }

}
