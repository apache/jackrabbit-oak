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
package org.apache.jackrabbit.oak.kernel;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

/**
 * Utility class for deciding which nodes and properties to serialize.
 */
class JsonFilter {

    private static final Pattern EVERYTHING = Pattern.compile(".*");

    private final List<Pattern> nodeIncludes = newArrayList(EVERYTHING);

    private final List<Pattern> nodeExcludes = newArrayList();

    private final List<Pattern> propertyIncludes = newArrayList(EVERYTHING);

    private final List<Pattern> propertyExcludes = newArrayList();

    JsonFilter(String filter) {
        JsopTokenizer tokenizer = new JsopTokenizer(filter);
        tokenizer.read('{');
        for (boolean first = true; !tokenizer.matches('}'); first = false) {
            if (!first) {
                tokenizer.read(',');
            }
            String key = tokenizer.readString();
            tokenizer.read(':');

            List<Pattern> includes = newArrayList();
            List<Pattern> excludes = newArrayList();
            readPatterns(tokenizer, includes, excludes);

            if (key.equals("nodes")) {
                nodeIncludes.clear();
                nodeIncludes.addAll(includes);
                nodeExcludes.clear();
                nodeExcludes.addAll(excludes);
            } else if (key.equals("properties")) {
                propertyIncludes.clear();
                propertyIncludes.addAll(includes);
                propertyExcludes.clear();
                propertyExcludes.addAll(excludes);
            } else {
                throw new IllegalStateException(key);
            }
        }
    }

    private void readPatterns(JsopTokenizer tokenizer, List<Pattern> includes,
            List<Pattern> excludes) {
        tokenizer.read('[');
        for (boolean first = true; !tokenizer.matches(']'); first = false) {
            if (!first) {
                tokenizer.read(',');
            }
            String pattern = tokenizer.readString();
            if (pattern.startsWith("-")) {
                excludes.add(glob(pattern.substring(1)));
            } else if (pattern.startsWith("\\-")) {
                includes.add(glob(pattern.substring(1)));
            } else {
                includes.add(glob(pattern));
            }
        }
    }

    private static Pattern glob(String pattern) {
        StringBuilder builder = new StringBuilder();
        int star = pattern.indexOf('*');
        while (star != -1) {
            if (star > 0 && pattern.charAt(star - 1) == '\\') {
                builder.append(Pattern.quote(pattern.substring(0, star - 1)));
                builder.append(Pattern.quote("*"));
            } else {
                builder.append(Pattern.quote(pattern.substring(0, star)));
                builder.append(".*");
            }
            pattern = pattern.substring(star + 1);
            star = pattern.indexOf('*');
        }
        builder.append(Pattern.quote(pattern));
        return Pattern.compile(builder.toString());
    }

    boolean includeNode(String name) {
        return include(name, nodeIncludes, nodeExcludes);
    }

    boolean includeProperty(String name) {
        return include(name, propertyIncludes, propertyExcludes);
    }

    private boolean include(
            String name, List<Pattern> includes, List<Pattern> excludes) {
        for (Pattern include : includes) {
            if (include.matcher(name).matches()) {
                for (Pattern exclude : excludes) {
                    if (exclude.matcher(name).matches()) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

}
