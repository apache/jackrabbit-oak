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
package org.apache.jackrabbit.oak.query;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles literal query templates with numbered string, number and path captures.
 */
final class QueryTemplate {

    private static final String PREFIX = "\\A(\\s*(?i:explain\\s+)?(?i:measure\\s+)?)";
    private static final String NUMBER_START = "(?<![\\p{L}\\p{N}_.])";
    private static final String NUMBER_END = "(?![\\p{L}\\p{N}_.])";
    private static final String NUMBER = NUMBER_START + "[+-]?"
            + "(?:[0-9]++(?:\\.[0-9]++)?|\\.[0-9]++)(?:[eE][+-]?[0-9]++)?"
            + NUMBER_END;
    private static final String SEGMENT = "[\\p{L}_][\\p{L}\\p{N}\\p{M}_.:-]*+";
    private static final String PATH = SEGMENT + "(?:/" + SEGMENT + ")*";

    final Pattern find;
    final String replace;

    QueryTemplate(String find, String replace) {
        Map<String, Character> captures = new HashMap<>();
        this.find = Pattern.compile(PREFIX + compile(find, captures, false) + "\\z");
        this.replace = "$1" + compile(replace, captures, true);
    }

    private static String compile(String template, Map<String, Character> captures, boolean replacement) {
        StringBuilder result = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        char quote = 0;
        int quoteStart = -1;
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '#') {
                if (i + 1 < template.length() && template.charAt(i + 1) == '#') {
                    literal.append('#');
                    i++;
                    continue;
                }
                int end = i + 1;
                while (end < template.length() && template.charAt(end) >= '0'
                        && template.charAt(end) <= '9') {
                    end++;
                }
                if (end > i + 1) {
                    String id = template.substring(i + 1, end);
                    if (id.charAt(0) == '0') {
                        throw new IllegalArgumentException("Template capture numbers must start at 1");
                    }
                    char type = 'n';
                    if (quote != 0) {
                        if (i != quoteStart + 1 || end == template.length() || template.charAt(end) != quote
                                || end + 1 < template.length() && template.charAt(end + 1) == quote) {
                            throw new IllegalArgumentException("A string capture must occupy the entire literal");
                        }
                        type = quote;
                    } else if (i > 0 && template.charAt(i - 1) == '/'
                            && (end == template.length() || template.charAt(end) == '/'
                                    || template.charAt(end) == '[')) {
                        type = '/';
                    }
                    Character previous = captures.get(id);
                    if (previous != null && previous != type) {
                        throw new IllegalArgumentException("Conflicting types for template capture #" + id);
                    }
                    appendLiteral(result, literal, replacement);
                    if (replacement) {
                        if (previous == null) {
                            throw new IllegalArgumentException("Undefined template capture #" + id);
                        }
                        result.append("${p").append(id).append('}');
                    } else if (previous != null) {
                        if (type == 'n') {
                            result.append(NUMBER_START);
                        }
                        result.append("\\k<p").append(id).append('>');
                        if (type == 'n') {
                            result.append(NUMBER_END);
                        }
                    } else {
                        captures.put(id, type);
                        String expression = type == '/' ? PATH : type == 'n' ? NUMBER
                                : "(?:[^" + type + "]++|" + type + type + ")*+";
                        result.append("(?<p").append(id).append('>').append(expression).append(')');
                    }
                    i = end - 1;
                    continue;
                }
            }
            literal.append(c);
            if (c == '\'' || c == '"') {
                if (quote == 0) {
                    quote = c;
                    quoteStart = i;
                } else if (quote == c) {
                    if (i + 1 < template.length() && template.charAt(i + 1) == c) {
                        literal.append(c);
                        i++;
                    } else {
                        quote = 0;
                    }
                }
            }
        }
        appendLiteral(result, literal, replacement);
        return result.toString();
    }

    private static void appendLiteral(StringBuilder result, StringBuilder literal, boolean replacement) {
        if (literal.length() != 0) {
            String text = literal.toString();
            result.append(replacement ? Matcher.quoteReplacement(text) : Pattern.quote(text));
            literal.setLength(0);
        }
    }
}
