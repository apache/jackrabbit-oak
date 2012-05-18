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
package org.apache.jackrabbit.mk.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple name filter utility class.
 * <ul>
 *     <li>a filter consists of one or more <i>globs</i></li>
 *     <li>a <i>glob</i> prefixed by {@code "\-"} is treated as an exclusion pattern;
 *     all others are considered inclusion patterns</li>
 *     <li>{@code "\*"} serves as a <i>wild card</i>, i.e. it matches any substring in the target name</li>
 *     <li>a filter matches a target name if any of the inclusion patterns match but
 *     none of the exclusion patterns</li>
 * </ul>
 * Example:
 * <p/>
 * {@code ["foo\*", "\-foo99"]} matches {@code "foo"} and {@code "foo bar"}
 * but not {@code "foo99"}.
 */
public class NameFilter {

    public static String WILDCARD = "\\*";
    public static String EXCLUDE_PREFIX = "\\-";

    // list of ORed inclusion patterns
    private final List<String> inclPatterns = new ArrayList<String>();
    // list of ORed exclusion patterns
    private final List<String> exclPatterns = new ArrayList<String>();

    public NameFilter(String[] patterns) {
        for (String pattern : patterns) {
            if (pattern.startsWith(EXCLUDE_PREFIX)) {
                exclPatterns.add(pattern.substring(EXCLUDE_PREFIX.length()));
            } else {
                inclPatterns.add(pattern);
            }
        }
    }

    public boolean matches(String name) {
        boolean matched = false;
        // check inclusion patterns
        for (String pattern : inclPatterns) {
            if (internalMatches(name, pattern, 0, 0)) {
                matched = true;
                break;
            }
        }
        if (matched) {
            // check exclusion patterns
            for (String pattern : exclPatterns) {
                if (internalMatches(name, pattern, 0, 0)) {
                    matched = false;
                    break;
                }
            }
        }
        return matched;
    }

    /**
     * Internal helper used to recursively match the pattern
     *
     * @param s       The string to be tested
     * @param pattern The pattern
     * @param sOff    offset within <code>s</code>
     * @param pOff    offset within <code>pattern</code>.
     * @return true if <code>s</code> matched pattern, else false.
     */
    private static boolean internalMatches(String s, String pattern,
                                           int sOff, int pOff) {
        int pLen = pattern.length();
        int sLen = s.length();

        while (true) {
            if (pOff >= pLen) {
                return sOff >= sLen ? true : false;
            }
            if (sOff >= sLen && pattern.indexOf(WILDCARD, pOff) != pOff) {
                return false;
            }

            // check for a wildcard '\*' as the next pattern;
            // this is handled by a recursive call for
            // each postfix of the name.
            if (pattern.indexOf(WILDCARD, pOff) == pOff) {
                pOff += WILDCARD.length();
                if (pOff >= pLen) {
                    return true;
                }

                while (true) {
                    if (internalMatches(s, pattern, sOff, pOff)) {
                        return true;
                    }
                    if (sOff >= sLen) {
                        return false;
                    }
                    sOff++;
                }
            }

            if (pOff < pLen && sOff < sLen) {
                if (pattern.charAt(pOff) != s.charAt(sOff)) {
                    return false;
                }
            }
            pOff++;
            sOff++;
        }
    }
}
