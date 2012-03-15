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

package org.apache.jackrabbit.oak.jcr.util;

import java.util.StringTokenizer;

public final class ItemNameMatcher {
    private static final char WILDCARD_CHAR = '*';
    private static final String OR = "|";

    private ItemNameMatcher() { }

    /**
     * Matches the name pattern against the specified name.
     * <p/>
     * The pattern may be a full name or a partial name with one or more
     * wildcard characters ("*"), or a disjunction (using the "|" character
     * to represent logical <i>OR</i>) of these. For example,
     * <p/>
     * {@code "jcr:*|foo:bar"}
     * <p/>
     * would match
     * <p/>
     * {@code "foo:bar"}, but also {@code "jcr:whatever"}.
     * <p/>
     * <pre>
     * The EBNF for pattern is:
     *
     * namePattern ::= disjunct {'|' disjunct}
     * disjunct ::= name [':' name]
     * name ::= '*' |
     *          ['*'] fragment {'*' fragment}['*']
     * fragment ::= char {char}
     * char ::= nonspace | ' '
     * nonspace ::= (* Any Unicode character except:
     *               '/', ':', '[', ']', '*',
     *               ''', '"', '|' or any whitespace
     *               character *)
     * </pre>
     * Note that leading and trailing whitespace around a pattern <i>is</i> ignored.
     *
     * @param name the name to test the pattern with
     * @param pattern the pattern to be matched against the name
     * @return true if the specified name matches the pattern
     * @see javax.jcr.Node#getNodes(String)
     */
    public static boolean matches(String name, String pattern) {
        StringTokenizer st = new StringTokenizer(pattern, OR, false);
        while (st.hasMoreTokens()) {
            // remove leading & trailing whitespace from token
            String token = st.nextToken().trim();
            if (internalMatches(name, token, 0, 0)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches the {@code nameGlob} strings in the passed array against
     * the specified name.
     * <p>
     * A glob may be a full name or a partial name with one or more
     * wildcard characters ("{@code *}").
     * <p>
     * Note that unlike in the case of the {@link #matches(String, String)}
     * leading and trailing whitespace around a glob is <i>not</i> ignored.
     *
     * @param name the name to test the pattern with
     * @param nameGlobs an array of globbing strings
     * @return true if the specified name matches any of the globs
     * @see javax.jcr.Node#getNodes(String[])
     */
    public static boolean matches(String name, String[] nameGlobs) {
        for (String nameGlob : nameGlobs) {
            // use globbing string as-is. Don't trim any leading/trailing whitespace
            if (internalMatches(name, nameGlob, 0, 0)) {
                return true;
            }
        }
        return false;
    }

    //------------------------------------------< private >---

    /**
     * Internal helper used to recursively match the pattern
     *
     * @param s       The string to be tested
     * @param pattern The pattern
     * @param sOff    offset within {@code s}
     * @param pOff    offset within {@code pattern}.
     * @return true if {@code s} matched pattern, else false.
     */
    private static boolean internalMatches(String s, String pattern, int sOff, int pOff) {
        int pLen = pattern.length();
        int sLen = s.length();

        while (true) {
            if (pOff >= pLen) {
                if (sOff >= sLen) {
                    return true;
                } else if (s.charAt(sOff) == '[') {
                    // check for subscript notation (e.g. "whatever[1]")
                    // the entire pattern matched up to the subscript:
                    // -> ignore the subscript
                    return true;
                } else {
                    return false;
                }
            }
            if (sOff >= sLen && pattern.charAt(pOff) != WILDCARD_CHAR) {
                return false;
            }

            // check for a '*' as the next pattern char; this is handled by
            // a recursive call for each postfix of the name.
            if (pattern.charAt(pOff) == WILDCARD_CHAR) {
                if (++pOff >= pLen) {
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
