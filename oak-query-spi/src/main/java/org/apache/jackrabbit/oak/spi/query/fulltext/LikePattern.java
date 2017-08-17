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
package org.apache.jackrabbit.oak.spi.query.fulltext;

/**
 * A pattern matcher.
 */
public class LikePattern {

    // TODO LIKE: optimize condition to '=' when no patterns are used, or 'between x and x+1'
    // TODO LIKE: what to do for invalid patterns (patterns ending with a backslash)

    private static final int MATCH = 0, ONE = 1, ANY = 2;

    private String patternString;
    private boolean invalidPattern;
    private char[] patternChars;
    private int[] patternTypes;
    private int patternLength;
    private String lowerBounds, upperBound;

    public LikePattern(String pattern) {
        initPattern(pattern);
        initBounds();
    }

    public boolean matches(String value) {
        return !invalidPattern && compareAt(value, 0, 0, value.length(), patternChars, patternTypes);
    }

    private static boolean compare(char[] pattern, String s, int pi, int si) {
        return pattern[pi] == s.charAt(si);
    }

    private boolean compareAt(String s, int pi, int si, int sLen, char[] pattern, int[] types) {
        for (; pi < patternLength; pi++) {
            int type = types[pi];
            switch (type) {
            case MATCH:
                if (si >= sLen || !compare(pattern, s, pi, si++)) {
                    return false;
                }
                break;
            case ONE:
                if (si++ >= sLen) {
                    return false;
                }
                break;
            case ANY:
                if (++pi >= patternLength) {
                    return true;
                }
                while (si < sLen) {
                    if (compare(pattern, s, pi, si) && compareAt(s, pi, si, sLen, pattern, types)) {
                        return true;
                    }
                    si++;
                }
                return false;
            default:
                throw new IllegalArgumentException("Internal error: " + type);
            }
        }
        return si == sLen;
    }

    private void initPattern(String p) {
        patternLength = 0;
        if (p == null) {
            patternTypes = null;
            patternChars = null;
            return;
        }
        int len = p.length();
        patternChars = new char[len];
        patternTypes = new int[len];
        boolean lastAny = false;
        for (int i = 0; i < len; i++) {
            char c = p.charAt(i);
            int type;
            if (c == '\\') {
                if (i >= len - 1) {
                    invalidPattern = true;
                    return;
                }
                c = p.charAt(++i);
                type = MATCH;
                lastAny = false;
            } else if (c == '%') {
                if (lastAny) {
                    continue;
                }
                type = ANY;
                lastAny = true;
            } else if (c == '_') {
                type = ONE;
            } else {
                type = MATCH;
                lastAny = false;
            }
            patternTypes[patternLength] = type;
            patternChars[patternLength++] = c;
        }
        for (int i = 0; i < patternLength - 1; i++) {
            if (patternTypes[i] == ANY && patternTypes[i + 1] == ONE) {
                patternTypes[i] = ONE;
                patternTypes[i + 1] = ANY;
            }
        }
        patternString = new String(patternChars, 0, patternLength);
    }

    @Override
    public String toString() {
        return patternString;
    }

    /**
     * Get the lower bound if any.
     *
     * @return return the lower bound, or null if unbound
     */
    public String getLowerBound() {
        return lowerBounds;
    }

    /**
     * Get the upper bound if any.
     *
     * @return return the upper bound, or null if unbound
     */
    public String getUpperBound() {
        return upperBound;
    }

    private void initBounds() {
        if (invalidPattern) {
            return;
        }
        if (patternLength <= 0 || patternTypes[0] != MATCH) {
            // can't use an index
            return;
        }
        int maxMatch = 0;
        StringBuilder buff = new StringBuilder();
        while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
            buff.append(patternChars[maxMatch++]);
        }
        String lower = buff.toString();
        if (lower.isEmpty()) {
            return;
        }
        if (maxMatch == patternLength) {
            lowerBounds = upperBound = lower;
            return;
        }
        lowerBounds = lower;
        char next = lower.charAt(lower.length() - 1);
        // search the 'next' unicode character (or at least a character
        // that is higher)
        for (int i = 1; i < 2000; i++) {
            String upper = lower.substring(0, lower.length() - 1) + (char) (next + i);
            if (upper.compareTo(lower) > 0) {
                upperBound = upper;
                return;
            }
        }
    }

}