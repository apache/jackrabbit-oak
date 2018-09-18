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
package org.apache.jackrabbit.oak.spi.mount;

import static org.apache.jackrabbit.oak.spi.mount.FragmentMatcher.Result.FULL_MATCH;
import static org.apache.jackrabbit.oak.spi.mount.FragmentMatcher.Result.MISMATCH;
import static org.apache.jackrabbit.oak.spi.mount.FragmentMatcher.Result.PARTIAL_MATCH;

/**
 * This utility class allows to match strings against a simple pattern language.
 * There are two special characters:
 * <ul>
 *     <li><code>*</code> - matches zero or more any characters different than slash</li>
 *     <li><code>$</code> - matches the end of the subject</li>
 * </ul>
 */
public final class FragmentMatcher {

    private FragmentMatcher() {
    }

    public enum Result {
        FULL_MATCH, PARTIAL_MATCH, MISMATCH
    }

    /**
     * Check if the subject starts with the pattern. See the class docs for the
     * pattern syntax.
     *
     * @param pattern pattern to be matched
     * @param subject subject
     * @return {@link Result#FULL_MATCH} if the subject starts with the pattern,
     * {@link Result#PARTIAL_MATCH} if the subject is shorter than the pattern,
     * but matches it so far and {@link Result#MISMATCH} if it doesn't start with
     * the pattern.
     */
    public static Result startsWith(String pattern, String subject) {
        int i = 0, j = 0;
        char patternChar = 0, subjectChar;

        while (i < pattern.length() && j < subject.length()) {
            patternChar = pattern.charAt(i);
            subjectChar = subject.charAt(j);
            switch (patternChar) {
                case '*': // matches everything until the next slash
                    if (subjectChar == '/') {
                        i++;
                    } else {
                        j++;
                    }
                    break;

                case '$':
                    return MISMATCH;

                default:
                    if (patternChar != subjectChar) {
                        return MISMATCH;
                    } else {
                        i++;
                        j++;
                        break;
                    }
            }
        }
        if (j == subject.length()) {
            while (i < pattern.length()) {
                patternChar = pattern.charAt(i);
                switch (patternChar) {
                    case '*':
                        i++;
                        break;
                    case '$':
                        i++;
                        break;
                    default:
                        return PARTIAL_MATCH;
                }
            }
            return FULL_MATCH;
        } else {
            return FULL_MATCH;
        }
    }
}
