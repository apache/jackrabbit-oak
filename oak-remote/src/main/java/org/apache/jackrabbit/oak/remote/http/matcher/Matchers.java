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

package org.apache.jackrabbit.oak.remote.http.matcher;

import java.util.regex.Pattern;

/**
 * Collection of matchers for HTTP requests.
 */
public class Matchers {

    private Matchers() {
    }

    /**
     * Create a matcher that will be satisfied when given requests have a method
     * matching the one provided as a parameter.
     *
     * @param method Method that requests must have for the matcher to be
     *               satisfied.
     * @return An instance of {@code Matcher}.
     */
    public static Matcher matchesMethod(String method) {
        if (method == null) {
            throw new IllegalArgumentException("method not provided");
        }

        return new MethodMatcher(method);
    }

    /**
     * Create a matcher that will be satisfied when given requests have a patch
     * matching the pattern provided as a parameter.
     *
     * @param pattern The pattern to use when checking the requests given to the
     *                matcher.
     * @return An instance of {@code Matcher}.
     */
    public static Matcher matchesPath(String pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern not provided");
        }

        return new PathMatcher(Pattern.compile(pattern));
    }

    /**
     * Create a matcher that will be satisfied when the given requests satisfies
     * every matcher provided as parameters. Calling this method is equivalent
     * as checking every provided matcher individually and chaining each result
     * as a short-circuit and.
     *
     * @param matchers The matchers that have to be satisfied for the returned
     *                 matcher to be satisfied.
     * @return An instance of {@code Matcher}.
     */
    public static Matcher matchesAll(Matcher... matchers) {
        if (matchers == null) {
            throw new IllegalArgumentException("matchers not provided");
        }

        for (Matcher matcher : matchers) {
            if (matcher == null) {
                throw new IllegalArgumentException("invalid matcher");
            }
        }

        return new AllMatcher(matchers);
    }

    /**
     * Create a matcher that will be satisifed when the given requests match the
     * provided method and path.
     *
     * @param method The method that requests must have for the matcher to be
     *               satisfied.
     * @param path   The pattern to use when checking the requests given to the
     *               matcher.
     * @return An instance of {@code Matcher}.
     */
    public static Matcher matchesRequest(String method, String path) {
        return matchesAll(matchesMethod(method), matchesPath(path));
    }

}
