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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * Matcher which matches all child nodes
 */
class IncludeAllMatcher implements Matcher {
    private final String matchingPath;
    private final int depth;

    IncludeAllMatcher(String matchingPath, int depth) {
        checkArgument(depth > 0);
        this.matchingPath = matchingPath;
        this.depth = depth;
    }

    @Override
    public Matcher next(String name) {
        return new IncludeAllMatcher(concat(matchingPath, name), depth + 1);
    }

    @Override
    public boolean isMatch() {
        return true;
    }

    @Override
    public String getMatchedPath() {
        return matchingPath;
    }

    @Override
    public int depth() {
        return depth;
    }

    @Override
    public boolean matchesAllChildren() {
        return true;
    }

    @Override
    public String toString() {
        return "ALL - " + matchingPath;
    }
}
