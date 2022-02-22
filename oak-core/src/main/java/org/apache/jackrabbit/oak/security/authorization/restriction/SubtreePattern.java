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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

class SubtreePattern implements RestrictionPattern {
    
    private static final String SEGMENT_DELIM = "/";
    
    private final String oakPath;
    private final int oakPathLength;
    private final String[] targets;
    private final String[] subtrees;
    
    SubtreePattern(@NotNull String oakPath, @NotNull Iterable<String> subtrees) {
        this.oakPath = oakPath;
        this.oakPathLength = oakPath.length();
        int size = Iterables.size(subtrees);
        List<String> tl = new ArrayList<>(size);
        List<String> sl = new ArrayList<>(size);
        subtrees.forEach(s -> {
            if (s != null && !s.isEmpty()) {
                if (s.endsWith(SEGMENT_DELIM)) {
                    sl.add(s);
                } else {
                    tl.add(s);
                    sl.add((s + SEGMENT_DELIM));
                }
            }
        });
        this.targets = tl.toArray(new String[0]);        
        this.subtrees = sl.toArray(new String[0]);
    }
    
    @Override
    public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
        String path = (property == null) ? tree.getPath() : PathUtils.concat(tree.getPath(), property.getName());
        return matches(path);
    }

    @Override
    public boolean matches(@NotNull String path) {
        if (path.indexOf(oakPath) != 0) {
            return false;
        }
        for (String subtree : subtrees) {
            if (path.indexOf(subtree, oakPathLength) > -1) {
                return true;
            }
        }
        for (String target : targets) {
            if (path.endsWith(target)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean matches() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(oakPath, Arrays.hashCode(targets), Arrays.hashCode(subtrees));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof SubtreePattern) {
            SubtreePattern other = (SubtreePattern) obj;
            return oakPath.equals(other.oakPath) && Arrays.equals(targets, other.targets) && Arrays.equals(subtrees, other.subtrees);
        }
        return false;
    }
}