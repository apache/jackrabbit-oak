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

/**
 * <p>Multi-valued variant of the {@link GlobPattern} that will match a given path (or tree/property) if any of the 
 * contained patterns matches. This is equivalent to creating multiple access control entries with a single rep:glob
 * restrictions each.</p>
 * 
 * <p>NOTE: An empty value array will not match any path/item</p>
 * <p>NOTE: Currently the pattern keeps a list of {@link GlobPattern} and doesn't attempt to optimize the evaluation.</p>
 * 
 * @see GlobPattern GlobPattern for details
 */
class GlobsPattern implements RestrictionPattern {
    
    private final GlobPattern[] patterns;

    GlobsPattern(@NotNull String path, @NotNull Iterable<String> restrictions)  {
        ArrayList<GlobPattern> l = new ArrayList<>(Iterables.size(restrictions));
        restrictions.forEach(restriction -> {
            if (restriction != null) {
                l.add(GlobPattern.create(path, restriction));
            }
        });
        patterns = l.toArray(new GlobPattern[0]);
    }

    @Override
    public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
        String itemPath = (property == null) ? tree.getPath() : PathUtils.concat(tree.getPath(), property.getName());
        return matches(itemPath);
    }

    @Override
    public boolean matches(@NotNull String path) {
        for (GlobPattern gp : patterns) {
            if (gp.matches(path)) {
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
        return Arrays.hashCode(patterns);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof GlobsPattern) {
            GlobsPattern other = (GlobsPattern) obj;
            return Arrays.equals(patterns, other.patterns);
        }
        return false;
    }
}