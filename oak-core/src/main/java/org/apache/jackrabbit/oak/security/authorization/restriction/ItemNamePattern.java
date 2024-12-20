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

import java.util.Set;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the {@link RestrictionPattern} interface that returns
 * {@code true} if the name of the target item (property or node) is contained
 * in the configured set of names. This allows to limit certain operations (e.g.
 * reading or modifying properties) to a subset of items in the tree defined
 * by the associated policy.
 */
class ItemNamePattern implements RestrictionPattern {

    private final Set<String> names;

    ItemNamePattern(Iterable<String> names) {
        this.names = CollectionUtils.toLinkedSet(names);
    }

    @Override
    public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
        if (property != null) {
            return names.contains(property.getName());
        } else {
            return names.contains(tree.getName());
        }
    }

    @Override
    public boolean matches(@NotNull String path) {
        return (!PathUtils.denotesRoot(path) && names.contains(PathUtils.getName(path)));
    }

    @Override
    public boolean matches() {
        // name pattern never matches for repository level permissions
        return false;
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return names.hashCode();
    }

    @Override
    public String toString() {
        return names.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ItemNamePattern) {
            ItemNamePattern other = (ItemNamePattern) obj;
            return names.equals(other.names);
        }
        return false;
    }
}
