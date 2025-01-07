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
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <p>Implementation of the
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern}
 * interface that returns {@code true} if the name of the target property or tree
 * starts with any of the configured namespace prefixes.
 * </p>s
 * Note: an empty string prefix will match qualified item names defined with the 
 * {@link javax.jcr.NamespaceRegistry#NAMESPACE_EMPTY empty namespace}. 
 * See also sections 
 * <a href="https://s.apache.org/jcr-2.0-spec/3_Repository_Model.html#3.2.5.2%20Qualified%20Form">3.2.5.2 Qualified Form</a>
 * and 
 * <a href="https://s.apache.org/jcr-2.0-spec/3_Repository_Model.html#3.2.5.3%20Qualified%20Form%20with%20the%20Empty%20Namespace">3.2.5.3 Qualified Form with the Empty Namespace</a>
 * of the JCR v2.0 specification.
 */
class PrefixPattern implements RestrictionPattern {
    
    private final Set<String> prefixes;

    PrefixPattern(@NotNull Iterable<String> prefixes) {
        this.prefixes = CollectionUtils.toSet(prefixes);
    }

    @Override
    public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
        String name = (property != null) ? property.getName() : tree.getName();
        return matchesPrefix(name);
    }

    @Override
    public boolean matches(@NotNull String path) {
        return matchesPrefix(PathUtils.getName(path));
    }
    
    private boolean matchesPrefix(String name) {
        String prefix = Text.getNamespacePrefix(name);
        return prefixes.contains(prefix);
    }

    @Override
    public boolean matches() {
        // prefix pattern never matches for repository level permissions
        return false;
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return prefixes.hashCode();
    }

    @Override
    public String toString() {
        return prefixes.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof PrefixPattern) {
            PrefixPattern other = (PrefixPattern) obj;
            return prefixes.equals(other.prefixes);
        }
        return false;
    }
}
