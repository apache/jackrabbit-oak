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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern}
 * interface that returns {@code true} if the name of the target property or tree
 * starts with any of the configured namespace prefixes.
 */
class PrefixPattern implements RestrictionPattern {

    private static final Logger log = LoggerFactory.getLogger(PrefixPattern.class);

    private final Set<String> prefixes;

    PrefixPattern(@Nonnull Iterable<String> prefixes) {
        this.prefixes = ImmutableSet.copyOf(prefixes);
    }

    @Override
    public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
        String name = (property != null) ? property.getName() : tree.getName();
        String prefix = Text.getNamespacePrefix(name);
        if (!prefix.isEmpty()) {
            for (String p : prefixes) {
                if (prefix.equals(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean matches(@Nonnull String path) {
        log.debug("Unable to validate node type restriction.");
        return false;
    }

    @Override
    public boolean matches() {
        // node type pattern never matches for repository level permissions
        return false;
    }

    //-------------------------------------------------------------< Object >---
    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return prefixes.hashCode();
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return prefixes.toString();
    }

    /**
     * @see Object#equals(Object)
     */
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