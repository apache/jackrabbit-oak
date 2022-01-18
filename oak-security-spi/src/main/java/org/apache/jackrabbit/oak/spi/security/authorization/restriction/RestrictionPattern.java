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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface used to verify if a given {@code restriction} applies to a given
 * item or path.
 */
public interface RestrictionPattern {

    /**
     * Returns {@code true} if the underlying restriction matches the specified
     * tree or property state.
     *
     * @param tree The target tree or the parent of the target property.
     * @param property The target property state or {@code null} if the target
     * item is a tree.
     * @return {@code true} if the underlying restriction matches the specified
     * tree or property state; {@code false} otherwise.
     */
    boolean matches(@NotNull Tree tree, @Nullable PropertyState property);

    /**
     * Returns {@code true} if the underlying restriction matches the specified path.
     * Note, that if the nature of the item at {@code path} is know {@link #matches(String, boolean)} should be called 
     * instead.
     *
     * @param path The path of the target item.
     * @return {@code true} if the underlying restriction matches the specified
     * path; {@code false} otherwise.
     */
    boolean matches(@NotNull String path);

    /**
     * Returns {@code true} if the underlying restriction matches the specified path and item type.
     * If the nature of the item at {@code path} is unknown {@link #matches(String)} should be called instead.
     * 
     * Note, for backwards compatibility this method comes with a default implementation making it equivalent to {@link #matches(String)}.
     * Implementations of the {@link RestrictionPattern} interface should overwrite the default if the underlying 
     * restriction applies different behavior for nodes and properties.
     *
     * @param path The path of the target item.
     * @param isProperty If {@code true} the target item is known to be a property, otherwise it is known to be a node.
     * @return {@code true} if the underlying restriction matches the specified path and item type; {@code false} otherwise.
     * @since OAK 1.42.0
     */
    default boolean matches(@NotNull String path, boolean isProperty) {
        return matches(path);
    }

    /**
     * Returns {@code true} if the underlying restriction matches for repository
     * level permissions.
     *
     * @return {@code true} if the underlying restriction matches for repository
     * level permissions that are not associated with a path or a dedicated item;
     * {@code false} otherwise.
     */
    boolean matches();

    /**
     * Default implementation of the {@code RestrictionPattern} that always
     * returns {@code true} and thus matches all items or paths.
     */
    RestrictionPattern EMPTY = new RestrictionPattern() {
        @Override
        public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
            return true;
        }

        @Override
        public boolean matches(@NotNull String path) {
            return true;
        }

        @Override
        public boolean matches(@NotNull String path, boolean isProperty) {
            return true;
        }

        @Override
        public boolean matches() {
            return true;
        }

        @Override
        public String toString() {
            return "RestrictionPattern.EMPTY";
        }
    };
}
