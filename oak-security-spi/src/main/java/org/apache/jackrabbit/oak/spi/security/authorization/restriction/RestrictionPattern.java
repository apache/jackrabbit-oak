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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

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
    boolean matches(@Nonnull Tree tree, @Nullable PropertyState property);

    /**
     * Returns {@code true} if the underlying restriction matches the specified
     * path.
     *
     * @param path The path of the target item.
     * @return {@code true} if the underlying restriction matches the specified
     * path; {@code false} otherwise.
     */
    boolean matches(@Nonnull String path);

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
        public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            return true;
        }

        @Override
        public boolean matches(@Nonnull String path) {
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
