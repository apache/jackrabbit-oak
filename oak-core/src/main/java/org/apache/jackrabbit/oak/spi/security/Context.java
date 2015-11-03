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
package org.apache.jackrabbit.oak.spi.security;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;

/**
 * {@code Context} represents item related information in relation to a
 * dedicated {@link org.apache.jackrabbit.oak.spi.security.SecurityConfiguration}.
 * This information allows to determine if a given {@code Tree} or {@link PropertyState}
 * is defined by or related to the security model provided by the configuration.
 */
public interface Context {

    /**
     * Reveals if the specified {@code PropertyState} is defined by the security
     * module that exposes this {@link Context} instance.
     *
     * @param parent The parent tree of the property state.
     * @param property The {@code PropertyState} to be tested.
     * @return {@code true} if the specified property state is related to or
     * defined by the security module.
     */
    boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property);

    /**
     * Reveals if the specified {@code Tree} is the root of a subtree defined by
     * the security module that exposes this {@link Context} instance. Note,
     * that in contrast to {@link #definesTree(org.apache.jackrabbit.oak.api.Tree)}
     * this method will only return {@code false} for any tree located in the
     * subtree.
     *
     * @param tree The tree to be tested.
     * @return {@code true} if the specified tree is the root of a subtree of items
     * that are defined by the security module.
     */
    boolean definesContextRoot(@Nonnull Tree tree);

    /**
     * Reveals if the specified {@code Tree} is defined by the security
     * module that exposes this {@link Context} instance.
     *
     * @param tree The tree to be tested.
     * @return {@code true} if the specified tree is related to or defined by the
     * security module.
     */
    boolean definesTree(@Nonnull Tree tree);

    /**
     * Reveals if the specified {@code TreeLocation} is defined by the security
     * module that exposes this {@link Context} instance.
     *
     * @param location The tree location to be tested.
     * @return {@code true} if the specified tree location is related to or
     * defined by the security module.
     */
    boolean definesLocation(@Nonnull TreeLocation location);

    Context DEFAULT = new Default();

    /**
     * Default implementation of the {@code Context} interface that always returns
     * {@code false}.
     */
    class Default implements Context {

        @Override
        public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
            return false;
        }

        @Override
        public boolean definesContextRoot(@Nonnull Tree tree) {
            return false;
        }

        @Override
        public boolean definesTree(@Nonnull Tree tree) {
            return false;
        }

        @Override
        public boolean definesLocation(@Nonnull TreeLocation location) {
            return false;
        }

    }
}