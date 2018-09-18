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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * A {@code ThreeWayConflictHandler} is responsible for handling conflicts which happen
 * on {@link org.apache.jackrabbit.oak.api.Root#rebase()} and on the implicit rebase operation which
 * takes part on {@link org.apache.jackrabbit.oak.api.Root#commit()}.
 *
 * This interface contains one method per type of conflict which might occur.
 * Each of these methods must return a {@link Resolution} for the current conflict.
 * The resolution indicates to use the changes in the current {@code Root} instance
 * ({@link Resolution#OURS}) or to use the changes from the underlying persistence
 * store ({@link Resolution#THEIRS}). Alternatively the resolution can also indicate
 * that the changes have been successfully merged by this {@code ThreeWayConflictHandler}
 * instance ({@link Resolution#MERGED}).
 *
 */
public interface ThreeWayConflictHandler {
    /**
     * Resolutions for conflicts
     */
    enum Resolution {
        /**
         * Use the changes from the current {@link org.apache.jackrabbit.oak.api.Root} instance
         */
        OURS,

        /**
         * Use the changes from the underlying persistence store
         */
        THEIRS,

        /**
         * Indicated changes have been merged by this {@code ConflictHandler} instance.
         */
        MERGED,

        /**
         * Changes are ignored by this handler.
         */
        IGNORED
    }

    /**
     * The property {@code ours} has been added to {@code parent} which conflicts
     * with property {@code theirs} which has been added in the persistence store.
     *
     * @param parent  root of the conflict
     * @param ours  our version of the property
     * @param theirs  their version of the property
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution addExistingProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState theirs);

    /**
     * The property {@code ours} has been changed in {@code parent} while it was
     * removed in the persistence store.
     *
     * @param parent  root of the conflict
     * @param ours  our version of the property
     * @param base  the base version of the property
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution changeDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState base);

    /**
     * The property {@code ours} has been changed in {@code parent} while it was
     * also changed to a different value ({@code theirs}) in the persistence store.
     *
     * @param parent  root of the conflict
     * @param ours  our version of the property
     * @param theirs  their version of the property
     * @param base  the base version of the property
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution changeChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState theirs, @NotNull PropertyState base);

    /**
     * The property {@code ours} has been removed in {@code parent} while it was
     * also removed in the persistence store.
     *
     * @param parent  root of the conflict
     * @param base  the base version of the property
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution deleteDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState base);

    /**
     * The property {@code theirs} changed in the persistence store while it has been
     * deleted locally.
     *
     * @param parent  root of the conflict
     * @param theirs  their version of the property
     * @param base  the base version of the property
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution deleteChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState theirs,
            @NotNull PropertyState base);

    /**
     * The node {@code ours} has been added to {@code parent} which conflicts
     * with node {@code theirs} which has been added in the persistence store.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param ours  our version of the node
     * @param theirs  their version of the node
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution addExistingNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours,
            @NotNull NodeState theirs);

    /**
     * The node {@code ours} has been changed in {@code parent} while it was
     * removed in the persistence store.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param ours  our version of the node
     * @param base  the base version of the node
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution changeDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours,
            @NotNull NodeState base);

    /**
     * The node {@code theirs} changed in the persistence store while it has been
     * deleted locally.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param theirs  their version of the node
     * @param base  the base version of the node
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution deleteChangedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState theirs,
            @NotNull NodeState base);

    /**
     * The node {@code name} has been removed in {@code parent} while it was
     * also removed in the persistence store.
     *
     * @param parent  root of the conflict
     * @param name  name of the node
     * @param base  the base version of the node
     * @return  {@link Resolution} of the conflict
     */
    @NotNull
    Resolution deleteDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState base);
}
