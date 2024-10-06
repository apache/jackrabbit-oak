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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.jetbrains.annotations.NotNull;


/**
 * Helper class to handle modifications to the hidden
 * {@link TreeConstants#OAK_CHILD_ORDER} property.
 */
final class ChildOrderDiff {

    private ChildOrderDiff() {}

    /**
     * Tests if there was any user-supplied reordering involved with the
     * modification of the {@link TreeConstants#OAK_CHILD_ORDER}
     * property.
     *
     * @param before
     * @param after
     * @return {@code true} if any user-supplied node
     * reorder happened; {@code false} otherwise.
     */
    static boolean isReordered(@NotNull PropertyState before, @NotNull PropertyState after) {
        Set<String> afterNames = CollectionUtils.toLinkedSet(after.getValue(Type.NAMES));
        Set<String> beforeNames = CollectionUtils.toLinkedSet(before.getValue(Type.NAMES));

        // drop all newly added values from 'afterNames'
        afterNames.retainAll(beforeNames);
        // drop all removed values from 'beforeNames'
        beforeNames.retainAll(afterNames);

        // names got reordered if the elements in the 2 intersections aren't equal
        return !Iterables.elementsEqual(afterNames, beforeNames);
    }
}
