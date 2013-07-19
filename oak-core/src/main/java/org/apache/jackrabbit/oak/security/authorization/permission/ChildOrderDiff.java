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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

/**
 * Helper class to handle modifications to the hidden
 * {@link org.apache.jackrabbit.oak.core.AbstractTree#OAK_CHILD_ORDER} property.
 */
class ChildOrderDiff {

    private final PropertyState before;
    private final PropertyState after;

    ChildOrderDiff(PropertyState before, PropertyState after) {
        this.before = before;
        this.after = after;
    }

    /**
     * Tests if there was any user-supplied reordering involved with the
     * modification of the {@link org.apache.jackrabbit.oak.core.AbstractTree#OAK_CHILD_ORDER}
     * property.
     *
     * @return the name of the first reordered child if any user-supplied node
     * reorder happened; {@code null} otherwise.
     */
    @CheckForNull
    String firstReordered() {
        Set<String> afterNames = newLinkedHashSet(after.getValue(Type.NAMES));
        Set<String> beforeNames = newLinkedHashSet(before.getValue(Type.NAMES));

        Iterator<String> a = afterNames.iterator();
        Iterator<String> b = beforeNames.iterator();
        while (a.hasNext() && b.hasNext()) {
            String aName = a.next();
            String bName = b.next();
            while (!aName.equals(bName)) {
                if (!beforeNames.contains(aName)) {
                    if (a.hasNext()) {
                        aName = a.next();
                    } else {
                        return null;
                    }
                } else if (!afterNames.contains(bName)) {
                    if (b.hasNext()) {
                        bName = b.next();
                    } else {
                        return null;
                    }
                } else {
                    return aName;
                }
            }
        }

        return null;
    }

    /**
     * Returns the names of all reordered nodes present in the 'after' property.
     *
     * @return
     */
    @Nonnull
    List<String> getReordered() {
        List<String> reordered = newArrayList();

        Set<String> afterNames = newLinkedHashSet(after.getValue(Type.NAMES));
        Set<String> beforeNames = newLinkedHashSet(before.getValue(Type.NAMES));

        afterNames.retainAll(beforeNames);
        beforeNames.retainAll(afterNames);

        Iterator<String> a = afterNames.iterator();
        Iterator<String> b = beforeNames.iterator();
        while (a.hasNext() && b.hasNext()) {
            String aName = a.next();
            String bName = b.next();
            if (!aName.equals(bName)) {
                reordered.add(aName);
            }
        }

        return reordered;
    }

}
