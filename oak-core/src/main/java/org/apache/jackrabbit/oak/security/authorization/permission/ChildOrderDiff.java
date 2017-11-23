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

import java.util.Iterator;
import java.util.Set;
import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;

import static com.google.common.collect.Sets.newLinkedHashSet;

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
     * @return the name of the first reordered child if any user-supplied node
     * reorder happened; {@code null} otherwise.
     */
    @CheckForNull
    static String firstReordered(PropertyState before, PropertyState after) {
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
}
