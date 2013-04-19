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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

/**
 * Helper class to handle modifications to the hidden
 * {@link org.apache.jackrabbit.oak.core.TreeImpl#OAK_CHILD_ORDER} property.
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
     * modification of the {@link org.apache.jackrabbit.oak.core.TreeImpl#OAK_CHILD_ORDER}
     * property.
     *
     * @return the name of the first reordered child if any user-supplied node
     * reorder happened; {@code null} otherwise.
     */
    @CheckForNull
    String firstReordered() {
        List<String> beforeNames = Lists.newArrayList(before.getValue(Type.NAMES));
        List<String> afterNames = Lists.newArrayList(after.getValue(Type.NAMES));
        // remove elements from before that have been deleted
        beforeNames.retainAll(afterNames);

        for (int i = 0; i < beforeNames.size() && i < afterNames.size(); i++) {
            String bName = beforeNames.get(i);
            String aName = afterNames.get(i);
            if (!bName.equals(aName)) {
                return aName;
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
        List<String> beforeNames = Lists.newArrayList(before.getValue(Type.NAMES));
        List<String> afterNames = Lists.newArrayList(after.getValue(Type.NAMES));
        // remove elements from before that have been deleted
        beforeNames.retainAll(afterNames);

        List<String> reordered = new ArrayList<String>();
        for (int i = 0; i < afterNames.size(); i++) {
            String aName = afterNames.get(i);
            int index = beforeNames.indexOf(aName);
            if (index != -1 && index != i) {
                reordered.add(aName);
            }
        }
        return reordered;
    }
}