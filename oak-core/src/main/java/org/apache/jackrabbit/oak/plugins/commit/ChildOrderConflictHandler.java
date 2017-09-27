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
package org.apache.jackrabbit.oak.plugins.commit;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This conflict handler instance takes care of properly merging conflicts
 * occurring by concurrent reorder operations.
 *
 * @see TreeConstants#OAK_CHILD_ORDER
 */
public class ChildOrderConflictHandler implements PartialConflictHandler {

    @Override
    public Resolution addExistingProperty(NodeBuilder parent,
            PropertyState ours,
            PropertyState theirs) {
        if (isChildOrderProperty(ours)) {
            // two sessions concurrently called orderBefore() on a Tree
            // that was previously unordered.
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        } else {
            return null;
        }
    }

    @Override
    public Resolution changeDeletedProperty(NodeBuilder parent,
            PropertyState ours) {
        if (isChildOrderProperty(ours)) {
            // orderBefore() on trees that were deleted
            return Resolution.THEIRS;
        } else {
            return null;
        }
    }

    @Override
    public Resolution changeChangedProperty(NodeBuilder parent,
            PropertyState ours,
            PropertyState theirs) {
        if (isChildOrderProperty(ours)) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        } else {
            return null;
        }
    }

    private static void merge(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        Set<String> theirOrder = Sets.newHashSet(theirs.getValue(Type.NAMES));
        PropertyBuilder<String> merged = PropertyBuilder.array(Type.NAME).assignFrom(theirs);

        // Append child node names from ours that are not in theirs
        for (String ourChild : ours.getValue(Type.NAMES)) {
            if (!theirOrder.contains(ourChild)) {
                merged.addValue(ourChild);
            }
        }

        // Remove child node names of nodes that have been removed
        for (String child : merged.getValues()) {
            if (!parent.hasChildNode(child)) {
                merged.removeValue(child);
            }
        }

        parent.setProperty(merged.getPropertyState());
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent,
            PropertyState ours) {
        if (isChildOrderProperty(ours)) {
            // concurrent remove of ordered trees
            return Resolution.THEIRS;
        } else {
            return null;
        }
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent,
            PropertyState theirs) {
        if (isChildOrderProperty(theirs)) {
            // remove trees that were reordered by another session
            return Resolution.THEIRS;
        } else {
            return null;
        }
    }

    @Override
    public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours, NodeState theirs) {
        return null;
    }

    @Override
    public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours) {
        return null;
    }

    @Override
    public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs) {
        return null;
    }

    @Override
    public Resolution deleteDeletedNode(NodeBuilder parent, String name) {
        return null;
    }

    //----------------------------< internal >----------------------------------

    private static boolean isChildOrderProperty(PropertyState p) {
        return TreeConstants.OAK_CHILD_ORDER.equals(p.getName());
    }
}

