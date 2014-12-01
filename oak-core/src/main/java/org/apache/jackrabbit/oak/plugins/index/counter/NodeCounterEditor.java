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
package org.apache.jackrabbit.oak.plugins.index.counter;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.ApproximateCounter;

/**
 * An approximate descendant node counter mechanism.
 */
public class NodeCounterEditor implements Editor {

    public static final String DATA_NODE_NAME = ":index";
    public static final String COUNT_PROPERTY_NAME = ":count";
    private static final int DEFAULT_RESOLUTION = 1000;
    
    private final NodeCounterRoot root;
    private final NodeCounterEditor parent;
    private final String name;
    private long countOffset;
    
    public NodeCounterEditor(NodeCounterRoot root, NodeCounterEditor parent, String name) {
        this.parent = parent;
        this.root = root;
        this.name = name;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        long offset = ApproximateCounter.calculateOffset(
                countOffset, root.resolution);
        if (offset == 0) {
            return;
        }
        // only read the value of the property if really needed
        NodeBuilder builder = getBuilder();
        PropertyState p = builder.getProperty(COUNT_PROPERTY_NAME);
        long count = p == null ? 0 : p.getValue(Type.LONG);
        offset = ApproximateCounter.adjustOffset(count,
                offset, root.resolution);
        if (offset == 0) {
            return;
        }
        count += offset;
        root.callback.indexUpdate();
        if (count == 0) {
            if (builder.getChildNodeCount(1) >= 0) {
                builder.removeProperty(COUNT_PROPERTY_NAME);
            } else {
                builder.remove();
            }
        } else {
            builder.setProperty(COUNT_PROPERTY_NAME, count);
        }
    }

    private NodeBuilder getBuilder() {
        if (parent == null) {
            return root.definition.child(DATA_NODE_NAME);
        }
        return parent.getBuilder().child(name);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        // nothing to do
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        // nothing to do
    }
    
    @Override
    @CheckForNull
    public Editor childNodeChanged(String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return getChildIndexEditor(this, name);
    }

    @Override
    @CheckForNull
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        count(1);
        return getChildIndexEditor(this, name);
    }

    @Override
    @CheckForNull
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        count(-1);
        return getChildIndexEditor(this, name);
    }
    
    private void count(int offset) {
        countOffset += offset;
        if (parent != null) {
            parent.count(offset);
        }
    }
    
    private Editor getChildIndexEditor(NodeCounterEditor nodeCounterEditor,
            String name) {
        return new NodeCounterEditor(root, this, name);
    }
    
    public static class NodeCounterRoot {
        int resolution = DEFAULT_RESOLUTION;
        NodeBuilder definition;
        NodeState root;
        IndexUpdateCallback callback;
    }

}
