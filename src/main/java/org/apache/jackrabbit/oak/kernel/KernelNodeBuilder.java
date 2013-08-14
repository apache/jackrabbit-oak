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
package org.apache.jackrabbit.oak.kernel;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * This class refines move and copy operations by delegating
 * them to the underlying store if possible.
 * @see KernelRootBuilder
 */
public class KernelNodeBuilder extends MemoryNodeBuilder {

    private final KernelRootBuilder root;

    public KernelNodeBuilder(MemoryNodeBuilder parent, String name, KernelRootBuilder root) {
        super(parent, name);
        this.root = checkNotNull(root);
    }

    //--------------------------------------------------< MemoryNodeBuilder >---

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new KernelNodeBuilder(this, name, root);
    }

    @Override
    protected void updated() {
        root.updated();
    }

    /**
     * If {@code newParent} is a {@link KernelNodeBuilder} this implementation
     * purges all pending changes before applying the move operation. This allows the
     * underlying store to better optimise move operations instead of just seeing
     * them as an added and a removed node.
     * If {@code newParent} is not a {@code KernelNodeBuilder} the implementation
     * falls back to the super class.
     */
    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) {
        if (newParent instanceof KernelNodeBuilder) {
            String source = getPath();
            String target = PathUtils.concat(((KernelNodeBuilder) newParent).getPath(), checkNotNull(newName));
            return root.move(source, target);
        } else {
            return super.moveTo(newParent, newName);
        }
    }

    /**
     * If {@code newParent} is a {@link KernelNodeBuilder} this implementation
     * purges all pending changes before applying the copy operation. This allows the
     * underlying store to better optimise copy operations instead of just seeing
     * them as an added node.
     * If {@code newParent} is not a {@code KernelNodeBuilder} the implementation
     * falls back to the super class.
     */
    @Override
    public boolean copyTo(NodeBuilder newParent, String newName) {
        if (newParent instanceof KernelNodeBuilder) {
            String source = getPath();
            String target = PathUtils.concat(((KernelNodeBuilder) newParent).getPath(), checkNotNull(newName));
            return root.copy(source, target);
        } else {
            return super.copyTo(newParent, newName);
        }
    }
}
