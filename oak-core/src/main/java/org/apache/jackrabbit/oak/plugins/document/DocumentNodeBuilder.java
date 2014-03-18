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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A node builder implementation for DocumentMK.
 */
class DocumentNodeBuilder extends MemoryNodeBuilder {

    private final DocumentRootBuilder root;

    private NodeState base;

    private NodeState rootBase;

    DocumentNodeBuilder(MemoryNodeBuilder base,
                        String name,
                        DocumentRootBuilder root) {
        super(base, name);
        this.root = checkNotNull(root);
    }

    @Override
    @Nonnull
    public NodeState getBaseState() {
        if (base == null || rootBase != root.getBaseState()) {
            base = getParent().getBaseState().getChildNode(getName());
            rootBase = root.getBaseState();
        }
        return base;
    }

    @Override
    protected DocumentNodeBuilder createChildBuilder(String name) {
        return new DocumentNodeBuilder(this, name, root);
    }

    @Override
    public boolean moveTo(NodeBuilder newParent, String newName) {
        if (newParent instanceof DocumentNodeBuilder) {
            // check if this builder is an ancestor of newParent or newParent
            DocumentNodeBuilder parent = (DocumentNodeBuilder) newParent;
            while (parent != null) {
                if (parent == this) {
                    return false;
                }
                if (parent.getParent() != root) {
                    parent = (DocumentNodeBuilder) parent.getParent();
                } else {
                    // reached root builder
                    break;
                }
            }
        }
        return super.moveTo(newParent, newName);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return root.createBlob(stream);
    }
}
