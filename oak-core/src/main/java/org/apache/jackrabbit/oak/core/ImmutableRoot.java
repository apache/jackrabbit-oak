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
package org.apache.jackrabbit.oak.core;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.io.InputStream;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.tree.ReadOnly;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.query.ExecutionContext;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Simple implementation of the Root interface that only supports simple read
 * operations based on the {@code NodeState} (or {@code ImmutableTree})
 * passed to the constructor. This root implementation provides a query engine
 * with index support limited to the {@link PropertyIndexProvider}.
 */
public final class ImmutableRoot implements Root, ReadOnly {

    private final ImmutableTree rootTree;

    public ImmutableRoot(@Nonnull NodeState rootState) {
        this(new ImmutableTree(rootState));
    }

    public ImmutableRoot(@Nonnull Root root) {
        if (root instanceof MutableRoot) {
            rootTree = new ImmutableTree(((MutableRoot) root).getBaseState());
        } else if (root instanceof ImmutableRoot) {
            rootTree = ((ImmutableRoot) root).getTree("/");
        } else {
            throw new IllegalArgumentException("Unsupported Root implementation: " + root.getClass());
        }
    }

    public ImmutableRoot(@Nonnull ImmutableTree rootTree) {
        checkArgument(rootTree.isRoot());
        this.rootTree = rootTree;
    }

    public static ImmutableRoot getInstance(@Nonnull Root root) {
        if (root instanceof ImmutableRoot) {
            return (ImmutableRoot) root;
        } else {
            return new ImmutableRoot(root);
        }
    }

    //---------------------------------------------------------------< Root >---

    @Nonnull
    @Override
    public ImmutableTree getTree(@Nonnull String path) {
        checkArgument(PathUtils.isAbsolute(path));
        ImmutableTree child = rootTree;
        for (String name : elements(path)) {
            child = child.getChild(name);
        }
        return child;
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rebase() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(@Nonnull Map<String, Object> info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPendingChanges() {
        return false;
    }

    @Nonnull
    @Override
    public QueryEngine getQueryEngine() {
        return new QueryEngineImpl() {
            @Override
            protected ExecutionContext getExecutionContext() {
                return new ExecutionContext(
                        rootTree.getNodeState(), ImmutableRoot.this,
                        new QueryEngineSettings(),
                        new PropertyIndexProvider(), null);
            }
        };
    }

    @Override @Nonnull
    public Blob createBlob(@Nonnull InputStream stream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        return null;
    }

    @Nonnull
    @Override
    public ContentSession getContentSession() {
        throw new UnsupportedOperationException();
    }

}
