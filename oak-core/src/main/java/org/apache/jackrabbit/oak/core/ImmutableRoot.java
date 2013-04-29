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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.BlobFactory;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Simple implementation of the Root interface that only supports simple read
 * operations (excluding query) based on the {@code NodeState} (or {@code ImmutableTree})
 * passed to the constructor.
 */
public final class ImmutableRoot implements Root {

    private final ImmutableTree rootTree;

    public ImmutableRoot(@Nonnull NodeState rootState) {
        this(new ImmutableTree(rootState));
    }

    public ImmutableRoot(@Nonnull Root root, @Nonnull TreeTypeProvider typeProvider) {
        this(ImmutableTree.createFromRoot(root, typeProvider));
    }

    public ImmutableRoot(@Nonnull ImmutableTree rootTree) {
        checkArgument(rootTree.isRoot());
        this.rootTree = rootTree;
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
    @Deprecated
    public ImmutableTree getTreeOrNull(String path) {
        ImmutableTree tree = getTree(path);
        return tree.exists() ? tree : null;
    }

    @Nonnull
    @Override
    public TreeLocation getLocation(String path) {
        checkArgument(PathUtils.isAbsolute(path));
        TreeLocation child = rootTree.getLocation();
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
    public boolean copy(String sourcePath, String destPath) {
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
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public BlobFactory getBlobFactory() {
        throw new UnsupportedOperationException();
    }
    
	@Override
	public ContentSession getContentSession() {
		throw new UnsupportedOperationException();
	}
    
}
