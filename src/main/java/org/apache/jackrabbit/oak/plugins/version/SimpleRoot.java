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
package org.apache.jackrabbit.oak.plugins.version;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.BlobFactory;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * <code>SimpleRoot</code>...
 */
class SimpleRoot implements Root {

    private final Tree rootTree;

    SimpleRoot(Tree rootTree) {
        checkArgument(rootTree.isRoot());
        this.rootTree = rootTree;
    }

    @Override
    public Tree getTree(String path) {
        return getLocation(path).getTree();
    }

    @Nonnull
    @Override
    public TreeLocation getLocation(String path) {
        checkArgument(path.startsWith("/"));
        return rootTree.getLocation().getChild(path.substring(1));
    }

    //---------------------< unsupported methods >------------------------------

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
    public void commit() throws CommitFailedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPendingChanges() {
        throw new UnsupportedOperationException();
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
}
