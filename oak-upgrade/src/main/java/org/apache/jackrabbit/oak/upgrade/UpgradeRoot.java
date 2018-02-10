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
package org.apache.jackrabbit.oak.upgrade;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.impl.NodeBuilderTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Simplistic 'Root' implementation around the {@code NodeBuilder} used for the
 * repository upgrade in order to be able to make use of existing functionality
 * like privilege and node type registration without attempting to properly
 * implement the full contract of the {@link Root} interface.
 */
class UpgradeRoot implements Root {
    private final NodeBuilderTree rootTree;

    UpgradeRoot(NodeBuilder nodeBuilder) {
        rootTree = new NodeBuilderTree("", nodeBuilder);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Tree getTree(@Nonnull String path) {
        Tree child = rootTree;
        for (String name : elements(path)) {
            child = child.getChild(name);
        }
        return child;
    }

    @Override
    public void rebase() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh() {
        // ignore
    }

    @Override
    public void commit(@Nonnull Map<String, Object> info) throws CommitFailedException {
        // ignore
    }

    @Override
    public void commit() throws CommitFailedException {
        // ignore
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
    public Blob createBlob(@Nonnull InputStream stream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public ContentSession getContentSession() {
        return new ContentSession() {
            @Nonnull
            @Override
            public AuthInfo getAuthInfo() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getWorkspaceName() {
                throw new UnsupportedOperationException();
            }

            @Nonnull
            @Override
            public Root getLatestRoot() {
                return UpgradeRoot.this;
            }

            @Override
            public void close() throws IOException {
                // nothing to do
            }
        };
    }

}
