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
package org.apache.jackrabbit.oak.jcr.version;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple abstraction of the version storage.
 */
public class VersionStorage {

    public static final String VERSION_STORAGE_PATH
            = '/' + JcrConstants.JCR_SYSTEM + '/' + JcrConstants.JCR_VERSIONSTORAGE;

    private final Root root;

    public VersionStorage(@Nonnull Root versionStorageRoot) {
        this.root = versionStorageRoot;
    }

    Root getRoot() {
        return root;
    }

    /**
     * The version storage tree. I.e. the tree at path
     * <code>/jcr:system/jcr:versionStorage</code>, though the returned
     * tree instance may not necessarily return this path on
     * {@link Tree#getPath()}!
     *
     * @return the version storage tree.
     */
    Tree getTree() {
        return getVersionStorageTree(root);
    }

    /**
     * Reverts all changes made to the version storage tree.
     */
    void refresh() {
        root.refresh();
    }

    /**
     * Returns the version storage tree for the given workspace.
     * @param workspaceRoot the root of the workspace.
     * @return the version storage tree.
     */
    private static Tree getVersionStorageTree(@Nonnull Root workspaceRoot) {
        // TODO: this assumes the version store is in the same workspace.
        return checkNotNull(workspaceRoot).getTree(VERSION_STORAGE_PATH);
    }

}
