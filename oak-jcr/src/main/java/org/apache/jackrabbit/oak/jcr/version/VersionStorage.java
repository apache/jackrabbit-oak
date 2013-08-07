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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Simple abstraction of the version storage.
 */
public class VersionStorage {

    private final Root root;
    private final Tree versionStorage;

    public VersionStorage(@Nonnull Root versionStorageRoot,
                   @Nonnull Tree versionStorageTree) {
        this.root = versionStorageRoot;
        this.versionStorage = versionStorageTree;
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
        return versionStorage;
    }

    /**
     * Commits changes made to the version storage tree.
     *
     * @throws CommitFailedException if the commit fails.
     */
    void commit() throws CommitFailedException {
        root.commit();
    }

    /**
     * Reverts all changes made to the version storage tree.
     */
    void refresh() {
        root.refresh();
    }
}
