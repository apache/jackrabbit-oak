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

package org.apache.jackrabbit.oak.plugins.tree.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Instances of this class represent trees that are inaccessible because
 * of the respective content would potentially be internal (hidden).
 * <p>
 * Calls to any of the mutator methods on this class throws an
 * {@code IllegalStateException}.
 */
class HiddenTree implements Tree {
    private final Tree parent;
    private final String name;

    HiddenTree(Tree parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    //------------------------------------------------------------< Object >---

    @Override
    public String toString() {
        return getPath() + ": {}";
    }

    //------------------------------------------------------------< Tree >---

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isRoot() {
        return false;
    }

    @Nonnull
    @Override
    public String getPath() {
        return PathUtils.concat(parent.getPath(), name);
    }

    @Nonnull
    @Override
    public Status getStatus() {
        return Status.UNCHANGED;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Nonnull
    @Override
    public Tree getParent() {
        return parent;
    }

    @Override
    @CheckForNull
    public PropertyState getProperty(@Nonnull String name) {
        return null;
    }

    @Override
    @CheckForNull
    public Status getPropertyStatus(@Nonnull String name) {
        return null;
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return false;
    }

    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Tree getChild(@Nonnull String name) {
        return new HiddenTree(this, checkNotNull(name));
    }

    @Override
    public boolean hasChild(@Nonnull String name) {
        return false;
    }

    @Override
    public long getChildrenCount(long max) {
        return 0;
    }

    @Nonnull
    @Override
    public Iterable<Tree> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public boolean remove() {
        return false;
    }

    @Nonnull
    @Override
    public Tree addChild(@Nonnull String name) {
        throw nonExistingTree();
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        throw nonExistingTree();
    }

    @Override
    public boolean orderBefore(@Nullable String name) {
        throw nonExistingTree();
    }

    @Override
    public void setProperty(@Nonnull PropertyState property) {
        throw nonExistingTree();
    }

    @Override
    public <T> void setProperty(@Nonnull String name, @Nonnull T value) {
        throw nonExistingTree();
    }

    @Override
    public <T> void setProperty(@Nonnull String name, @Nonnull T value, @Nonnull Type<T> type) {
        throw nonExistingTree();
    }

    @Override
    public void removeProperty(@Nonnull String name) {
        throw nonExistingTree();
    }

    private static IllegalStateException nonExistingTree() {
        return new IllegalStateException("This tree does not exist");
    }

}
