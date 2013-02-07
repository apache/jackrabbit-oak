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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This {@code TreeLocation} refers to an invalid location in a tree. That is
 * to a location where no item resides.
 */
final class NullLocation implements TreeLocation {
    public static final NullLocation NULL = new NullLocation();

    private final TreeLocation parent;
    private final String name;

    public NullLocation(TreeLocation parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    private NullLocation() {
        this.parent = this;
        this.name = "";
    }

    @Override
    public TreeLocation getParent() {
        return parent;
    }

    @Override
    public TreeLocation getChild(String relPath) {
        checkArgument(!PathUtils.isAbsolute(relPath), "Not a relative path: " + relPath);

        TreeLocation child = this;
        for (String e : PathUtils.elements(relPath)) {
            child = new NullLocation(child, e);
        }

        return child;
    }

    /**
     * @return {@code false}
     */
    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public String getPath() {
        return parent == this ? "" : PathUtils.concat(parent.getPath(), name);
    }

    /**
     * @return Always {@code false}.
     */
    @Override
    public boolean remove() {
        return false;
    }

    /**
     * @return  {@code null}
     */
    @Override
    public Tree getTree() {
        return null;
    }

    /**
     * @return  {@code null}
     */
    @Override
    public PropertyState getProperty() {
        return null;
    }

    /**
     * @return  {@code null}
     */
    @Override
    public Status getStatus() {
        return null;
    }

    /**
     * @return {@code false}
     */
    @Override
    public boolean set(PropertyState property) {
        return false;
    }
}
